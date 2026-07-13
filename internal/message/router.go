package message

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Router is the durable outbox / inbox state machine for run-scoped messages.
// Persistence always precedes in-memory index updates.
// Per-message locks linearize mutations for a single message_id (intent freeze,
// delivery attempts, transitions) so concurrent clients cannot both observe a
// stale snapshot and both append conflicting journal records.
type Router struct {
	mu       sync.Mutex
	runID    string
	store    *Store
	index    map[string]Message
	now      func() time.Time
	newID    func(time.Time) (string, error)
	msgLocks sync.Map // messageID -> *sync.Mutex
}

func (r *Router) lockMessage(messageID string) *sync.Mutex {
	v, _ := r.msgLocks.LoadOrStore(messageID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// NewRouterOptions configures a Router.
type NewRouterOptions struct {
	RunID   string
	Store   *Store
	Initial map[string]Message
	Now     func() time.Time
	NewID   func(time.Time) (string, error)
}

// NewRouter constructs a Router. Initial is copied; the caller's map is never retained.
func NewRouter(options NewRouterOptions) (*Router, error) {
	if strings.TrimSpace(options.RunID) == "" {
		return nil, fmt.Errorf("run id is required")
	}
	if options.Store == nil {
		return nil, fmt.Errorf("message store is required")
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	newID := options.NewID
	if newID == nil {
		newID = NewID
	}
	index := map[string]Message{}
	for id, value := range options.Initial {
		index[id] = copyMessage(value)
	}
	return &Router{
		runID: options.RunID,
		store: options.Store,
		index: index,
		now:   now,
		newID: newID,
	}, nil
}

// EnqueueInstruction persists a queued instruction before updating memory.
func (r *Router) EnqueueInstruction(
	taskID string,
	workerID string,
	text string,
	mode DeliveryMode,
) (Message, error) {
	return r.EnqueueInstructionWithAttempt(taskID, workerID, 0, text, mode)
}

// EnqueueInstructionWithAttempt persists a queued instruction with optional attempt linkage.
func (r *Router) EnqueueInstructionWithAttempt(
	taskID string,
	workerID string,
	attemptNumber int,
	text string,
	mode DeliveryMode,
) (Message, error) {
	if strings.TrimSpace(taskID) == "" {
		return Message{}, fmt.Errorf("task id is required")
	}
	if strings.TrimSpace(text) == "" {
		return Message{}, fmt.Errorf("instruction text is required")
	}

	now := r.now().UTC()
	id, err := r.newID(now)
	if err != nil {
		return Message{}, err
	}
	payload, err := json.Marshal(InstructionPayload{Text: text})
	if err != nil {
		return Message{}, err
	}
	value := Message{
		SchemaVersion: SchemaVersion,
		MessageID:     id,
		RunID:         r.runID,
		TaskID:        taskID,
		WorkerID:      workerID,
		AttemptNumber: attemptNumber,
		Type:          Instruction,
		Status:        Queued,
		CreatedAt:     now,
		UpdatedAt:     now,
		DeliveryMode:  mode,
		Payload:       payload,
	}
	if err := r.store.Append(value); err != nil {
		return Message{}, err
	}

	r.mu.Lock()
	r.index[id] = copyMessage(value)
	r.mu.Unlock()
	return copyMessage(value), nil
}

// EnqueueDecision persists a queued decision/question message before memory update.
func (r *Router) EnqueueDecision(
	taskID, workerID string,
	messageType Type,
	category Category,
	payload json.RawMessage,
) (Message, error) {
	return r.EnqueueDecisionWithAttempt(taskID, workerID, 0, messageType, category, payload)
}

// EnqueueDecisionWithAttempt persists a decision message with attempt linkage.
// Native permission requests must pass a non-zero attempt when known; 0 is
// reserved for legacy/non-native callers and is not a delivery wildcard.
func (r *Router) EnqueueDecisionWithAttempt(
	taskID, workerID string,
	attemptNumber int,
	messageType Type,
	category Category,
	payload json.RawMessage,
) (Message, error) {
	if strings.TrimSpace(taskID) == "" {
		return Message{}, fmt.Errorf("task id is required")
	}
	if messageType == "" {
		return Message{}, fmt.Errorf("message type is required")
	}
	now := r.now().UTC()
	id, err := r.newID(now)
	if err != nil {
		return Message{}, err
	}
	value := Message{
		SchemaVersion: SchemaVersion,
		MessageID:     id,
		RunID:         r.runID,
		TaskID:        taskID,
		WorkerID:      workerID,
		AttemptNumber: attemptNumber,
		Type:          messageType,
		Category:      category,
		Status:        Queued,
		CreatedAt:     now,
		UpdatedAt:     now,
		Payload:       append(json.RawMessage(nil), payload...),
	}
	if err := r.store.Append(value); err != nil {
		return Message{}, err
	}
	r.mu.Lock()
	r.index[id] = copyMessage(value)
	r.mu.Unlock()
	return copyMessage(value), nil
}

// ResolutionFreezeResult describes the outcome of FreezeResolution.
type ResolutionFreezeResult int

const (
	// ResolutionFrozen is a new canonical resolution.
	ResolutionFrozen ResolutionFreezeResult = iota
	// ResolutionAlreadyIdentical is an idempotent retry of the same resolution.
	ResolutionAlreadyIdentical
	// ResolutionConflict is a different resolution from the frozen/terminal one.
	ResolutionConflict
)

// FreezeResolution freezes a canonical decision Resolution without making the
// message terminal. Works for every decision type (question, scope_expansion_request,
// permission_request). Journal append precedes the in-memory index update.
// Identical semantic resolutions are idempotent; conflicts are rejected.
// Does not increment DeliveryAttempts.
// Holds the per-message lock for the entire mutation (linearizable).
func (r *Router) FreezeResolution(messageID string, resolution json.RawMessage) (Message, ResolutionFreezeResult, error) {
	if len(resolution) == 0 {
		return Message{}, ResolutionConflict, fmt.Errorf("resolution is required")
	}
	ml := r.lockMessage(messageID)
	ml.Lock()
	defer ml.Unlock()

	r.mu.Lock()
	current, ok := r.index[messageID]
	if !ok {
		r.mu.Unlock()
		return Message{}, ResolutionConflict, fmt.Errorf("message %q was not found", messageID)
	}
	current = copyMessage(current)
	r.mu.Unlock()

	if IsTerminal(current.Status) {
		// Terminal: compare against persisted resolution for idempotency.
		if len(current.Resolution) == 0 {
			return Message{}, ResolutionConflict,
				fmt.Errorf("message %q is already terminal (%s) with no resolution", messageID, current.Status)
		}
		normalized, err := normalizeResolutionJSON(current.Type, resolution)
		if err != nil {
			return Message{}, ResolutionConflict, err
		}
		existing, err := normalizeResolutionJSON(current.Type, current.Resolution)
		if err != nil {
			return Message{}, ResolutionConflict, err
		}
		if bytesEqualJSON(existing, normalized) {
			return current, ResolutionAlreadyIdentical, nil
		}
		return Message{}, ResolutionConflict,
			fmt.Errorf("message %q is already terminal (%s) with a different resolution", messageID, current.Status)
	}

	if current.Status != Queued {
		return Message{}, ResolutionConflict,
			fmt.Errorf("freeze resolution requires queued status (status=%s)", current.Status)
	}

	normalized, err := normalizeResolutionJSON(current.Type, resolution)
	if err != nil {
		return Message{}, ResolutionConflict, err
	}
	if len(current.Resolution) > 0 {
		existing, err := normalizeResolutionJSON(current.Type, current.Resolution)
		if err != nil {
			return Message{}, ResolutionConflict, err
		}
		if bytesEqualJSON(existing, normalized) {
			return current, ResolutionAlreadyIdentical, nil
		}
		return Message{}, ResolutionConflict,
			fmt.Errorf("conflicting resolution: decision is already frozen")
	}

	candidate := copyMessage(current)
	candidate.Resolution = append(json.RawMessage(nil), normalized...)
	candidate.UpdatedAt = r.now().UTC()
	if err := r.store.Append(candidate); err != nil {
		return Message{}, ResolutionConflict, err
	}
	r.mu.Lock()
	// Re-check: another path may have installed a terminal state (expire/cancel).
	if latest, ok := r.index[messageID]; ok && IsTerminal(latest.Status) {
		r.mu.Unlock()
		return Message{}, ResolutionConflict,
			fmt.Errorf("message %q became terminal during intent freeze", messageID)
	}
	r.index[messageID] = copyMessage(candidate)
	r.mu.Unlock()
	return copyMessage(candidate), ResolutionFrozen, nil
}

// RecordResolutionIntent is deprecated; use FreezeResolution instead.
// Preserved for existing native permission callers during the transition.
func (r *Router) RecordResolutionIntent(messageID string, resolution json.RawMessage) (Message, error) {
	msg, result, err := r.FreezeResolution(messageID, resolution)
	if err != nil {
		if result == ResolutionAlreadyIdentical {
			return msg, nil
		}
		return Message{}, err
	}
	return msg, nil
}

// ReclassifyDelivery updates DeliveryMode for a still-queued instruction.
// Only next_turn → resume is permitted (session gone, resume path remains).
func (r *Router) ReclassifyDelivery(messageID string, mode DeliveryMode) (Message, error) {
	ml := r.lockMessage(messageID)
	ml.Lock()
	defer ml.Unlock()

	r.mu.Lock()
	current, ok := r.index[messageID]
	if !ok {
		r.mu.Unlock()
		return Message{}, fmt.Errorf("message %q was not found", messageID)
	}
	current = copyMessage(current)
	r.mu.Unlock()

	if current.Type != Instruction {
		return Message{}, fmt.Errorf("message %q is not an instruction", messageID)
	}
	if current.Status != Queued {
		return Message{}, fmt.Errorf("message %q is not queued (status %s)", messageID, current.Status)
	}
	if current.DeliveryMode == mode {
		return current, nil
	}
	if !(current.DeliveryMode == DeliveryNextTurn && mode == DeliveryResume) {
		return Message{}, fmt.Errorf("cannot reclassify delivery %s -> %s", current.DeliveryMode, mode)
	}

	candidate := copyMessage(current)
	candidate.DeliveryMode = mode
	candidate.UpdatedAt = r.now().UTC()
	if err := r.store.Append(candidate); err != nil {
		return Message{}, err
	}
	r.mu.Lock()
	if latest, ok := r.index[messageID]; ok && IsTerminal(latest.Status) {
		r.mu.Unlock()
		return Message{}, fmt.Errorf("message %q became terminal during reclassify", messageID)
	}
	r.index[messageID] = copyMessage(candidate)
	r.mu.Unlock()
	return copyMessage(candidate), nil
}

// RecordDeliveryAttempt increments delivery_attempts and optionally updates status.
// On success (cause == nil) a previous Error is cleared. On failure the message
// may remain Queued (next empty) with Error set — used for retryable native
// permission delivery. Holds the per-message lock for the entire mutation.
func (r *Router) RecordDeliveryAttempt(messageID string, next Status, cause error) (Message, error) {
	ml := r.lockMessage(messageID)
	ml.Lock()
	defer ml.Unlock()

	r.mu.Lock()
	current, ok := r.index[messageID]
	if !ok {
		r.mu.Unlock()
		return Message{}, fmt.Errorf("message %q was not found", messageID)
	}
	current = copyMessage(current)
	r.mu.Unlock()

	candidate := copyMessage(current)
	candidate.DeliveryAttempts = current.DeliveryAttempts + 1
	candidate.UpdatedAt = r.now().UTC()
	if next != "" && next != current.Status {
		if err := ValidateTransition(current.Status, next); err != nil {
			return Message{}, err
		}
		candidate.Status = next
	}
	if cause != nil {
		candidate.Error = cause.Error()
	} else {
		candidate.Error = ""
	}
	if err := r.store.Append(candidate); err != nil {
		return Message{}, err
	}
	r.mu.Lock()
	if latest, ok := r.index[messageID]; ok && IsTerminal(latest.Status) && next != latest.Status {
		// Do not overwrite a newer terminal with a stale delivery attempt.
		r.mu.Unlock()
		return copyMessage(latest), fmt.Errorf("message %q is already terminal (%s)", messageID, latest.Status)
	}
	r.index[messageID] = copyMessage(candidate)
	r.mu.Unlock()
	return copyMessage(candidate), nil
}

// Transition validates and persists a status change, then updates memory.
// Holds the per-message lock for the entire mutation (linearizable).
func (r *Router) Transition(
	messageID string,
	next Status,
	deliveryMode DeliveryMode,
	resolution json.RawMessage,
	cause error,
) (Message, error) {
	ml := r.lockMessage(messageID)
	ml.Lock()
	defer ml.Unlock()

	r.mu.Lock()
	current, ok := r.index[messageID]
	if !ok {
		r.mu.Unlock()
		return Message{}, fmt.Errorf("message %q was not found", messageID)
	}
	current = copyMessage(current)
	r.mu.Unlock()

	if err := ValidateTransition(current.Status, next); err != nil {
		return Message{}, err
	}
	if current.Status == next {
		// Same status alone does not prove semantic idempotence when resolution data differs.
		if len(resolution) > 0 && !bytesEqualJSON(current.Resolution, resolution) {
			return Message{}, fmt.Errorf("same-status transition with different resolution is forbidden (status=%s)", current.Status)
		}
		return current, nil
	}

	candidate := copyMessage(current)
	candidate.Status = next
	candidate.UpdatedAt = r.now().UTC()
	if deliveryMode != "" {
		candidate.DeliveryMode = deliveryMode
	}
	if resolution != nil {
		candidate.Resolution = append(json.RawMessage(nil), resolution...)
	}
	if cause != nil {
		candidate.Error = cause.Error()
	}

	if err := r.store.Append(candidate); err != nil {
		return Message{}, err
	}

	r.mu.Lock()
	if latest, ok := r.index[messageID]; ok && IsTerminal(latest.Status) && latest.Status != next {
		r.mu.Unlock()
		return copyMessage(latest), fmt.Errorf("message %q is already terminal (%s)", messageID, latest.Status)
	}
	r.index[messageID] = copyMessage(candidate)
	r.mu.Unlock()
	return copyMessage(candidate), nil
}

// WithMessageLock runs fn while holding the per-message mutation lock.
// Used by Supervisor native permission delivery so freeze + deliver + record
// are serialized without holding the global Router mutex during adapter I/O.
func (r *Router) WithMessageLock(messageID string, fn func() error) error {
	ml := r.lockMessage(messageID)
	ml.Lock()
	defer ml.Unlock()
	return fn()
}

// GetAnsweredResolution re-reads the authoritative Router message and returns the
// persisted Resolution when the message is terminal Answered. Used by waiter
// re-check to recover resolution after a missed wake-up.
func (r *Router) GetAnsweredResolution(messageID string) (Resolution, bool, error) {
	r.mu.Lock()
	value, ok := r.index[messageID]
	r.mu.Unlock()
	if !ok {
		return Resolution{}, false, fmt.Errorf("message %q was not found", messageID)
	}
	if value.Status != Answered {
		return Resolution{}, false, nil
	}
	if len(value.Resolution) == 0 {
		return Resolution{}, false, fmt.Errorf("message %q is answered but has no resolution", messageID)
	}
	res, err := DecodeResolutionForType(value.Type, value.Resolution)
	if err != nil {
		return Resolution{}, false, err
	}
	return res, true, nil
}

// Get returns a copy of a message by id.
func (r *Router) Get(messageID string) (Message, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.index[messageID]
	if !ok {
		return Message{}, false
	}
	return copyMessage(value), true
}

// Snapshot returns messages sorted by CreatedAt/MessageID.
func (r *Router) Snapshot(includeResolved bool) []Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Sorted(copyIndex(r.index), includeResolved)
}

// PendingInstructions returns instructions that still need delivery (StatusQueued).
// Delivered/failed/expired instructions are never returned (no re-send on flush).
func (r *Router) PendingInstructions(taskID string, modes ...DeliveryMode) []Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	modeFilter := map[DeliveryMode]bool{}
	for _, mode := range modes {
		modeFilter[mode] = true
	}
	result := make([]Message, 0)
	for _, value := range r.index {
		if value.TaskID != taskID || !IsDeliveryPending(value) {
			continue
		}
		if len(modeFilter) > 0 && !modeFilter[value.DeliveryMode] {
			continue
		}
		result = append(result, copyMessage(value))
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].MessageID < result[j].MessageID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}

// ExpireTask marks all pending messages for taskID as expired.
// Answered/failed (and other terminal) messages are left unchanged.
func (r *Router) ExpireTask(taskID, reason string) ([]Message, error) {
	if strings.TrimSpace(taskID) == "" {
		return nil, fmt.Errorf("task id is required")
	}
	r.mu.Lock()
	ids := make([]string, 0)
	for id, value := range r.index {
		if value.TaskID == taskID && IsPending(value.Status) {
			ids = append(ids, id)
		}
	}
	r.mu.Unlock()
	sort.Strings(ids)

	var cause error
	if strings.TrimSpace(reason) != "" {
		cause = fmt.Errorf("%s", reason)
	}
	expired := make([]Message, 0, len(ids))
	for _, id := range ids {
		value, err := r.Transition(id, Expired, "", nil, cause)
		if err != nil {
			return expired, err
		}
		expired = append(expired, value)
	}
	return expired, nil
}

// PendingDecisions returns pending non-instruction decision messages for a task.
func (r *Router) PendingDecisions(taskID string) []Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Message, 0)
	for _, value := range r.index {
		if value.TaskID != taskID || !IsDecisionPending(value) {
			continue
		}
		result = append(result, copyMessage(value))
	}
	sort.Slice(result, func(i, j int) bool {
		pi, pj := decisionPriority(result[i].Type), decisionPriority(result[j].Type)
		if pi != pj {
			return pi < pj
		}
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].MessageID < result[j].MessageID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}

// HasPendingDecisions reports whether the task still has blocking questions.
func (r *Router) HasPendingDecisions(taskID string) bool {
	return len(r.PendingDecisions(taskID)) > 0
}

// PendingForTask returns all pending messages for a task.
func (r *Router) PendingForTask(taskID string) []Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Message, 0)
	for _, value := range r.index {
		if value.TaskID == taskID && IsPending(value.Status) {
			result = append(result, copyMessage(value))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].MessageID < result[j].MessageID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}

func decisionPriority(t Type) int {
	switch t {
	case PermissionRequest:
		return 0
	case ScopeExpansionRequest:
		return 1
	case Question:
		return 2
	default:
		return 3
	}
}

func bytesEqualJSON(a, b json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(a), bytes.TrimSpace(b))
}

func copyIndex(values map[string]Message) map[string]Message {
	result := make(map[string]Message, len(values))
	for id, value := range values {
		result[id] = copyMessage(value)
	}
	return result
}

func copyMessage(value Message) Message {
	if value.Payload != nil {
		value.Payload = append(json.RawMessage(nil), value.Payload...)
	}
	if value.Resolution != nil {
		value.Resolution = append(json.RawMessage(nil), value.Resolution...)
	}
	return value
}
