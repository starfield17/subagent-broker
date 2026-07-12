package message

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Router is the durable outbox / inbox state machine for run-scoped messages.
// Persistence always precedes in-memory index updates.
type Router struct {
	mu    sync.Mutex
	runID string
	store *Store
	index map[string]Message
	now   func() time.Time
	newID func(time.Time) (string, error)
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

// RecordDeliveryAttempt increments delivery_attempts and optionally updates status.
func (r *Router) RecordDeliveryAttempt(messageID string, next Status, cause error) (Message, error) {
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
	}
	if err := r.store.Append(candidate); err != nil {
		return Message{}, err
	}
	r.mu.Lock()
	r.index[messageID] = copyMessage(candidate)
	r.mu.Unlock()
	return copyMessage(candidate), nil
}

// Transition validates and persists a status change, then updates memory.
func (r *Router) Transition(
	messageID string,
	next Status,
	deliveryMode DeliveryMode,
	resolution json.RawMessage,
	cause error,
) (Message, error) {
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
		// Idempotent no-op: do not rewrite terminal (or other) records.
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
	r.index[messageID] = copyMessage(candidate)
	r.mu.Unlock()
	return copyMessage(candidate), nil
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
