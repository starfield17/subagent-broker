package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/scope"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
	"github.com/vnai/subagent-broker/internal/task"
	"github.com/vnai/subagent-broker/internal/wave"
)

func (s *Service) Inbox(includeResolved bool) []message.Message {
	if s.router != nil {
		return s.router.Snapshot(includeResolved)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return message.Sorted(s.messageIndex, includeResolved)
}

// decisionOpLock returns the Service-level per-message operation mutex.
// Serializes RequestMessage setup, ResolveMessage, native permission resolution,
// and expiration for a single messageID without holding Router's internal lock
// across adapter I/O.
func (s *Service) decisionOpLock(messageID string) *sync.Mutex {
	v, _ := s.decisionOperationLocks.LoadOrStore(messageID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (s *Service) RequestMessage(ctx context.Context, taskID, workerID string, messageType message.Type, category message.Category, payload any) (message.Resolution, string, error) {
	if s.router == nil {
		return message.Resolution{}, "", fmt.Errorf("message router is not initialized")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return message.Resolution{}, "", err
	}

	// Enrich question payload with real current WriteScope before persist.
	raw, err = s.enrichQuestionPayload(taskID, messageType, raw)
	if err != nil {
		return message.Resolution{}, "", err
	}

	value, err := s.router.EnqueueDecision(taskID, workerID, messageType, category, raw)
	if err != nil {
		return message.Resolution{}, "", err
	}
	// Test hook: after durable enqueue, before operation lock / queued publication.
	s.fireAfterDecisionEnqueue(value.MessageID)

	// Acquire Service decision-operation lock before any pending-state publication
	// so MessageQueued cannot be appended after a concurrent MessageAnswered.
	opLock := s.decisionOpLock(value.MessageID)
	opLock.Lock()
	s.fireAfterDecisionOpLock(value.MessageID)

	// Authoritative re-read before publishing queued projection.
	if res, ok, checkErr := s.loadAnsweredResolution(value.MessageID); checkErr == nil && ok {
		_ = s.refreshQuestionProjection(taskID)
		_ = s.recomputeTaskWaiting(taskID)
		opLock.Unlock()
		return res, value.MessageID, nil
	}
	if current, found := s.router.Get(value.MessageID); found && message.IsTerminal(current.Status) {
		_ = s.refreshQuestionProjection(taskID)
		_ = s.recomputeTaskWaiting(taskID)
		opLock.Unlock()
		return message.Resolution{}, value.MessageID, &message.ErrMessageTerminalNotAnswered{
			MessageID: value.MessageID, Status: current.Status,
		}
	}

	// Commit MessageQueued only while still pending and holding the op lock.
	if err := s.CommitMessageProjection(ctx, value, event.MessageQueued); err != nil {
		opLock.Unlock()
		return message.Resolution{}, value.MessageID, err
	}

	// Register buffered waiter under the operation lock.
	waiter := s.registerDecisionWaiter(value.MessageID)

	// Re-check after projection: resolver may have raced during commit.
	if res, ok, checkErr := s.loadAnsweredResolution(value.MessageID); checkErr == nil && ok {
		s.unregisterDecisionWaiter(value.MessageID, waiter)
		_ = s.refreshQuestionProjection(taskID)
		_ = s.recomputeTaskWaiting(taskID)
		opLock.Unlock()
		return res, value.MessageID, nil
	}
	if current, found := s.router.Get(value.MessageID); found && message.IsTerminal(current.Status) {
		s.unregisterDecisionWaiter(value.MessageID, waiter)
		_ = s.refreshQuestionProjection(taskID)
		_ = s.recomputeTaskWaiting(taskID)
		opLock.Unlock()
		return message.Resolution{}, value.MessageID, &message.ErrMessageTerminalNotAnswered{
			MessageID: value.MessageID, Status: current.Status,
		}
	}

	question := s.questionEnvelopeFor(value)
	taskDir := filepath.Join(s.paths.Tasks, taskID)
	// Always archive; update top-level if this is the highest-priority pending.
	if err := message.PublishQuestionProjection(taskDir, value.MessageID, taskID, question, false); err != nil {
		failed, tErr := s.router.Transition(value.MessageID, message.Failed, "", nil, err)
		if tErr == nil {
			_ = s.CommitMessageProjection(ctx, failed, event.MessageFailed)
		}
		s.unregisterDecisionWaiter(value.MessageID, waiter)
		_ = s.recomputeTaskWaiting(taskID)
		opLock.Unlock()
		return message.Resolution{}, value.MessageID, err
	}
	if err := s.refreshQuestionProjection(taskID); err != nil {
		s.unregisterDecisionWaiter(value.MessageID, waiter)
		_ = s.recomputeTaskWaiting(taskID)
		opLock.Unlock()
		return message.Resolution{}, value.MessageID, err
	}

	if err := s.setTaskWaiting(taskID, messageType); err != nil {
		s.unregisterDecisionWaiter(value.MessageID, waiter)
		opLock.Unlock()
		return message.Resolution{}, value.MessageID, err
	}

	eventType := event.QuestionPublished
	if messageType == message.ScopeExpansionRequest {
		eventType = event.ScopeExpansionRequested
	} else if messageType == message.PermissionRequest {
		eventType = event.PermissionRequested
	}
	if err := s.appendEvent(event.Input{TaskID: taskID, WorkerID: workerID, Source: "message-router", Type: eventType, Severity: "warning", Payload: map[string]any{"message_id": value.MessageID, "type": messageType}}); err != nil {
		s.unregisterDecisionWaiter(value.MessageID, waiter)
		_ = s.recomputeTaskWaiting(taskID)
		opLock.Unlock()
		return message.Resolution{}, value.MessageID, err
	}

	// Final authoritative recheck while still holding the operation lock.
	if res, ok, checkErr := s.loadAnsweredResolution(value.MessageID); checkErr == nil && ok {
		s.unregisterDecisionWaiter(value.MessageID, waiter)
		_ = s.recomputeTaskWaiting(taskID)
		opLock.Unlock()
		return res, value.MessageID, nil
	}
	if current, found := s.router.Get(value.MessageID); found && message.IsTerminal(current.Status) {
		s.unregisterDecisionWaiter(value.MessageID, waiter)
		_ = s.recomputeTaskWaiting(taskID)
		opLock.Unlock()
		return message.Resolution{}, value.MessageID, &message.ErrMessageTerminalNotAnswered{
			MessageID: value.MessageID, Status: current.Status,
		}
	}
	// Message remains pending — release op lock before blocking wait so resolver can proceed.
	opLock.Unlock()

	select {
	case <-waiter.ch:
		// After any wake-up, re-read durable Router state rather than trusting
		// a channel payload. The durable Message.Resolution is authoritative.
		s.unregisterDecisionWaiter(value.MessageID, waiter)
		_ = s.recomputeTaskWaiting(taskID)
		if res, ok, checkErr := s.loadAnsweredResolution(value.MessageID); checkErr == nil && ok {
			return res, value.MessageID, nil
		}
		// Should not happen: if we were notified the message should be Answered.
		// Fall through to return an error rather than hanging.
		return message.Resolution{}, value.MessageID, fmt.Errorf("waiter notified but message %q is not answered", value.MessageID)
	case <-ctx.Done():
		s.unregisterDecisionWaiter(value.MessageID, waiter)
		// Recompute waiting so a concurrent answer does not leave the Task blocked
		// with no pending decisions. Preserve any durable answer.
		_ = s.recomputeTaskWaiting(taskID)
		return message.Resolution{}, value.MessageID, ctx.Err()
	}
}

func (s *Service) ResolveMessage(id string, resolution message.Resolution) error {
	if s.router == nil {
		return fmt.Errorf("message router is not initialized")
	}

	// Hold Service decision-operation lock across the complete application operation:
	// reload → validate → freeze → scope side effect → Answered → notify → recompute.
	// Does not hold Router's global mutex across adapter I/O.
	opLock := s.decisionOpLock(id)
	opLock.Lock()
	defer opLock.Unlock()

	value, ok := s.router.Get(id)
	if !ok {
		// Fallback to index for legacy paths.
		s.mu.Lock()
		value, ok = s.messageIndex[id]
		s.mu.Unlock()
		if !ok {
			return fmt.Errorf("message %q was not found", id)
		}
	}

	resJSON, err := json.Marshal(resolution)
	if err != nil {
		return err
	}

	// Terminal semantics: only Answered + identical resolution is idempotent success.
	if message.IsTerminal(value.Status) {
		if value.Status == message.Answered && message.ResolutionsEqual(value.Resolution, resJSON) {
			return nil
		}
		if value.Status == message.Answered {
			return &message.ErrResolutionConflict{MessageID: id, Status: value.Status}
		}
		return &message.ErrMessageTerminalNotAnswered{MessageID: id, Status: value.Status}
	}
	if value.Type == message.Question && strings.TrimSpace(resolution.Answer) == "" {
		return fmt.Errorf("question answer is required")
	}

	// Native permission: freeze intent, bind exactly, deliver with retryable failures.
	// Claude hooks (no NativePermissionID) use the Answered + waiter path below.
	if isNativePermission(value) {
		return s.resolveNativePermission(context.Background(), value, resolution)
	}

	// 1) Freeze canonical resolution before any side effect.
	frozen, freezeResult, freezeErr := s.router.FreezeResolution(id, resJSON)
	if freezeErr != nil {
		if freezeResult == message.ResolutionAlreadyIdentical {
			// Already frozen with same resolution; continue to complete.
			frozen = value
			if latest, ok := s.router.Get(id); ok {
				frozen = latest
			}
		} else if freezeResult == message.ResolutionConflict {
			return &message.ErrResolutionConflict{MessageID: id, Status: value.Status}
		} else {
			return freezeErr
		}
	}

	// 2) For scope expansion: apply expansion only after freeze confirms allow.
	// Held under the Service op lock so expiration cannot interleave.
	if value.Type == message.ScopeExpansionRequest && resolution.Decision.Allowed {
		if err := s.approveScope(frozen, resolution.Decision); err != nil {
			return err
		}
	}

	// 3) Transition to Answered using the exact frozen resolution.
	answered, err := s.router.Transition(id, message.Answered, "", frozen.Resolution, nil)
	if err != nil {
		// Re-read: may have raced with another path (should not under op lock).
		if latest, ok := s.router.Get(id); ok && message.IsTerminal(latest.Status) {
			if latest.Status == message.Answered && message.ResolutionsEqual(latest.Resolution, resJSON) {
				return nil
			}
			if latest.Status == message.Answered {
				return &message.ErrResolutionConflict{MessageID: id, Status: latest.Status}
			}
			return &message.ErrMessageTerminalNotAnswered{MessageID: id, Status: latest.Status}
		}
		return err
	}
	if err := s.CommitMessageProjection(context.Background(), answered, event.MessageAnswered); err != nil {
		return err
	}

	// 4) Notify waiter (non-blocking; durable state is authoritative; never close channel).
	s.notifyDecisionWaiter(id)

	// Refresh top-level projection: switch to next pending or clear.
	if err := s.refreshQuestionProjection(value.TaskID); err != nil {
		return err
	}
	// Recompute waiting: only clear if no other pending decisions remain.
	if err := s.recomputeTaskWaiting(value.TaskID); err != nil {
		return err
	}

	eventType := event.MessageAnswered
	if value.Type == message.ScopeExpansionRequest {
		eventType = event.ScopeExpansionResolved
	} else if value.Type == message.PermissionRequest {
		eventType = event.PermissionResolved
	}
	if err := s.appendEvent(event.Input{TaskID: value.TaskID, WorkerID: value.WorkerID, Source: "message-router", Type: eventType, Severity: "info", Payload: map[string]any{"message_id": id, "status": string(message.Answered)}}); err != nil {
		return err
	}
	// A resolved decision may remove the Barrier's blocking input. Wake the
	// long-lived Supervisor so it re-evaluates the Barrier automatically.
	if !s.router.HasPendingDecisions(value.TaskID) {
		s.signalAdvance()
	}
	return nil
}

func (s *Service) SendInstruction(ctx context.Context, taskID, text string) (adapter.DeliveryResult, error) {
	if strings.TrimSpace(text) == "" {
		return adapter.DeliveryResult{}, fmt.Errorf("instruction text is required")
	}
	if s.router == nil {
		return adapter.DeliveryResult{}, fmt.Errorf("message router is not initialized")
	}

	// Refuse instructions to terminal tasks.
	if runtime, ok := s.taskState(domain.TaskID(taskID)); ok && recoveryTaskTerminal(runtime) {
		return adapter.DeliveryResult{}, fmt.Errorf("task %s is terminal; cannot enqueue instruction", taskID)
	}

	mode, workerID, attemptNumber := s.selectInstructionRoute(taskID)

	queued, err := s.router.EnqueueInstructionWithAttempt(taskID, workerID, attemptNumber, text, mode)
	if err != nil {
		return adapter.DeliveryResult{}, err
	}
	if err := s.CommitMessageProjection(ctx, queued, event.MessageQueued); err != nil {
		return adapter.DeliveryResult{MessageID: queued.MessageID}, err
	}
	if err := s.appendEvent(event.Input{
		TaskID: taskID, WorkerID: workerID, Source: "message-router", Type: event.MessageQueued, Severity: "info",
		Payload: map[string]any{"message_id": queued.MessageID, "delivery_mode": string(mode), "status": string(message.Queued)},
	}); err != nil {
		return adapter.DeliveryResult{MessageID: queued.MessageID}, err
	}

	return s.deliverInstruction(ctx, queued, text)
}

func (s *Service) effectiveCapsForTask(taskID string, harness adapter.Adapter) adapter.Capabilities {
	if runtime, ok := s.taskState(domain.TaskID(taskID)); ok && runtime.Worker != nil && len(runtime.Worker.Capabilities) > 0 {
		return adapter.CapabilitiesFromMap(runtime.Worker.Capabilities)
	}
	if harness != nil {
		return s.computeSessionCapabilities(harness, domain.Task{TaskID: domain.TaskID(taskID)}).Effective
	}
	return adapter.Capabilities{}
}

func (s *Service) selectInstructionRoute(taskID string) (message.DeliveryMode, string, int) {
	s.mu.Lock()
	active, isActive := s.active[taskID]
	s.mu.Unlock()
	workerID := ""
	attemptNumber := 0

	// Prefer EffectiveCapabilities from the active WorkerSession projection.
	effective := adapter.Capabilities{}
	if runtime, ok := s.taskState(domain.TaskID(taskID)); ok && runtime.Worker != nil {
		effective = adapter.CapabilitiesFromMap(runtime.Worker.Capabilities)
		workerID = string(runtime.Worker.WorkerID)
		attemptNumber = runtime.Worker.Attempt
	}

	if isActive {
		workerID = active.workerID
		attemptNumber = active.attempt
		effective = s.effectiveCapsForTask(taskID, active.adapter)
		if effective.SteerActiveTurn {
			return message.DeliveryImmediate, workerID, attemptNumber
		}
		if effective.BidirectionalStream {
			return message.DeliveryNextTurn, workerID, attemptNumber
		}
		if effective.ResumeSession {
			return message.DeliveryResume, workerID, attemptNumber
		}
	}
	// No active session: resume if native session + effective resume capability.
	if runtime, ok := s.taskState(domain.TaskID(taskID)); ok {
		if runtime.Worker != nil {
			workerID = string(runtime.Worker.WorkerID)
			attemptNumber = runtime.Worker.Attempt
			eff := adapter.CapabilitiesFromMap(runtime.Worker.Capabilities)
			if runtime.Worker.NativeSessionID != "" && !recoveryTaskTerminal(runtime) && eff.ResumeSession {
				return message.DeliveryResume, workerID, attemptNumber
			}
			// Recompute from registry if map empty (legacy workers).
			if len(runtime.Worker.Capabilities) == 0 {
				if harness, ok := s.adapterForTask(runtime.Task, runtime.Worker); ok {
					eff = s.computeSessionCapabilities(harness, runtime.Task).Effective
					if runtime.Worker.NativeSessionID != "" && !recoveryTaskTerminal(runtime) && eff.ResumeSession {
						return message.DeliveryResume, workerID, attemptNumber
					}
				}
			}
		}
	}
	return message.DeliveryUnsupported, workerID, attemptNumber
}

func (s *Service) deliverInstruction(ctx context.Context, queued message.Message, text string) (adapter.DeliveryResult, error) {
	mode := queued.DeliveryMode
	switch mode {
	case message.DeliveryImmediate:
		s.mu.Lock()
		active, isActive := s.active[queued.TaskID]
		s.mu.Unlock()
		if !isActive {
			failed, tErr := s.router.Transition(queued.MessageID, message.Failed, mode, nil, adapter.ErrUnsupported)
			if tErr == nil {
				if cErr := s.CommitMessageProjection(ctx, failed, event.MessageFailed); cErr != nil {
					return adapter.DeliveryResult{Mode: adapter.DeliveryUnsupported, MessageID: queued.MessageID}, cErr
				}
			}
			return adapter.DeliveryResult{Mode: adapter.DeliveryUnsupported, MessageID: queued.MessageID}, adapter.ErrUnsupported
		}
		var sendErr error
		// Use EffectiveCapabilities (from worker map or recomputed session facts).
		eff := s.effectiveCapsForTask(queued.TaskID, active.adapter)
		if eff.SteerActiveTurn {
			_, sendErr = active.adapter.SteerActiveTurn(ctx, active.sessionID, text)
		} else {
			_, sendErr = active.adapter.SendMessage(ctx, active.sessionID, text)
		}
		if sendErr != nil {
			failed, tErr := s.router.RecordDeliveryAttempt(queued.MessageID, message.Failed, sendErr)
			if tErr != nil {
				return adapter.DeliveryResult{Mode: adapter.DeliveryImmediate, MessageID: queued.MessageID}, tErr
			}
			if cErr := s.CommitMessageProjection(ctx, failed, event.MessageFailed); cErr != nil {
				return adapter.DeliveryResult{Mode: adapter.DeliveryImmediate, MessageID: queued.MessageID}, cErr
			}
			return adapter.DeliveryResult{Mode: adapter.DeliveryImmediate, MessageID: queued.MessageID}, sendErr
		}
		delivered, tErr := s.router.RecordDeliveryAttempt(queued.MessageID, message.Delivered, nil)
		if tErr != nil {
			return adapter.DeliveryResult{Mode: adapter.DeliveryImmediate, MessageID: queued.MessageID}, tErr
		}
		if cErr := s.CommitMessageProjection(ctx, delivered, event.MessageDelivered); cErr != nil {
			return adapter.DeliveryResult{Mode: adapter.DeliveryImmediate, MessageID: queued.MessageID}, cErr
		}
		return adapter.DeliveryResult{Mode: adapter.DeliveryImmediate, MessageID: queued.MessageID}, nil

	case message.DeliveryNextTurn:
		// Remain queued until an actual turn boundary flush. Do not claim delivery
		// and do not call the adapter here — FlushInstructionOutbox owns send.
		return adapter.DeliveryResult{Mode: adapter.DeliveryNextTurn, MessageID: queued.MessageID}, nil

	case message.DeliveryResume:
		s.mu.Lock()
		_, isActive := s.active[queued.TaskID]
		s.mu.Unlock()
		if isActive {
			// Session already live: keep outbox semantics (queued until explicit
			// session_resume/recovery flush). Do not create another attempt.
			return adapter.DeliveryResult{Mode: adapter.DeliveryResume, MessageID: queued.MessageID}, nil
		}
		// Inactive task: actively create recovery_resume attempt and deliver.
		if err := s.EnsureResumeAndFlushOutbox(ctx, queued.TaskID); err != nil {
			return adapter.DeliveryResult{Mode: adapter.DeliveryResume, MessageID: queued.MessageID}, err
		}
		// Re-read message status after flush.
		if latest, ok := s.router.Get(queued.MessageID); ok {
			switch latest.Status {
			case message.Delivered:
				return adapter.DeliveryResult{Mode: adapter.DeliveryResume, MessageID: queued.MessageID}, nil
			case message.Failed:
				return adapter.DeliveryResult{Mode: adapter.DeliveryResume, MessageID: queued.MessageID}, fmt.Errorf("%s", latest.Error)
			}
		}
		return adapter.DeliveryResult{Mode: adapter.DeliveryResume, MessageID: queued.MessageID}, nil

	default:
		failed, tErr := s.router.Transition(queued.MessageID, message.Failed, message.DeliveryUnsupported, nil, adapter.ErrUnsupported)
		if tErr != nil {
			return adapter.DeliveryResult{Mode: adapter.DeliveryUnsupported, MessageID: queued.MessageID}, tErr
		}
		if cErr := s.CommitMessageProjection(ctx, failed, event.MessageFailed); cErr != nil {
			return adapter.DeliveryResult{Mode: adapter.DeliveryUnsupported, MessageID: queued.MessageID}, cErr
		}
		return adapter.DeliveryResult{Mode: adapter.DeliveryUnsupported, MessageID: queued.MessageID}, adapter.ErrUnsupported
	}
}

// startNextTurnAtBoundary starts at most one queued next_turn instruction after
// a successful ResultSubmitted. FIFO uses Router.PendingInstructions order.
// processExited forces failure of any attempt to send on a dead session.
func (s *Service) startNextTurnAtBoundary(ctx context.Context, taskID string, processExited bool) TurnBoundaryResult {
	if s.router == nil {
		return TurnBoundaryResult{}
	}
	if runtime, ok := s.taskState(domain.TaskID(taskID)); ok && recoveryTaskTerminal(runtime) {
		return TurnBoundaryResult{}
	}
	pending := s.router.PendingInstructions(taskID, message.DeliveryNextTurn)
	if len(pending) == 0 {
		return TurnBoundaryResult{}
	}
	// Exactly one instruction per boundary (oldest first).
	item := pending[0]
	if !message.IsDeliveryPending(item) {
		return TurnBoundaryResult{}
	}
	text := instructionText(item)

	if processExited {
		cause := fmt.Errorf("process already exited; cannot start in-process next turn: %w", adapter.ErrUnsupported)
		failed, tErr := s.router.RecordDeliveryAttempt(item.MessageID, message.Failed, cause)
		if tErr == nil {
			_ = s.CommitMessageProjection(ctx, failed, event.MessageFailed)
		}
		return TurnBoundaryResult{}
	}

	s.mu.Lock()
	active, isActive := s.active[taskID]
	s.mu.Unlock()
	if !isActive {
		// Not live: reclassify to resume or fail; never claim next turn started.
		if err := s.handleInactiveQueuedInstruction(ctx, item); err != nil {
			// handleInactive may convert to resume; still not an in-process next turn.
			_ = err
		}
		return TurnBoundaryResult{}
	}

	// Accept prompt first; only then record Delivered (durable ordering).
	_, sendErr := active.adapter.SendMessage(ctx, active.sessionID, text)
	if sendErr != nil {
		failed, tErr := s.router.RecordDeliveryAttempt(item.MessageID, message.Failed, sendErr)
		if tErr == nil {
			_ = s.CommitMessageProjection(ctx, failed, event.MessageFailed)
		}
		// First result remains final; do not claim next turn started.
		return TurnBoundaryResult{}
	}
	delivered, tErr := s.router.RecordDeliveryAttempt(item.MessageID, message.Delivered, nil)
	if tErr != nil {
		return TurnBoundaryResult{}
	}
	if cErr := s.CommitMessageProjection(ctx, delivered, event.MessageDelivered); cErr != nil {
		return TurnBoundaryResult{}
	}
	_ = s.appendEvent(event.Input{
		TaskID: taskID, WorkerID: active.workerID, Source: "message-router",
		Type: event.InstructionFlushed, Severity: "info",
		Payload: map[string]any{
			"message_id":        item.MessageID,
			"trigger":           "turn_boundary",
			"started_next_turn": true,
			"delivery_mode":     string(message.DeliveryNextTurn),
		},
	})
	return TurnBoundaryResult{StartedNextTurn: true, MessageID: item.MessageID}
}

// FlushInstructionOutbox delivers queued next_turn/resume instructions for a task.
// turn_boundary next_turn starts are owned by startNextTurnAtBoundary via runWorkerSession;
// this path still supports active_session immediate flushes and resume delivery.
func (s *Service) FlushInstructionOutbox(ctx context.Context, taskID, trigger string) error {
	if s.router == nil {
		return nil
	}
	if runtime, ok := s.taskState(domain.TaskID(taskID)); ok && recoveryTaskTerminal(runtime) {
		if _, err := s.expireTaskMessages(taskID, "task terminal during flush"); err != nil {
			return err
		}
		return nil
	}

	switch trigger {
	case "turn_boundary":
		// Lifecycle-owned by runWorkerSession.startNextTurnAtBoundary; avoid double-start.
		// Immediate-only residual flush (steer leftovers).
		pending := s.router.PendingInstructions(taskID, message.DeliveryImmediate)
		var firstErr error
		for _, item := range pending {
			if message.IsTerminal(item.Status) || !message.IsDeliveryPending(item) {
				continue
			}
			text := instructionText(item)
			if _, err := s.deliverInstruction(ctx, item, text); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				if !s.AcceptingWork() {
					return err
				}
				continue
			}
			if err := s.appendEvent(event.Input{TaskID: taskID, Source: "message-router", Type: event.InstructionFlushed, Severity: "info", Payload: map[string]any{"message_id": item.MessageID, "trigger": trigger}}); err != nil {
				return err
			}
		}
		return firstErr
	case "active_session":
		pending := s.router.PendingInstructions(taskID, message.DeliveryNextTurn, message.DeliveryImmediate)
		var firstErr error
		for _, item := range pending {
			if message.IsTerminal(item.Status) || !message.IsDeliveryPending(item) {
				continue
			}
			text := instructionText(item)
			var err error
			switch item.DeliveryMode {
			case message.DeliveryImmediate:
				_, err = s.deliverInstruction(ctx, item, text)
			case message.DeliveryNextTurn:
				err = s.deliverQueuedToActiveSession(ctx, item, text)
			default:
				continue
			}
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				if !s.AcceptingWork() {
					return err
				}
				continue
			}
			if err := s.appendEvent(event.Input{TaskID: taskID, Source: "message-router", Type: event.InstructionFlushed, Severity: "info", Payload: map[string]any{"message_id": item.MessageID, "trigger": trigger}}); err != nil {
				return err
			}
		}
		return firstErr
	case "session_resume", "recovery":
		pending := s.router.PendingInstructions(taskID, message.DeliveryResume)
		if len(pending) == 0 {
			return nil
		}
		s.mu.Lock()
		active, isActive := s.active[taskID]
		s.mu.Unlock()
		if !isActive {
			// Active resume entry point handles creating the recovery_resume attempt.
			return s.EnsureResumeAndFlushOutbox(ctx, taskID)
		}
		var firstErr error
		for _, item := range pending {
			// Skip already-delivered (PendingInstructions already filters, but double-check).
			if message.IsTerminal(item.Status) {
				continue
			}
			text := instructionText(item)
			// Physical send on the active resumed session (retain durable resume mode).
			if err := s.deliverQueuedToActiveSession(ctx, item, text); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				if !s.AcceptingWork() {
					return err
				}
				continue
			}
			if err := s.appendEvent(event.Input{TaskID: taskID, WorkerID: active.workerID, Source: "message-router", Type: event.InstructionFlushed, Severity: "info", Payload: map[string]any{"message_id": item.MessageID, "trigger": trigger}}); err != nil {
				return err
			}
		}
		return firstErr
	}
	return nil
}

// deliverQueuedToActiveSession physically delivers a queued instruction to the
// active adapter session via SendMessage. Used at turn boundaries and resume
// flush so next_turn never recursively no-ops. On success records Delivered;
// on failure records Failed. If no active session remains but resume is
// possible, reclassifies next_turn → resume; otherwise fails explicitly.
func (s *Service) deliverQueuedToActiveSession(ctx context.Context, queued message.Message, text string) error {
	if s.router == nil {
		return fmt.Errorf("message router is not initialized")
	}
	// Re-read: already delivered messages must not be sent again.
	if latest, ok := s.router.Get(queued.MessageID); ok {
		if !message.IsDeliveryPending(latest) {
			return nil
		}
		queued = latest
	}

	s.mu.Lock()
	active, isActive := s.active[queued.TaskID]
	s.mu.Unlock()

	if !isActive {
		return s.handleInactiveQueuedInstruction(ctx, queued)
	}

	_, sendErr := active.adapter.SendMessage(ctx, active.sessionID, text)
	if sendErr != nil {
		failed, tErr := s.router.RecordDeliveryAttempt(queued.MessageID, message.Failed, sendErr)
		if tErr != nil {
			return tErr
		}
		if cErr := s.CommitMessageProjection(ctx, failed, event.MessageFailed); cErr != nil {
			return cErr
		}
		return sendErr
	}
	delivered, tErr := s.router.RecordDeliveryAttempt(queued.MessageID, message.Delivered, nil)
	if tErr != nil {
		return tErr
	}
	if cErr := s.CommitMessageProjection(ctx, delivered, event.MessageDelivered); cErr != nil {
		return cErr
	}
	return nil
}

// handleInactiveQueuedInstruction reclassifies next_turn → resume when possible,
// otherwise fails the instruction explicitly (never silent loss).
func (s *Service) handleInactiveQueuedInstruction(ctx context.Context, queued message.Message) error {
	if queued.DeliveryMode == message.DeliveryResume {
		// Resume flush without active session is owned by EnsureResumeAndFlushOutbox.
		return s.EnsureResumeAndFlushOutbox(ctx, queued.TaskID)
	}

	canResume := false
	if runtime, ok := s.taskState(domain.TaskID(queued.TaskID)); ok && !recoveryTaskTerminal(runtime) {
		eff := s.effectiveCapsForTask(queued.TaskID, nil)
		native := ""
		if runtime.Worker != nil {
			native = runtime.Worker.NativeSessionID
			if len(runtime.Worker.Capabilities) > 0 {
				eff = adapter.CapabilitiesFromMap(runtime.Worker.Capabilities)
			}
		}
		if native != "" && eff.ResumeSession {
			canResume = true
		}
	}
	if canResume && queued.DeliveryMode == message.DeliveryNextTurn {
		reclass, err := s.router.ReclassifyDelivery(queued.MessageID, message.DeliveryResume)
		if err != nil {
			return err
		}
		if cErr := s.CommitMessageProjection(ctx, reclass, event.MessageQueued); cErr != nil {
			return cErr
		}
		return s.EnsureResumeAndFlushOutbox(ctx, queued.TaskID)
	}

	cause := fmt.Errorf("no active session for next_turn delivery and resume unavailable: %w", adapter.ErrUnsupported)
	failed, tErr := s.router.RecordDeliveryAttempt(queued.MessageID, message.Failed, cause)
	if tErr != nil {
		return tErr
	}
	if cErr := s.CommitMessageProjection(ctx, failed, event.MessageFailed); cErr != nil {
		return cErr
	}
	return cause
}

func instructionText(value message.Message) string {
	var payload message.InstructionPayload
	if json.Unmarshal(value.Payload, &payload) == nil {
		return payload.Text
	}
	return string(value.Payload)
}

// expireTaskMessages expires pending messages for a task under Service-level
// per-message serialization. Messages with a frozen resolution are not silently
// expired — they surface as reconciliation-required and remain queued.
func (s *Service) expireTaskMessages(taskID, reason string) ([]message.Message, error) {
	if s.router == nil {
		return nil, nil
	}
	// Enumerate candidates without holding Service op locks, then expire one-by-one.
	candidates := s.router.PendingDecisions(taskID)
	// Also include pending non-decision messages (instructions etc.) via Snapshot.
	all := s.router.Snapshot(true)
	ids := make([]string, 0)
	seen := map[string]bool{}
	for _, item := range candidates {
		if !seen[item.MessageID] && message.IsPending(item.Status) {
			ids = append(ids, item.MessageID)
			seen[item.MessageID] = true
		}
	}
	for _, item := range all {
		if item.TaskID != taskID || !message.IsPending(item.Status) || seen[item.MessageID] {
			continue
		}
		ids = append(ids, item.MessageID)
		seen[item.MessageID] = true
	}
	sort.Strings(ids) // stable lock order

	var cause error
	if strings.TrimSpace(reason) != "" {
		cause = fmt.Errorf("%s", reason)
	}
	expired := make([]message.Message, 0, len(ids))
	var firstReconcile error
	for _, id := range ids {
		item, err := s.expireOneMessage(id, cause)
		if err != nil {
			var recon *message.ErrResolutionReconciliationRequired
			if errors.As(err, &recon) {
				if firstReconcile == nil {
					firstReconcile = err
				}
				continue
			}
			return expired, err
		}
		if item.MessageID != "" {
			expired = append(expired, item)
		}
	}
	if err := message.ClearTopLevelQuestion(filepath.Join(s.paths.Tasks, taskID)); err != nil {
		return expired, err
	}
	if err := s.recomputeTaskWaiting(taskID); err != nil {
		return expired, err
	}
	if firstReconcile != nil && len(expired) == 0 {
		return expired, firstReconcile
	}
	return expired, nil
}

// expireOneMessage acquires the Service decision-operation lock, re-reads
// authoritative state, and expires only when safe.
func (s *Service) expireOneMessage(id string, cause error) (message.Message, error) {
	opLock := s.decisionOpLock(id)
	opLock.Lock()
	defer opLock.Unlock()

	value, ok := s.router.Get(id)
	if !ok {
		return message.Message{}, nil
	}
	if message.IsTerminal(value.Status) {
		return message.Message{}, nil
	}
	// Queued decision with frozen resolution must not be silently expired.
	if len(value.Resolution) > 0 {
		return message.Message{}, &message.ErrResolutionReconciliationRequired{
			MessageID: id,
			Reason:    "frozen resolution present; refuse silent expiration",
		}
	}
	item, err := s.router.Transition(id, message.Expired, "", nil, cause)
	if err != nil {
		// Terminal race under lock is unexpected; re-read.
		if latest, ok := s.router.Get(id); ok && message.IsTerminal(latest.Status) {
			return message.Message{}, nil
		}
		return message.Message{}, err
	}
	if cErr := s.CommitMessageProjection(context.Background(), item, event.MessageExpired); cErr != nil {
		return item, cErr
	}
	s.notifyDecisionWaiter(item.MessageID)
	return item, nil
}

func (s *Service) setTaskWaiting(taskID string, kind message.Type) error {
	if s.events == nil {
		// Lightweight path for unit tests without an event sink.
		s.mu.Lock()
		defer s.mu.Unlock()
		for index := range s.snapshot.Tasks {
			if string(s.snapshot.Tasks[index].Task.TaskID) != taskID {
				continue
			}
			s.snapshot.Tasks[index].Task.Status = state.TaskBlocked
			s.snapshot.Tasks[index].Dimensions.Task = state.TaskBlocked
			s.snapshot.Tasks[index].BlockKind = BlockKindWaitingMessage
			return nil
		}
		return nil
	}
	return s.commitMutate(context.Background(), event.Input{
		TaskID: taskID, Source: "message-router", Type: event.TaskWaitingRecomputed, Severity: "warning",
		Payload: map[string]any{"from": string(state.TaskRunning), "to": string(state.TaskBlocked), "reason": "pending_message", "message_type": string(kind)},
	}, func(candidate *Snapshot) error {
		index, err := findTaskIndex(candidate, domain.TaskID(taskID))
		if err != nil {
			return err
		}
		candidate.Tasks[index].Task.Status = state.TaskBlocked
		candidate.Tasks[index].Dimensions.Task = state.TaskBlocked
		candidate.Tasks[index].Dimensions.Progress = state.ProgressQuiet
		candidate.Tasks[index].BlockKind = BlockKindWaitingMessage
		switch kind {
		case message.ScopeExpansionRequest:
			candidate.Tasks[index].Dimensions.Protocol = state.ProtocolWaitingScope
		case message.PermissionRequest:
			candidate.Tasks[index].Dimensions.Protocol = state.ProtocolWaitingPermission
		default:
			candidate.Tasks[index].Dimensions.Protocol = state.ProtocolWaitingUser
		}
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
}

func (s *Service) recomputeTaskWaiting(taskID string) error {
	hasPending := s.router != nil && s.router.HasPendingDecisions(taskID)
	if s.events == nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		for index := range s.snapshot.Tasks {
			if string(s.snapshot.Tasks[index].Task.TaskID) != taskID {
				continue
			}
			if !hasPending && s.snapshot.Tasks[index].BlockKind != BlockKindFinal {
				if s.snapshot.Tasks[index].Task.Status == state.TaskBlocked {
					s.snapshot.Tasks[index].Task.Status = state.TaskRunning
					s.snapshot.Tasks[index].Dimensions.Task = state.TaskRunning
					s.snapshot.Tasks[index].BlockKind = BlockKindNone
				}
			}
			return nil
		}
		return nil
	}
	return s.commitMutate(context.Background(), event.Input{
		TaskID: taskID, Source: "message-router", Type: event.TaskWaitingRecomputed, Severity: "info",
		Payload: map[string]any{"has_pending": hasPending, "reason": "recompute_after_resolve"},
	}, func(candidate *Snapshot) error {
		index, err := findTaskIndex(candidate, domain.TaskID(taskID))
		if err != nil {
			return err
		}
		if hasPending {
			// Stay blocked; refresh protocol from highest priority pending.
			// Failed native permission delivery remains pending and must keep
			// Task blocked / waiting_permission / quiet (never false running).
			pending := s.router.PendingDecisions(taskID)
			if len(pending) > 0 {
				candidate.Tasks[index].Task.Status = state.TaskBlocked
				candidate.Tasks[index].Dimensions.Task = state.TaskBlocked
				candidate.Tasks[index].Dimensions.Progress = state.ProgressQuiet
				candidate.Tasks[index].BlockKind = BlockKindWaitingMessage
				switch pending[0].Type {
				case message.ScopeExpansionRequest:
					candidate.Tasks[index].Dimensions.Protocol = state.ProtocolWaitingScope
				case message.PermissionRequest:
					candidate.Tasks[index].Dimensions.Protocol = state.ProtocolWaitingPermission
				default:
					candidate.Tasks[index].Dimensions.Protocol = state.ProtocolWaitingUser
				}
			}
			candidate.UpdatedAt = time.Now().UTC()
			return nil
		}
		// No pending decisions: clear waiting if worker still active and not final-blocked.
		if candidate.Tasks[index].BlockKind == BlockKindFinal {
			candidate.UpdatedAt = time.Now().UTC()
			return nil
		}
		if candidate.Tasks[index].Task.Status == state.TaskBlocked {
			if candidate.Tasks[index].Worker != nil && candidate.Tasks[index].Worker.ExitCode == nil {
				candidate.Tasks[index].Task.Status = state.TaskRunning
				candidate.Tasks[index].Dimensions.Task = state.TaskRunning
				candidate.Tasks[index].Dimensions.Protocol = state.ProtocolThinking
				candidate.Tasks[index].Dimensions.Progress = state.ProgressActive
				candidate.Tasks[index].BlockKind = BlockKindNone
			} else if candidate.Tasks[index].BlockKind == BlockKindWaitingMessage {
				// Worker already exited while waiting — leave status for recovery/result path.
				candidate.Tasks[index].BlockKind = BlockKindNone
			}
		}
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
}

func (s *Service) refreshQuestionProjection(taskID string) error {
	taskDir := filepath.Join(s.paths.Tasks, taskID)
	if s.router == nil {
		return message.ClearTopLevelQuestion(taskDir)
	}
	pending := s.router.PendingDecisions(taskID)
	if len(pending) == 0 {
		return message.ClearTopLevelQuestion(taskDir)
	}
	current := pending[0]
	envelope := s.questionEnvelopeFor(current)
	return message.PublishQuestionProjection(taskDir, current.MessageID, taskID, envelope, true)
}

func (s *Service) enrichQuestionPayload(taskID string, messageType message.Type, raw json.RawMessage) (json.RawMessage, error) {
	writeScope := s.taskWriteScope(taskID)
	if messageType == message.Question {
		var env message.QuestionEnvelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return raw, nil
		}
		if len(writeScope) > 0 {
			env.CurrentScope = append([]string(nil), writeScope...)
		}
		if env.SchemaVersion == "" {
			env.SchemaVersion = SchemaVersion
		}
		return json.Marshal(env)
	}
	return raw, nil
}

func (s *Service) taskWriteScope(taskID string) []string {
	if runtime, ok := s.taskState(domain.TaskID(taskID)); ok {
		return append([]string(nil), runtime.Task.WriteScope...)
	}
	return nil
}

func (s *Service) questionEnvelopeFor(value message.Message) message.QuestionEnvelope {
	scope := s.taskWriteScope(value.TaskID)
	if len(scope) == 0 {
		scope = []string{"(no write scope declared)"}
	}
	result := message.QuestionEnvelope{
		SchemaVersion:  SchemaVersion,
		CurrentScope:   scope,
		WorkspaceState: "The Worker is waiting and has not been authorized to proceed.",
	}
	switch value.Type {
	case message.ScopeExpansionRequest:
		var payload message.ScopeRequestPayload
		_ = json.Unmarshal(value.Payload, &payload)
		result.Question = "May this Task expand its write scope?"
		result.Reason = payload.Reason + " Consequence: " + payload.Consequence
		result.RequestedScope = payload.RequestedScope
		result.RelatedTasks = payload.RelatedTasks
		result.Suggestion = payload.Recommendation
	case message.PermissionRequest:
		var payload message.PermissionRequestPayload
		_ = json.Unmarshal(value.Payload, &payload)
		result.Question = "May the Worker use " + payload.ToolName + "?"
		result.Reason = "The Harness requested permission for a tool call."
		// When a decision is frozen but native delivery is still pending, say so.
		if len(value.Resolution) > 0 && !message.IsTerminal(value.Status) {
			result.Reason = "Decision recorded; native delivery is pending/requires retry."
			if value.Error != "" {
				result.Suggestion = "Last delivery error: " + value.Error
			}
		}
	default:
		var payload message.QuestionEnvelope
		if json.Unmarshal(value.Payload, &payload) == nil {
			if len(payload.CurrentScope) == 0 || (len(payload.CurrentScope) == 1 && payload.CurrentScope[0] == "See Task Contract") {
				payload.CurrentScope = scope
			}
			if payload.SchemaVersion == "" {
				payload.SchemaVersion = SchemaVersion
			}
			if payload.WorkspaceState == "" {
				payload.WorkspaceState = result.WorkspaceState
			}
			return payload
		}
		result.Question = "The Worker requested Main Agent input."
		result.Reason = "The Worker cannot continue without an answer."
	}
	return result
}

// CommitMessageProjection updates the in-memory message index and persists the
// Snapshot message projection via the existing Commit API. Snapshot.Messages is
// never written directly on the production path; only the Commit candidate is mutated.
// Router journal append must already have succeeded before calling this.
// On Commit failure the Supervisor fail-closes and the error is returned (not swallowed).
func (s *Service) CommitMessageProjection(ctx context.Context, value message.Message, eventType string) error {
	s.mu.Lock()
	if s.messageIndex == nil {
		s.messageIndex = map[string]message.Message{}
	}
	s.messageIndex[value.MessageID] = value
	s.mu.Unlock()

	if eventType == "" {
		eventType = event.MessageQueued
		switch value.Status {
		case message.Delivered:
			eventType = event.MessageDelivered
		case message.Answered:
			eventType = event.MessageAnswered
		case message.Expired:
			eventType = event.MessageExpired
		case message.Failed:
			eventType = event.MessageFailed
		case message.Acknowledged:
			eventType = event.MessageAcknowledged
		}
	}

	// Lightweight unit-test path without an event sink: update snapshot under lock only.
	if s.events == nil {
		s.mu.Lock()
		if s.router != nil {
			s.snapshot.Messages = s.router.Snapshot(false)
		}
		s.mu.Unlock()
		return nil
	}
	if !s.AcceptingWork() {
		return &CommitError{Stage: CommitStageValidate, Err: fmt.Errorf("supervisor is not accepting work after a fatal persistence failure")}
	}
	return s.commitMutate(ctx, event.Input{
		TaskID: value.TaskID, Source: "message-router", Type: eventType, Severity: "info",
		Payload: map[string]any{"message_id": value.MessageID, "status": string(value.Status), "reason": "projection_sync"},
	}, func(candidate *Snapshot) error {
		if s.router != nil {
			candidate.Messages = s.router.Snapshot(false)
		}
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
}

// syncMessageProjection is retained for tests; production paths must use CommitMessageProjection.
func (s *Service) syncMessageProjection(value message.Message) {
	_ = s.CommitMessageProjection(context.Background(), value, "")
}

func (s *Service) approveScope(value message.Message, decision message.DecisionPayload) error {
	var request message.ScopeRequestPayload
	if err := json.Unmarshal(value.Payload, &request); err != nil {
		return err
	}
	if len(request.RequestedScope) == 0 {
		return fmt.Errorf("scope request has no requested paths")
	}
	return s.commitMutate(context.Background(), event.Input{
		TaskID: value.TaskID, Source: "message-router", Type: event.ScopeExpansionResolved, Severity: "info",
		Payload: map[string]any{"message_id": value.MessageID, "allowed": true, "paths": request.RequestedScope},
	}, func(candidate *Snapshot) error {
		index, err := findTaskIndex(candidate, domain.TaskID(value.TaskID))
		if err != nil {
			return err
		}
		target := &candidate.Tasks[index]

		// Idempotency: track which scopes are already present.
		existing := map[string]bool{}
		for _, s := range target.Task.WriteScope {
			existing[s] = true
		}
		alreadyExpanded := true
		for _, requested := range request.RequestedScope {
			if _, err := scope.Compile(requested); err != nil {
				return err
			}
			for _, forbidden := range target.Task.ForbiddenScope {
				overlap, err := scope.MayOverlap(requested, forbidden)
				if err != nil || overlap {
					return fmt.Errorf("requested scope %q conflicts with forbidden scope %q", requested, forbidden)
				}
			}
			if !existing[requested] {
				alreadyExpanded = false
			}
		}
		if alreadyExpanded && target.Task.AllowPublicInterfaceChange == decision.AllowPublicInterfaceChange {
			// Scope already expanded identically; skip duplicate work.
			return nil
		}

		if request.RequiresPublicInterfaceChange && !decision.AllowPublicInterfaceChange {
			return fmt.Errorf("scope request requires explicit public-interface approval")
		}
		if !alreadyExpanded {
			target.Task.WriteScope = append(target.Task.WriteScope, request.RequestedScope...)
		}
		if decision.AllowPublicInterfaceChange {
			target.Task.AllowPublicInterfaceChange = true
		}
		var sameWave []domain.Task
		for _, runtime := range candidate.Tasks {
			if runtime.Task.WaveID == target.Task.WaveID {
				if runtime.Task.TaskID == target.Task.TaskID {
					sameWave = append(sameWave, target.Task)
				} else {
					sameWave = append(sameWave, runtime.Task)
				}
			}
		}
		result := wave.Preflight(sameWave)
		if !result.Allowed {
			return fmt.Errorf("expanded scope fails Wave preflight: %s", formatPreflightIssues(result.Issues))
		}
		contract, err := task.RenderContract(target.Task, candidate.Run.RunID)
		if err != nil {
			return err
		}
		layout, _ := storage.NewLayout(s.config.BrokerHome)
		paths, _ := layout.TaskPaths(string(candidate.Run.ProjectID), string(candidate.Run.RunID), value.TaskID)
		if err := storage.AtomicWriteFile(paths.Contract, []byte(contract), 0o600); err != nil {
			return err
		}
		if err := storage.AtomicWriteJSON(s.wavePaths(target.Task.WaveID).Preflight, result, 0o600); err != nil {
			return err
		}
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
}

// Used by finish/fail/cancel paths — expire zombies. Errors are returned (fail-closed).
func (s *Service) onTaskTerminalMessages(taskID, reason string) error {
	_, err := s.expireTaskMessages(taskID, reason)
	return err
}

// EnsureResumeAndFlushOutbox is the single active entry for resume delivery:
// load queued resume instructions, create one recovery_resume attempt when needed,
// ResumeSession, register the active worker, and flush outbox in order.
// Concurrent callers for the same task are serialized via the Service mutex / active map CAS.
func (s *Service) EnsureResumeAndFlushOutbox(ctx context.Context, taskID string) error {
	if s.router == nil {
		return fmt.Errorf("message router is not initialized")
	}
	pending := s.router.PendingInstructions(taskID, message.DeliveryResume)
	if len(pending) == 0 {
		return nil
	}

	// Already active: flush on the live session without creating another attempt.
	s.mu.Lock()
	_, isActive := s.active[taskID]
	s.mu.Unlock()
	if isActive {
		return s.flushResumeOnActive(ctx, taskID)
	}

	runtime, ok := s.taskState(domain.TaskID(taskID))
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}
	if recoveryTaskTerminal(runtime) {
		return s.onTaskTerminalMessages(taskID, "task terminal during resume")
	}

	// Singleflight / CAS: claim resume-in-progress slot so concurrent callers
	// do not create multiple recovery_resume attempts.
	s.mu.Lock()
	if s.resumeInFlight == nil {
		s.resumeInFlight = map[string]bool{}
	}
	if s.resumeInFlight[taskID] {
		s.mu.Unlock()
		// Another caller owns resume; wait for active session or completion.
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
			s.mu.Lock()
			_, activeNow := s.active[taskID]
			inFlight := s.resumeInFlight[taskID]
			s.mu.Unlock()
			if activeNow {
				return s.flushResumeOnActive(ctx, taskID)
			}
			if !inFlight {
				// Owner finished: if outbox empty, succeed without a second attempt.
				if len(s.router.PendingInstructions(taskID, message.DeliveryResume)) == 0 {
					return nil
				}
				// Remaining queued resume instructions: try again from the top.
				return s.EnsureResumeAndFlushOutbox(ctx, taskID)
			}
		}
		return fmt.Errorf("concurrent resume in progress for task %s", taskID)
	}
	s.resumeInFlight[taskID] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.resumeInFlight, taskID)
		s.mu.Unlock()
	}()

	// Validate native session + Effective.ResumeSession.
	nativeSessionID := ""
	if runtime.Worker != nil {
		nativeSessionID = runtime.Worker.NativeSessionID
	}
	if nativeSessionID == "" {
		for i := len(runtime.Attempts) - 1; i >= 0; i-- {
			if runtime.Attempts[i].Worker.NativeSessionID != "" {
				nativeSessionID = runtime.Attempts[i].Worker.NativeSessionID
				break
			}
		}
	}
	if nativeSessionID == "" {
		return s.failQueuedResumeInstructions(taskID, adapter.ErrUnsupported, "native session missing")
	}

	harness, ok := s.adapterForTask(runtime.Task, runtime.Worker)
	if !ok {
		return s.failQueuedResumeInstructions(taskID, adapter.ErrUnsupported, "adapter not registered")
	}
	eff := s.effectiveCapsForTask(taskID, harness)
	if !eff.ResumeSession {
		return s.failQueuedResumeInstructions(taskID, adapter.ErrUnsupported, "resume capability unsupported")
	}

	// Refuse resume when process state is unknown/orphaned — cannot invent exited.
	if runtime.Dimensions.Process == state.ProcessUnknown || runtime.Dimensions.Process == state.ProcessOrphaned {
		return s.failQueuedResumeInstructions(taskID, adapter.ErrUnsupported,
			fmt.Sprintf("process state %s forbids resume", runtime.Dimensions.Process))
	}
	// Ensure state selects recovery_resume (never fresh) inside executeTask.
	if runtime.Worker == nil {
		runtime.Worker = &domain.WorkerSession{TaskID: runtime.Task.TaskID}
	}
	if runtime.Worker.NativeSessionID == "" {
		runtime.Worker.NativeSessionID = nativeSessionID
	}
	// Only mark exited when already exited or we have a proven exit path.
	if runtime.Dimensions.Process != state.ProcessExited {
		return s.failQueuedResumeInstructions(taskID, adapter.ErrUnsupported,
			fmt.Sprintf("process state %s is not recovery-resumable", runtime.Dimensions.Process))
	}
	runtime.Worker.StatusDimensions.Process = state.ProcessExited
	if err := s.saveRuntime(runtime); err != nil {
		return err
	}
	// Full Worker Session lifecycle: attempt create → ResumeSession → register →
	// outbox flush → Events/stderr drain → Exited → result → finish → unregister.
	// Reuses executeTask; does not start a second lightweight path.
	return s.executeTask(ctx, &runtime)
}

func (s *Service) failQueuedResumeInstructions(taskID string, cause error, reason string) error {
	pending := s.router.PendingInstructions(taskID, message.DeliveryResume)
	var firstErr error
	for _, item := range pending {
		failed, tErr := s.router.Transition(item.MessageID, message.Failed, message.DeliveryUnsupported, nil, fmt.Errorf("%s: %w", reason, cause))
		if tErr != nil {
			if firstErr == nil {
				firstErr = tErr
			}
			continue
		}
		if cErr := s.CommitMessageProjection(context.Background(), failed, event.MessageFailed); cErr != nil {
			return cErr
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return fmt.Errorf("%s: %w", reason, cause)
}

// flushResumeOnActive delivers pending resume instructions on an already-registered
// active session via physical SendMessage (not the next_turn no-op path).
func (s *Service) flushResumeOnActive(ctx context.Context, taskID string) error {
	if s.router == nil {
		return nil
	}
	pending := s.router.PendingInstructions(taskID, message.DeliveryResume)
	s.mu.Lock()
	active, isActive := s.active[taskID]
	s.mu.Unlock()
	if !isActive {
		return fmt.Errorf("no active session for resume flush on task %s", taskID)
	}
	var firstErr error
	for _, item := range pending {
		if message.IsTerminal(item.Status) || !message.IsDeliveryPending(item) {
			continue
		}
		text := instructionText(item)
		if err := s.deliverQueuedToActiveSession(ctx, item, text); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if !s.AcceptingWork() {
				return err
			}
			continue
		}
		if err := s.appendEvent(event.Input{
			TaskID: taskID, WorkerID: active.workerID, Source: "message-router",
			Type: event.InstructionFlushed, Severity: "info",
			Payload: map[string]any{"message_id": item.MessageID, "trigger": "ensure_resume"},
		}); err != nil {
			return err
		}
	}
	return firstErr
}
