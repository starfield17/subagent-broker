package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
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
	workerpkg "github.com/vnai/subagent-broker/internal/worker"
)

func (s *Service) Inbox(includeResolved bool) []message.Message {
	if s.router != nil {
		return s.router.Snapshot(includeResolved)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return message.Sorted(s.messageIndex, includeResolved)
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
	s.syncMessageProjection(value)
	_ = s.appendEvent(event.Input{
		TaskID: taskID, WorkerID: workerID, Source: "message-router",
		Type: event.MessageQueued, Severity: "info",
		Payload: map[string]any{"message_id": value.MessageID, "type": messageType, "status": string(message.Queued)},
	})

	question := s.questionEnvelopeFor(value)
	taskDir := filepath.Join(s.paths.Tasks, taskID)
	// Always archive; update top-level if this is the highest-priority pending.
	if err := message.PublishQuestionProjection(taskDir, value.MessageID, taskID, question, false); err != nil {
		failed, tErr := s.router.Transition(value.MessageID, message.Failed, "", nil, err)
		if tErr == nil {
			s.syncMessageProjection(failed)
		}
		return message.Resolution{}, value.MessageID, err
	}
	if err := s.refreshQuestionProjection(taskID); err != nil {
		return message.Resolution{}, value.MessageID, err
	}

	wait := make(chan message.Resolution, 1)
	s.mu.Lock()
	s.pending[value.MessageID] = wait
	s.mu.Unlock()

	if err := s.setTaskWaiting(taskID, messageType); err != nil {
		return message.Resolution{}, value.MessageID, err
	}

	eventType := event.QuestionPublished
	if messageType == message.ScopeExpansionRequest {
		eventType = event.ScopeExpansionRequested
	} else if messageType == message.PermissionRequest {
		eventType = event.PermissionRequested
	}
	_ = s.appendEvent(event.Input{TaskID: taskID, WorkerID: workerID, Source: "message-router", Type: eventType, Severity: "warning", Payload: map[string]any{"message_id": value.MessageID, "type": messageType}})

	select {
	case resolution := <-wait:
		return resolution, value.MessageID, nil
	case <-ctx.Done():
		return message.Resolution{}, value.MessageID, ctx.Err()
	}
}

func (s *Service) ResolveMessage(id string, resolution message.Resolution) error {
	if s.router == nil {
		return fmt.Errorf("message router is not initialized")
	}
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
	if message.IsTerminal(value.Status) {
		return fmt.Errorf("message %q is already resolved", id)
	}
	if value.Type == message.Question && strings.TrimSpace(resolution.Answer) == "" {
		return fmt.Errorf("question answer is required")
	}
	if value.Type == message.ScopeExpansionRequest && resolution.Decision.Allowed {
		if err := s.approveScope(value, resolution.Decision); err != nil {
			return err
		}
	}
	answerPayload, _ := json.Marshal(resolution)
	answered, err := s.router.Transition(id, message.Answered, "", answerPayload, nil)
	if err != nil {
		return err
	}
	s.syncMessageProjection(answered)

	s.mu.Lock()
	wait := s.pending[id]
	delete(s.pending, id)
	s.mu.Unlock()
	if wait != nil {
		wait <- resolution
		close(wait)
	}

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
	_ = s.appendEvent(event.Input{TaskID: value.TaskID, WorkerID: value.WorkerID, Source: "message-router", Type: eventType, Severity: "info", Payload: map[string]any{"message_id": id, "status": string(message.Answered)}})
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
	s.syncMessageProjection(queued)
	_ = s.appendEvent(event.Input{
		TaskID: taskID, WorkerID: workerID, Source: "message-router", Type: event.MessageQueued, Severity: "info",
		Payload: map[string]any{"message_id": queued.MessageID, "delivery_mode": string(mode), "status": string(message.Queued)},
	})

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
				if harness, ok := s.registry.Get(adapter.HarnessName(s.config.Harness)); ok {
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
				s.syncMessageProjection(failed)
				_ = s.appendEvent(event.Input{TaskID: queued.TaskID, Source: "message-router", Type: event.MessageFailed, Severity: "error", Payload: map[string]any{"message_id": queued.MessageID}})
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
			if tErr == nil {
				s.syncMessageProjection(failed)
			}
			_ = s.appendEvent(event.Input{TaskID: queued.TaskID, Source: "message-router", Type: event.MessageFailed, Severity: "error", Payload: map[string]any{"message_id": queued.MessageID, "error": sendErr.Error()}})
			return adapter.DeliveryResult{Mode: adapter.DeliveryImmediate, MessageID: queued.MessageID}, sendErr
		}
		delivered, tErr := s.router.RecordDeliveryAttempt(queued.MessageID, message.Delivered, nil)
		if tErr != nil {
			return adapter.DeliveryResult{Mode: adapter.DeliveryImmediate, MessageID: queued.MessageID}, tErr
		}
		s.syncMessageProjection(delivered)
		_ = s.appendEvent(event.Input{TaskID: queued.TaskID, Source: "message-router", Type: event.MessageDelivered, Severity: "info", Payload: map[string]any{"message_id": queued.MessageID, "delivery_mode": string(mode)}})
		return adapter.DeliveryResult{Mode: adapter.DeliveryImmediate, MessageID: queued.MessageID}, nil

	case message.DeliveryNextTurn:
		return adapter.DeliveryResult{Mode: adapter.DeliveryNextTurn, MessageID: queued.MessageID}, nil

	case message.DeliveryResume:
		return adapter.DeliveryResult{Mode: adapter.DeliveryResume, MessageID: queued.MessageID}, nil

	default:
		failed, tErr := s.router.Transition(queued.MessageID, message.Failed, message.DeliveryUnsupported, nil, adapter.ErrUnsupported)
		if tErr == nil {
			s.syncMessageProjection(failed)
			_ = s.appendEvent(event.Input{TaskID: queued.TaskID, Source: "message-router", Type: event.MessageFailed, Severity: "error", Payload: map[string]any{"message_id": queued.MessageID}})
		}
		return adapter.DeliveryResult{Mode: adapter.DeliveryUnsupported, MessageID: queued.MessageID}, adapter.ErrUnsupported
	}
}

// FlushInstructionOutbox delivers queued next_turn/resume instructions for a task.
func (s *Service) FlushInstructionOutbox(ctx context.Context, taskID, trigger string) error {
	if s.router == nil {
		return nil
	}
	if runtime, ok := s.taskState(domain.TaskID(taskID)); ok && recoveryTaskTerminal(runtime) {
		_, _ = s.expireTaskMessages(taskID, "task terminal during flush")
		return nil
	}

	switch trigger {
	case "turn_boundary", "active_session":
		pending := s.router.PendingInstructions(taskID, message.DeliveryNextTurn, message.DeliveryImmediate)
		for _, item := range pending {
			if item.DeliveryMode != message.DeliveryNextTurn && item.DeliveryMode != message.DeliveryImmediate {
				continue
			}
			text := instructionText(item)
			if _, err := s.deliverInstruction(ctx, item, text); err != nil {
				// Keep going for remaining messages when failure is non-fatal.
				continue
			}
			_ = s.appendEvent(event.Input{TaskID: taskID, Source: "message-router", Type: event.InstructionFlushed, Severity: "info", Payload: map[string]any{"message_id": item.MessageID, "trigger": trigger}})
		}
	case "session_resume", "recovery":
		pending := s.router.PendingInstructions(taskID, message.DeliveryResume)
		if len(pending) == 0 {
			return nil
		}
		// Resume delivery creates a recovery_resume attempt via execute path if needed.
		// Here we only deliver if an active session already exists after resume.
		s.mu.Lock()
		active, isActive := s.active[taskID]
		s.mu.Unlock()
		if !isActive {
			// Leave queued for executeTask recovery_resume to pick up; do not fresh-retry.
			return nil
		}
		for _, item := range pending {
			text := instructionText(item)
			// Force immediate delivery on active resumed session.
			item.DeliveryMode = message.DeliveryImmediate
			if _, err := s.deliverInstruction(ctx, item, text); err != nil {
				continue
			}
			_ = s.appendEvent(event.Input{TaskID: taskID, WorkerID: active.workerID, Source: "message-router", Type: event.InstructionFlushed, Severity: "info", Payload: map[string]any{"message_id": item.MessageID, "trigger": trigger}})
		}
	}
	return nil
}

func instructionText(value message.Message) string {
	var payload message.InstructionPayload
	if json.Unmarshal(value.Payload, &payload) == nil {
		return payload.Text
	}
	return string(value.Payload)
}

// expireTaskMessages expires all pending messages for a task and clears projection.
func (s *Service) expireTaskMessages(taskID, reason string) ([]message.Message, error) {
	if s.router == nil {
		return nil, nil
	}
	expired, err := s.router.ExpireTask(taskID, reason)
	if err != nil {
		return expired, err
	}
	for _, item := range expired {
		s.syncMessageProjection(item)
		_ = s.appendEvent(event.Input{
			TaskID: taskID, Source: "message-router", Type: event.MessageExpired, Severity: "warning",
			Payload: map[string]any{"message_id": item.MessageID, "reason": reason},
		})
		// Unblock any waiters.
		s.mu.Lock()
		if wait := s.pending[item.MessageID]; wait != nil {
			delete(s.pending, item.MessageID)
			close(wait)
		}
		s.mu.Unlock()
	}
	_ = message.ClearTopLevelQuestion(filepath.Join(s.paths.Tasks, taskID))
	_ = s.recomputeTaskWaiting(taskID)
	return expired, nil
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
			pending := s.router.PendingDecisions(taskID)
			if len(pending) > 0 {
				candidate.Tasks[index].Task.Status = state.TaskBlocked
				candidate.Tasks[index].Dimensions.Task = state.TaskBlocked
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

// syncMessageProjection updates the legacy messageIndex / snapshot projection
// after a Router mutation. Router remains the authority for message state.
func (s *Service) syncMessageProjection(value message.Message) {
	s.mu.Lock()
	if s.messageIndex == nil {
		s.messageIndex = map[string]message.Message{}
	}
	s.messageIndex[value.MessageID] = value
	if s.router != nil {
		s.snapshot.Messages = s.router.Snapshot(false)
	}
	s.mu.Unlock()

	// When the event sink is available, also checkpoint Messages via Commit.
	if s.events == nil || !s.AcceptingWork() {
		return
	}
	eventType := event.MessageQueued
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
	_ = s.commitMutate(context.Background(), event.Input{
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
		}
		if request.RequiresPublicInterfaceChange && !decision.AllowPublicInterfaceChange {
			return fmt.Errorf("scope request requires explicit public-interface approval")
		}
		target.Task.WriteScope = append(target.Task.WriteScope, request.RequestedScope...)
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

// Used by finish/fail/cancel paths — expire zombies.
func (s *Service) onTaskTerminalMessages(taskID, reason string) {
	_, _ = s.expireTaskMessages(taskID, reason)
}

// Ensure workerpkg import used when flush triggers resume attempt metadata.
var _ = workerpkg.AttemptFresh
