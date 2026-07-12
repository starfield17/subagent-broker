package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
)

func (s *Service) Inbox(includeResolved bool) []message.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return message.Sorted(s.messageIndex, includeResolved)
}

func (s *Service) RequestMessage(ctx context.Context, taskID, workerID string, messageType message.Type, category message.Category, payload any) (message.Resolution, string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return message.Resolution{}, "", err
	}
	now := time.Now().UTC()
	id, err := message.NewID(now)
	if err != nil {
		return message.Resolution{}, "", err
	}
	value := message.Message{SchemaVersion: SchemaVersion, MessageID: id, RunID: string(s.snapshot.Run.RunID), TaskID: taskID, WorkerID: workerID, Type: messageType, Category: category, Status: message.Queued, CreatedAt: now, UpdatedAt: now, Payload: raw}
	if err := s.messages.Append(value); err != nil {
		return message.Resolution{}, "", err
	}
	question := questionEnvelope(value)
	if err := message.PublishQuestionID(filepath.Join(s.paths.Tasks, taskID), id, question); err != nil {
		value.Status = message.Failed
		value.Error = err.Error()
		value.UpdatedAt = time.Now().UTC()
		_ = s.messages.Append(value)
		return message.Resolution{}, id, err
	}
	wait := make(chan message.Resolution, 1)
	s.mu.Lock()
	s.messageIndex[id] = value
	s.pending[id] = wait
	s.snapshot.Messages = message.Sorted(s.messageIndex, false)
	s.markWaitingLocked(taskID, messageType)
	_ = s.saveLocked()
	s.mu.Unlock()
	eventType := event.QuestionPublished
	if messageType == message.ScopeExpansionRequest {
		eventType = event.ScopeExpansionRequested
	} else if messageType == message.PermissionRequest {
		eventType = event.PermissionRequested
	}
	s.append(event.Input{TaskID: taskID, WorkerID: workerID, Source: "message-router", Type: eventType, Severity: "warning", Payload: map[string]any{"message_id": id, "type": messageType}})
	select {
	case resolution := <-wait:
		return resolution, id, nil
	case <-ctx.Done():
		return message.Resolution{}, id, ctx.Err()
	}
}

func (s *Service) ResolveMessage(id string, resolution message.Resolution) error {
	s.mu.Lock()
	value, ok := s.messageIndex[id]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("message %q was not found", id)
	}
	if value.Status == message.Answered || value.Status == message.Failed || value.Status == message.Expired {
		s.mu.Unlock()
		return fmt.Errorf("message %q is already resolved", id)
	}
	s.mu.Unlock()
	if value.Type == message.Question && strings.TrimSpace(resolution.Answer) == "" {
		return fmt.Errorf("question answer is required")
	}
	if value.Type == message.ScopeExpansionRequest && resolution.Decision.Allowed {
		if err := s.approveScope(value, resolution.Decision); err != nil {
			return err
		}
	}
	answerPayload, _ := json.Marshal(resolution)
	value.Status = message.Answered
	value.UpdatedAt = time.Now().UTC()
	value.Resolution = answerPayload
	if err := s.messages.Append(value); err != nil {
		return err
	}
	s.mu.Lock()
	s.messageIndex[id] = value
	wait := s.pending[id]
	delete(s.pending, id)
	s.snapshot.Messages = message.Sorted(s.messageIndex, false)
	s.clearWaitingLocked(value.TaskID)
	_ = s.saveLocked()
	s.mu.Unlock()
	if wait != nil {
		wait <- resolution
		close(wait)
	}
	_ = os.Remove(filepath.Join(s.paths.Tasks, value.TaskID, "question.md"))
	_ = os.Remove(filepath.Join(s.paths.Tasks, value.TaskID, "question.meta.json"))
	eventType := "message.answered"
	if value.Type == message.ScopeExpansionRequest {
		eventType = event.ScopeExpansionResolved
	} else if value.Type == message.PermissionRequest {
		eventType = event.PermissionResolved
	}
	s.append(event.Input{TaskID: value.TaskID, WorkerID: value.WorkerID, Source: "message-router", Type: eventType, Severity: "info", Payload: map[string]any{"message_id": id}})
	return nil
}

func (s *Service) SendInstruction(ctx context.Context, taskID, text string) (adapter.DeliveryResult, error) {
	if strings.TrimSpace(text) == "" {
		return adapter.DeliveryResult{}, fmt.Errorf("instruction text is required")
	}
	s.mu.Lock()
	active, isActive := s.active[taskID]
	s.mu.Unlock()
	if !isActive {
		return adapter.DeliveryResult{Mode: adapter.DeliveryUnsupported}, adapter.ErrUnsupported
	}
	descriptor := active.adapter.Descriptor()
	var result adapter.DeliveryResult
	var err error
	if descriptor.Capabilities.SteerActiveTurn {
		result, err = active.adapter.SteerActiveTurn(ctx, active.sessionID, text)
	} else if descriptor.Capabilities.BidirectionalStream {
		result, err = active.adapter.SendMessage(ctx, active.sessionID, text)
	} else {
		return adapter.DeliveryResult{Mode: adapter.DeliveryUnsupported}, adapter.ErrUnsupported
	}
	if err != nil {
		return result, err
	}
	now := time.Now().UTC()
	id, err := message.NewID(now)
	if err != nil {
		return result, err
	}
	payload, _ := json.Marshal(message.InstructionPayload{Text: text})
	value := message.Message{SchemaVersion: SchemaVersion, MessageID: id, RunID: string(s.snapshot.Run.RunID), TaskID: taskID, Type: message.Instruction, Status: message.Delivered, DeliveryMode: string(result.Mode), CreatedAt: now, UpdatedAt: now, Payload: payload}
	if err := s.messages.Append(value); err != nil {
		return result, err
	}
	s.mu.Lock()
	s.messageIndex[id] = value
	s.mu.Unlock()
	return result, nil
}

func (s *Service) approveScope(value message.Message, decision message.DecisionPayload) error {
	var request message.ScopeRequestPayload
	if err := json.Unmarshal(value.Payload, &request); err != nil {
		return err
	}
	if len(request.RequestedScope) == 0 {
		return fmt.Errorf("scope request has no requested paths")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var target *TaskState
	for index := range s.snapshot.Tasks {
		if string(s.snapshot.Tasks[index].Task.TaskID) == value.TaskID {
			target = &s.snapshot.Tasks[index]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("task %q was not found", value.TaskID)
	}
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
	candidate := target.Task
	candidate.WriteScope = append(candidate.WriteScope, request.RequestedScope...)
	if decision.AllowPublicInterfaceChange {
		candidate.AllowPublicInterfaceChange = true
	}
	var sameWave []domain.Task
	for _, runtime := range s.snapshot.Tasks {
		if runtime.Task.WaveID == candidate.WaveID {
			if runtime.Task.TaskID == candidate.TaskID {
				sameWave = append(sameWave, candidate)
			} else {
				sameWave = append(sameWave, runtime.Task)
			}
		}
	}
	result := wave.Preflight(sameWave)
	if !result.Allowed {
		return fmt.Errorf("expanded scope fails Wave preflight: %s", formatPreflightIssues(result.Issues))
	}
	target.Task = candidate
	contract, err := task.RenderContract(candidate, s.snapshot.Run.RunID)
	if err != nil {
		return err
	}
	layout, _ := storage.NewLayout(s.config.BrokerHome)
	paths, _ := layout.TaskPaths(string(s.snapshot.Run.ProjectID), string(s.snapshot.Run.RunID), value.TaskID)
	if err := storage.AtomicWriteFile(paths.Contract, []byte(contract), 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteJSON(s.wavePaths(candidate.WaveID).Preflight, result, 0o600); err != nil {
		return err
	}
	return s.saveLocked()
}

func (s *Service) markWaitingLocked(taskID string, kind message.Type) {
	for index := range s.snapshot.Tasks {
		runtime := &s.snapshot.Tasks[index]
		if string(runtime.Task.TaskID) != taskID {
			continue
		}
		runtime.Task.Status = state.TaskBlocked
		runtime.Dimensions.Task = state.TaskBlocked
		runtime.Dimensions.Progress = state.ProgressQuiet
		switch kind {
		case message.ScopeExpansionRequest:
			runtime.Dimensions.Protocol = state.ProtocolWaitingScope
		case message.PermissionRequest:
			runtime.Dimensions.Protocol = state.ProtocolWaitingPermission
		default:
			runtime.Dimensions.Protocol = state.ProtocolWaitingUser
		}
		return
	}
}

func (s *Service) clearWaitingLocked(taskID string) {
	for index := range s.snapshot.Tasks {
		runtime := &s.snapshot.Tasks[index]
		if string(runtime.Task.TaskID) != taskID {
			continue
		}
		if runtime.Worker != nil && runtime.Worker.ExitCode == nil {
			runtime.Task.Status = state.TaskRunning
			runtime.Dimensions.Task = state.TaskRunning
			runtime.Dimensions.Protocol = state.ProtocolThinking
			runtime.Dimensions.Progress = state.ProgressActive
		}
		return
	}
}

func questionEnvelope(value message.Message) message.QuestionEnvelope {
	result := message.QuestionEnvelope{SchemaVersion: SchemaVersion, CurrentScope: []string{"See Task Contract"}, WorkspaceState: "The Worker is waiting and has not been authorized to proceed."}
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
			return payload
		}
		result.Question = "The Worker requested Main Agent input."
		result.Reason = "The Worker cannot continue without an answer."
	}
	return result
}
