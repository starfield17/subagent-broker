package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
)

// nativePermissionDeliveryTimeout bounds RespondPermission calls. Timeout is a
// retryable delivery failure (decision stays frozen, message stays pending).
const nativePermissionDeliveryTimeout = 15 * time.Second

// nativePermissionParse is the normalized view of a protocol-native permission event.
type nativePermissionParse struct {
	RequestID string
	ToolName  string
	Input     json.RawMessage
	Options   []message.PermissionOption
	TurnID    string
}

// bridgeNativePermission persists a Broker permission_request for a protocol-native
// PermissionRequested event. Claude hook-backed permissions still use RequestMessage
// via the interaction hook and do not go through this path.
//
// Replay of the same full identity tuple is idempotent: no duplicate message is created.
//
// Crash semantics: resolution intent is persisted before adapter delivery. A crash
// after the harness receives a response but before Answered is persisted may cause
// an identical retry to resend (at-least-once across that window, not exactly-once).
func (s *Service) bridgeNativePermission(runtime *TaskState, harness adapter.Adapter, native adapter.NativeEvent, workerID string) {
	if s.router == nil || runtime == nil || runtime.Worker == nil {
		return
	}
	// Only bridge when this session claims effective permission events.
	eff := adapter.CapabilitiesFromMap(runtime.Worker.Capabilities)
	if !eff.PermissionEvents {
		return
	}
	harnessName := string(harness.Descriptor().Name)
	if runtime.Worker.Harness != "" {
		harnessName = runtime.Worker.Harness
	}
	// Claude uses PreToolUse hooks + RequestMessage, not adapter RespondPermission.
	if harnessName == string(adapter.HarnessClaudeCode) {
		return
	}

	parsed := parseNativePermission(harnessName, native.Payload)
	if parsed.RequestID == "" {
		_ = s.appendEvent(event.Input{
			TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor",
			Type: "permission.bridge_failed", Severity: "error",
			Payload: map[string]any{"reason": "missing native permission request id"},
		})
		return
	}
	sessionID := runtime.Worker.NativeSessionID
	if sessionID == "" {
		s.mu.Lock()
		if active, ok := s.active[string(runtime.Task.TaskID)]; ok {
			sessionID = active.sessionID
		}
		s.mu.Unlock()
	}
	if sessionID == "" {
		_ = s.appendEvent(event.Input{
			TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor",
			Type: "permission.bridge_failed", Severity: "error",
			Payload: map[string]any{"reason": "missing native session id for permission"},
		})
		return
	}

	// Prefer the active Worker's persisted attempt; fail closed if unknown.
	attemptNumber := runtime.Worker.Attempt
	if attemptNumber <= 0 && runtime.ActiveAttempt > 0 {
		attemptNumber = runtime.ActiveAttempt
	}
	if attemptNumber <= 0 {
		s.mu.Lock()
		if active, ok := s.active[string(runtime.Task.TaskID)]; ok && active.attempt > 0 {
			attemptNumber = active.attempt
		}
		s.mu.Unlock()
	}
	if attemptNumber <= 0 {
		_ = s.appendEvent(event.Input{
			TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor",
			Type: "permission.bridge_failed", Severity: "error",
			Payload: map[string]any{"reason": "no trustworthy attempt number for native permission"},
		})
		return
	}
	if workerID == "" {
		workerID = string(runtime.Worker.WorkerID)
	}

	// Deduplicate by full originating identity (not request ID alone).
	if existing := s.findPermissionByIdentity(string(runtime.Task.TaskID), harnessName, sessionID, parsed.RequestID, workerID, attemptNumber); existing != nil {
		return
	}

	toolName := parsed.ToolName
	if toolName == "" {
		toolName = "unknown"
	}
	input := parsed.Input
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	turnID := parsed.TurnID
	if turnID == "" {
		turnID = runtime.Worker.NativeTurnID
	}
	payload := message.PermissionRequestPayload{
		ToolName:           toolName,
		Input:              input,
		Harness:            harnessName,
		NativeSessionID:    sessionID,
		NativePermissionID: parsed.RequestID,
		NativeTurnID:       turnID,
		NativeOptions:      parsed.Options,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	value, err := s.router.EnqueueDecisionWithAttempt(
		string(runtime.Task.TaskID), workerID, attemptNumber,
		message.PermissionRequest, message.Permission, raw,
	)
	if err != nil {
		return
	}
	if err := s.CommitMessageProjection(context.Background(), value, event.MessageQueued); err != nil {
		// Fail-closed: durable enqueue succeeded but projection failed.
		return
	}
	question := s.questionEnvelopeFor(value)
	taskDir := filepath.Join(s.paths.Tasks, string(runtime.Task.TaskID))
	// Projection failure must not terminal-fail the permission while the harness is blocked.
	if err := message.PublishQuestionProjection(taskDir, value.MessageID, string(runtime.Task.TaskID), question, false); err != nil {
		_ = s.appendEvent(event.Input{
			TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor",
			Type: "permission.projection_failed", Severity: "error",
			Payload: map[string]any{"message_id": value.MessageID, "error": err.Error()},
		})
		// Keep pending and blocked.
		_ = s.setTaskWaiting(string(runtime.Task.TaskID), message.PermissionRequest)
		return
	}
	if err := s.refreshQuestionProjection(string(runtime.Task.TaskID)); err != nil {
		_ = s.appendEvent(event.Input{
			TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor",
			Type: "permission.projection_failed", Severity: "error",
			Payload: map[string]any{"message_id": value.MessageID, "error": err.Error()},
		})
		_ = s.setTaskWaiting(string(runtime.Task.TaskID), message.PermissionRequest)
		return
	}
	if err := s.setTaskWaiting(string(runtime.Task.TaskID), message.PermissionRequest); err != nil {
		return
	}
	_ = s.appendEvent(event.Input{
		TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor",
		Type: event.PermissionRequested, Severity: "warning",
		Payload: map[string]any{
			"message_id":           value.MessageID,
			"type":                 message.PermissionRequest,
			"native_permission_id": parsed.RequestID,
			"harness":              harnessName,
			"native_session_id":    sessionID,
			"attempt_number":       attemptNumber,
			"worker_id":            workerID,
		},
	})
}

// findPermissionByIdentity matches the full originating identity tuple.
func (s *Service) findPermissionByIdentity(taskID, harness, sessionID, nativeID, workerID string, attempt int) *message.Message {
	if s.router == nil || nativeID == "" {
		return nil
	}
	for _, item := range s.router.Snapshot(true) {
		if item.TaskID != taskID || item.Type != message.PermissionRequest {
			continue
		}
		var payload message.PermissionRequestPayload
		if json.Unmarshal(item.Payload, &payload) != nil {
			continue
		}
		if payload.NativePermissionID != nativeID {
			continue
		}
		if payload.Harness != "" && harness != "" && payload.Harness != harness {
			continue
		}
		if payload.NativeSessionID != sessionID {
			continue
		}
		if item.WorkerID != workerID {
			continue
		}
		if item.AttemptNumber != attempt {
			continue
		}
		copy := item
		return &copy
	}
	return nil
}

// resolveNativePermission freezes decision intent, validates exact binding, and
// attempts Adapter.RespondPermission. On delivery failure the message stays
// non-terminal Queued with frozen Resolution for identical retry.
// Per-message delivery serialization ensures concurrent identical/conflicting
// resolutions are linearizable without holding Router locks during adapter I/O.
func (s *Service) resolveNativePermission(ctx context.Context, value message.Message, resolution message.Resolution) error {
	lock := s.deliveryLock(value.MessageID)
	lock.Lock()
	defer lock.Unlock()

	// Re-load authoritative state under delivery serialization.
	if latest, ok := s.router.Get(value.MessageID); ok {
		value = latest
	}
	if message.IsTerminal(value.Status) {
		// Compare canonical resolution: same semantic decision → idempotent success;
		// conflicting decision → explicit conflict.
		resJSON, marshalErr := json.Marshal(resolution)
		if marshalErr != nil {
			return marshalErr
		}
		if message.ResolutionsEqual(value.Resolution, resJSON) {
			return nil // idempotent success
		}
		return fmt.Errorf("message %q is already terminal (%s) with a different resolution", value.MessageID, value.Status)
	}

	var payload message.PermissionRequestPayload
	if err := json.Unmarshal(value.Payload, &payload); err != nil {
		return err
	}
	if payload.NativePermissionID == "" {
		return fmt.Errorf("not a native permission message")
	}

	// 1) Freeze decision intent before physical delivery.
	resJSON, err := json.Marshal(resolution)
	if err != nil {
		return err
	}
	frozen, err := s.router.RecordResolutionIntent(value.MessageID, resJSON)
	if err != nil {
		return err
	}
	if err := s.CommitMessageProjection(ctx, frozen, event.MessageQueued); err != nil {
		return err
	}
	// Re-load frozen message for delivery.
	value = frozen
	if err := json.Unmarshal(value.Payload, &payload); err != nil {
		return err
	}

	// 2) Exact binding to the originating active session.
	bindErr := s.validatePermissionBinding(value, payload)
	if bindErr != nil {
		// Routing attempt without adapter call.
		updated, tErr := s.router.RecordDeliveryAttempt(value.MessageID, "", bindErr)
		if tErr == nil {
			_ = s.CommitMessageProjection(ctx, updated, event.MessageQueued)
		}
		_ = s.appendEvent(event.Input{
			TaskID: value.TaskID, WorkerID: value.WorkerID, Source: "message-router",
			Type: "permission.reconciliation_required", Severity: "error",
			Payload: s.permissionBindingEventPayload(value, payload, bindErr),
		})
		_ = s.refreshQuestionProjection(value.TaskID)
		_ = s.recomputeTaskWaiting(value.TaskID)
		return bindErr
	}

	s.mu.Lock()
	active := s.active[value.TaskID]
	s.mu.Unlock()

	decision := adapter.PermissionDecision{
		RequestID: payload.NativePermissionID,
		Allowed:   resolution.Decision.Allowed,
		Reason:    resolution.Decision.Reason,
	}
	if len(payload.NativeOptions) > 0 {
		optionID, optErr := message.SelectPermissionOptionID(payload.NativeOptions, resolution.Decision.Allowed)
		if optErr != nil {
			updated, tErr := s.router.RecordDeliveryAttempt(value.MessageID, "", optErr)
			if tErr == nil {
				_ = s.CommitMessageProjection(ctx, updated, event.MessageQueued)
			}
			_ = s.refreshQuestionProjection(value.TaskID)
			_ = s.recomputeTaskWaiting(value.TaskID)
			return optErr
		}
		decision.OptionID = optionID
	}

	// 3) Bounded delivery context (not context.Background).
	deliverCtx, cancel := context.WithTimeout(ctx, nativePermissionDeliveryTimeout)
	defer cancel()
	if deliverCtx.Err() != nil {
		// Parent already cancelled — still treat as retryable failure.
		deliverCtx, cancel = context.WithTimeout(context.Background(), nativePermissionDeliveryTimeout)
		defer cancel()
	}

	sendErr := active.adapter.RespondPermission(deliverCtx, payload.NativeSessionID, decision)
	if sendErr != nil {
		// Keep non-terminal Queued with frozen resolution for identical retry.
		updated, tErr := s.router.RecordDeliveryAttempt(value.MessageID, "", sendErr)
		if tErr != nil {
			return fmt.Errorf("permission delivery failed: %w (also record attempt: %v)", sendErr, tErr)
		}
		if cErr := s.CommitMessageProjection(ctx, updated, event.MessageQueued); cErr != nil {
			return cErr
		}
		_ = s.appendEvent(event.Input{
			TaskID: value.TaskID, WorkerID: value.WorkerID, Source: "message-router",
			Type: "permission.delivery_failed", Severity: "error",
			Payload: map[string]any{
				"message_id":        value.MessageID,
				"status":            string(message.Queued),
				"delivery_attempts": updated.DeliveryAttempts,
				"error":             sendErr.Error(),
				"retry_pending":     true,
			},
		})
		_ = s.refreshQuestionProjection(value.TaskID)
		_ = s.recomputeTaskWaiting(value.TaskID)
		return sendErr
	}

	// 4) Success: Answered (clears Error, increments attempts).
	// Status change Queued → Answered; Resolution already frozen (must not rewrite).
	updated, tErr := s.router.RecordDeliveryAttempt(value.MessageID, message.Answered, nil)
	if tErr != nil {
		return fmt.Errorf("permission delivered but failed to record Answered: %w", tErr)
	}
	if updated.Status != message.Answered {
		return fmt.Errorf("permission delivered but status is %s after record", updated.Status)
	}
	if err := s.CommitMessageProjection(ctx, updated, event.MessageAnswered); err != nil {
		return err
	}
	if err := s.refreshQuestionProjection(value.TaskID); err != nil {
		return err
	}
	if err := s.recomputeTaskWaiting(value.TaskID); err != nil {
		return err
	}
	if err := s.appendEvent(event.Input{
		TaskID: value.TaskID, WorkerID: value.WorkerID, Source: "message-router",
		Type: event.PermissionResolved, Severity: "info",
		Payload: map[string]any{"message_id": value.MessageID, "status": string(message.Answered)},
	}); err != nil {
		return err
	}
	if !s.router.HasPendingDecisions(value.TaskID) {
		s.signalAdvance()
	}
	return nil
}

// validatePermissionBinding enforces exact harness/session/worker/attempt match.
// AttemptNumber 0 is never a wildcard for a newer attempt.
func (s *Service) validatePermissionBinding(value message.Message, payload message.PermissionRequestPayload) error {
	if payload.NativePermissionID == "" {
		return fmt.Errorf("native permission id is required")
	}
	// Incomplete identity → reconciliation (legacy journals).
	if payload.Harness == "" || payload.NativeSessionID == "" {
		return fmt.Errorf("incomplete native permission routing identity; reconciliation required")
	}
	// AttemptNumber 0 must not match an arbitrary newer attempt.
	if value.AttemptNumber <= 0 {
		return fmt.Errorf("permission attempt_number %d is not a delivery wildcard; reconciliation required", value.AttemptNumber)
	}
	if value.WorkerID == "" {
		return fmt.Errorf("permission worker_id is required for delivery")
	}

	s.mu.Lock()
	active, isActive := s.active[value.TaskID]
	s.mu.Unlock()
	if !isActive {
		return fmt.Errorf("no active worker for exact permission delivery (session %s attempt %d)", payload.NativeSessionID, value.AttemptNumber)
	}

	actualHarness := string(active.adapter.Descriptor().Name)
	if payload.Harness != actualHarness {
		return fmt.Errorf("permission harness mismatch: expected %q actual %q", payload.Harness, actualHarness)
	}
	if payload.NativeSessionID != active.sessionID {
		return fmt.Errorf("permission session mismatch: expected %q actual %q", payload.NativeSessionID, active.sessionID)
	}
	if value.WorkerID != active.workerID {
		return fmt.Errorf("permission worker mismatch: expected %q actual %q", value.WorkerID, active.workerID)
	}
	if value.AttemptNumber != active.attempt {
		return fmt.Errorf("permission attempt mismatch: expected %d actual %d", value.AttemptNumber, active.attempt)
	}
	if active.taskID != "" && value.TaskID != active.taskID {
		return fmt.Errorf("permission task mismatch: expected %q actual %q", value.TaskID, active.taskID)
	}
	return nil
}

func (s *Service) permissionBindingEventPayload(value message.Message, payload message.PermissionRequestPayload, cause error) map[string]any {
	out := map[string]any{
		"message_id":          value.MessageID,
		"expected_harness":    payload.Harness,
		"expected_session_id": payload.NativeSessionID,
		"expected_worker_id":  value.WorkerID,
		"expected_attempt":    value.AttemptNumber,
		"reason":              "permission binding mismatch",
	}
	if cause != nil {
		out["error"] = cause.Error()
	}
	s.mu.Lock()
	active, isActive := s.active[value.TaskID]
	s.mu.Unlock()
	if isActive {
		out["actual_harness"] = string(active.adapter.Descriptor().Name)
		out["actual_session_id"] = active.sessionID
		out["actual_worker_id"] = active.workerID
		out["actual_attempt"] = active.attempt
	}
	return out
}

func (s *Service) deliveryLock(messageID string) *sync.Mutex {
	v, _ := s.deliveryLocks.LoadOrStore(messageID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// isNativePermission reports whether the message is a protocol-native permission
// (has NativePermissionID). Claude hooks have no native id.
func isNativePermission(value message.Message) bool {
	if value.Type != message.PermissionRequest {
		return false
	}
	var payload message.PermissionRequestPayload
	if json.Unmarshal(value.Payload, &payload) != nil {
		return false
	}
	return payload.NativePermissionID != ""
}

// parseNativePermission dispatches harness-specific normalization, then falls
// back to a conservative generic parser for fake/Codex fixtures.
func parseNativePermission(harnessName string, raw json.RawMessage) nativePermissionParse {
	switch harnessName {
	case string(adapter.HarnessGrokBuild):
		if p, ok := parseGrokACPPermission(raw); ok {
			return p
		}
	case string(adapter.HarnessOpenCode):
		if p, ok := parseOpenCodePermission(raw); ok {
			return p
		}
	}
	return parseGenericNativePermission(raw)
}

// parseGrokACPPermission handles session/request_permission JSON-RPC server requests.
func parseGrokACPPermission(raw json.RawMessage) (nativePermissionParse, bool) {
	var rpc struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(raw, &rpc); err != nil || len(rpc.ID) == 0 {
		return nativePermissionParse{}, false
	}
	requestID := normalizeJSONID(rpc.ID)
	if requestID == "" {
		return nativePermissionParse{}, false
	}
	var params struct {
		SessionID string `json:"sessionId"`
		ToolCall  struct {
			ToolCallID string `json:"toolCallId"`
			Title      string `json:"title"`
			Kind       string `json:"kind"`
		} `json:"toolCall"`
		Options []struct {
			OptionID string `json:"optionId"`
			Name     string `json:"name"`
			Kind     string `json:"kind"`
		} `json:"options"`
	}
	_ = json.Unmarshal(rpc.Params, &params)
	toolName := params.ToolCall.Title
	if toolName == "" {
		toolName = params.ToolCall.Kind
	}
	if toolName == "" {
		toolName = rpc.Method
	}
	input := rpc.Params
	if params.ToolCall.ToolCallID != "" {
		input, _ = json.Marshal(params.ToolCall)
	}
	options := make([]message.PermissionOption, 0, len(params.Options))
	for _, opt := range params.Options {
		if strings.TrimSpace(opt.OptionID) == "" {
			continue
		}
		options = append(options, message.PermissionOption{
			OptionID: opt.OptionID,
			Kind:     opt.Kind,
			Name:     opt.Name,
		})
	}
	return nativePermissionParse{
		RequestID: requestID,
		ToolName:  toolName,
		Input:     input,
		Options:   options,
	}, true
}

// parseOpenCodePermission handles permission.asked SSE properties, including
// object-valued tool fields. Does not recurse into nested objects for id.
func parseOpenCodePermission(raw json.RawMessage) (nativePermissionParse, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return nativePermissionParse{}, false
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nativePermissionParse{}, false
	}
	requestID := firstStringField(top, "id", "permissionID", "permission_id", "request_id")
	if requestID == "" {
		if props, ok := top["properties"]; ok {
			var nested map[string]json.RawMessage
			if json.Unmarshal(props, &nested) == nil {
				requestID = firstStringField(nested, "id", "permissionID", "permission_id", "request_id")
				if requestID != "" {
					top = nested
				}
			}
		}
	}
	if requestID == "" {
		return nativePermissionParse{}, false
	}
	toolName := firstStringField(top, "tool_name", "title", "name")
	if toolName == "" {
		if toolRaw, ok := top["tool"]; ok {
			var asString string
			if json.Unmarshal(toolRaw, &asString) == nil && asString != "" {
				toolName = asString
			} else {
				var toolObj map[string]any
				if json.Unmarshal(toolRaw, &toolObj) == nil {
					if v, ok := toolObj["name"].(string); ok && v != "" {
						toolName = v
					} else if v, ok := toolObj["callID"].(string); ok && v != "" {
						toolName = "tool:" + v
					} else if v, ok := toolObj["callId"].(string); ok && v != "" {
						toolName = "tool:" + v
					} else {
						toolName = "tool"
					}
				}
			}
		}
	}
	input := raw
	if props, ok := top["properties"]; ok && len(top) <= 3 {
		input = props
	}
	return nativePermissionParse{
		RequestID: requestID,
		ToolName:  toolName,
		Input:     input,
	}, true
}

func firstStringField(m map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		raw, ok := m[key]
		if !ok || len(raw) == 0 {
			continue
		}
		var s string
		if json.Unmarshal(raw, &s) == nil && strings.TrimSpace(s) != "" {
			return s
		}
		var n json.Number
		if json.Unmarshal(raw, &n) == nil && n.String() != "" {
			return n.String()
		}
	}
	return ""
}

// parseGenericNativePermission covers Codex server requests and flat fake fixtures.
func parseGenericNativePermission(raw json.RawMessage) nativePermissionParse {
	if len(raw) == 0 || string(raw) == "null" {
		return nativePermissionParse{}
	}

	var rpc struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(raw, &rpc); err == nil && len(rpc.ID) > 0 && (rpc.Method != "" || len(rpc.Params) > 0) {
		requestID := normalizeJSONID(rpc.ID)
		toolName, input := extractToolFromParams(rpc.Params)
		if toolName == "" && rpc.Method != "" {
			toolName = rpc.Method
		}
		if requestID != "" {
			return nativePermissionParse{RequestID: requestID, ToolName: toolName, Input: input}
		}
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nativePermissionParse{}
	}
	requestID := firstStringField(top, "request_id", "permissionID", "permission_id", "id")
	toolName := firstStringField(top, "tool_name", "title", "name")
	if toolName == "" {
		if toolRaw, ok := top["tool"]; ok {
			var asString string
			if json.Unmarshal(toolRaw, &asString) == nil {
				toolName = asString
			}
		}
	}
	var input json.RawMessage
	if in, ok := top["input"]; ok {
		input = in
	}
	if len(input) == 0 {
		if p, ok := top["params"]; ok {
			toolName2, input2 := extractToolFromParams(p)
			if toolName == "" {
				toolName = toolName2
			}
			input = input2
		}
	}
	return nativePermissionParse{RequestID: requestID, ToolName: toolName, Input: input}
}

func normalizeJSONID(raw json.RawMessage) string {
	raw = json.RawMessage(strings.TrimSpace(string(raw)))
	if len(raw) == 0 {
		return ""
	}
	var asString string
	if json.Unmarshal(raw, &asString) == nil {
		return asString
	}
	var asNumber json.Number
	if json.Unmarshal(raw, &asNumber) == nil {
		return asNumber.String()
	}
	text := strings.TrimSpace(string(raw))
	if unquoted, err := strconv.Unquote(text); err == nil {
		return unquoted
	}
	return text
}

func extractToolFromParams(params json.RawMessage) (toolName string, input json.RawMessage) {
	if len(params) == 0 {
		return "", nil
	}
	var value map[string]json.RawMessage
	if json.Unmarshal(params, &value) != nil {
		return "", params
	}
	toolName = firstStringField(value, "tool_name", "name", "title", "command")
	if toolName == "" {
		if toolRaw, ok := value["tool"]; ok {
			var asString string
			if json.Unmarshal(toolRaw, &asString) == nil {
				toolName = asString
			}
		}
	}
	if toolName == "" && firstStringField(value, "command") != "" {
		toolName = "command"
	}
	if in, ok := value["input"]; ok {
		input = in
	}
	if len(input) == 0 {
		if args, ok := value["arguments"]; ok {
			input = args
		}
	}
	if len(input) == 0 {
		input = params
	}
	return toolName, input
}
