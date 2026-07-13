package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
)

// bridgeNativePermission persists a Broker permission_request for a protocol-native
// PermissionRequested event. Claude hook-backed permissions still use RequestMessage
// via the interaction hook and do not go through this path.
//
// Replay of the same native request ID is idempotent: no duplicate message is created.
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

	nativeID, toolName, input := parseNativePermissionPayload(native.Payload)
	if nativeID == "" {
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

	// Deduplicate: same native request already present (pending or terminal).
	if existing := s.findPermissionByNativeID(string(runtime.Task.TaskID), sessionID, nativeID); existing != nil {
		return
	}

	if toolName == "" {
		toolName = "unknown"
	}
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	payload := message.PermissionRequestPayload{
		ToolName:           toolName,
		Input:              input,
		Harness:            harnessName,
		NativeSessionID:    sessionID,
		NativePermissionID: nativeID,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	value, err := s.router.EnqueueDecision(string(runtime.Task.TaskID), workerID, message.PermissionRequest, message.Permission, raw)
	if err != nil {
		return
	}
	if err := s.CommitMessageProjection(context.Background(), value, event.MessageQueued); err != nil {
		return
	}
	question := s.questionEnvelopeFor(value)
	taskDir := filepath.Join(s.paths.Tasks, string(runtime.Task.TaskID))
	if err := message.PublishQuestionProjection(taskDir, value.MessageID, string(runtime.Task.TaskID), question, false); err != nil {
		failed, tErr := s.router.Transition(value.MessageID, message.Failed, "", nil, err)
		if tErr == nil {
			_ = s.CommitMessageProjection(context.Background(), failed, event.MessageFailed)
		}
		return
	}
	if err := s.refreshQuestionProjection(string(runtime.Task.TaskID)); err != nil {
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
			"native_permission_id": nativeID,
			"harness":              harnessName,
			"native_session_id":    sessionID,
		},
	})
}

func (s *Service) findPermissionByNativeID(taskID, sessionID, nativeID string) *message.Message {
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
		if sessionID != "" && payload.NativeSessionID != "" && payload.NativeSessionID != sessionID {
			continue
		}
		copy := item
		return &copy
	}
	return nil
}

// deliverNativePermissionResponse calls Adapter.RespondPermission when the
// message carries native routing metadata. Returns (false, nil) when the
// message is not a native permission request (Claude hook path).
func (s *Service) deliverNativePermissionResponse(ctx context.Context, value message.Message, resolution message.Resolution) (handled bool, err error) {
	if value.Type != message.PermissionRequest {
		return false, nil
	}
	var payload message.PermissionRequestPayload
	if err := json.Unmarshal(value.Payload, &payload); err != nil {
		return false, nil
	}
	if payload.NativePermissionID == "" {
		// Claude hook path: response is delivered via the blocked RequestMessage waiter.
		return false, nil
	}

	sessionID := payload.NativeSessionID
	var harness adapter.Adapter
	s.mu.Lock()
	active, isActive := s.active[value.TaskID]
	s.mu.Unlock()
	if isActive {
		harness = active.adapter
		if sessionID == "" {
			sessionID = active.sessionID
		}
	}
	if harness == nil {
		if runtime, ok := s.taskState(domain.TaskID(value.TaskID)); ok {
			if a, ok := s.adapterForTask(runtime.Task, runtime.Worker); ok {
				harness = a
			}
			if sessionID == "" && runtime.Worker != nil {
				sessionID = runtime.Worker.NativeSessionID
			}
		}
	}
	if harness == nil {
		return true, fmt.Errorf("no adapter available to deliver permission response for %s", value.MessageID)
	}
	if sessionID == "" {
		return true, fmt.Errorf("native session id missing for permission %s", value.MessageID)
	}
	decision := adapter.PermissionDecision{
		RequestID: payload.NativePermissionID,
		Allowed:   resolution.Decision.Allowed,
		Reason:    resolution.Decision.Reason,
	}
	if err := harness.RespondPermission(ctx, sessionID, decision); err != nil {
		return true, err
	}
	return true, nil
}

// parseNativePermissionPayload extracts a stable native request id, tool name,
// and input from Codex/Grok JSON-RPC server requests or OpenCode/fake payloads.
func parseNativePermissionPayload(raw json.RawMessage) (requestID, toolName string, input json.RawMessage) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", "", nil
	}

	// JSON-RPC server request (Codex / Grok ACP): {"id":..., "method":"...", "params":{...}}
	var rpc struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(raw, &rpc); err == nil && len(rpc.ID) > 0 && (rpc.Method != "" || len(rpc.Params) > 0) {
		requestID = normalizeJSONID(rpc.ID)
		toolName, input = extractToolFromParams(rpc.Params)
		if toolName == "" && rpc.Method != "" {
			toolName = rpc.Method
		}
		if requestID != "" {
			return requestID, toolName, input
		}
	}

	// Flat / OpenCode-style / fake payload.
	var flat struct {
		ID            json.RawMessage `json:"id"`
		RequestID     string          `json:"request_id"`
		PermissionID  string          `json:"permissionID"`
		PermissionID2 string          `json:"permission_id"`
		ToolName      string          `json:"tool_name"`
		Tool          string          `json:"tool"`
		Title         string          `json:"title"`
		Input         json.RawMessage `json:"input"`
		Params        json.RawMessage `json:"params"`
		Properties    json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(raw, &flat); err != nil {
		return "", "", nil
	}
	switch {
	case flat.RequestID != "":
		requestID = flat.RequestID
	case flat.PermissionID != "":
		requestID = flat.PermissionID
	case flat.PermissionID2 != "":
		requestID = flat.PermissionID2
	case len(flat.ID) > 0:
		requestID = normalizeJSONID(flat.ID)
	}
	toolName = flat.ToolName
	if toolName == "" {
		toolName = flat.Tool
	}
	if toolName == "" {
		toolName = flat.Title
	}
	input = flat.Input
	if len(input) == 0 && len(flat.Params) > 0 {
		toolName2, input2 := extractToolFromParams(flat.Params)
		if toolName == "" {
			toolName = toolName2
		}
		input = input2
	}
	if len(input) == 0 && len(flat.Properties) > 0 {
		// Nested OpenCode properties may themselves hold id/tool.
		nestedID, nestedTool, nestedInput := parseNativePermissionPayload(flat.Properties)
		if requestID == "" {
			requestID = nestedID
		}
		if toolName == "" {
			toolName = nestedTool
		}
		if len(input) == 0 {
			input = nestedInput
		}
	}
	return requestID, toolName, input
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
	// Keep compact JSON form (e.g. quoted already stripped).
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
	var value struct {
		ToolName  string          `json:"tool_name"`
		Tool      string          `json:"tool"`
		Name      string          `json:"name"`
		Command   string          `json:"command"`
		Title     string          `json:"title"`
		Input     json.RawMessage `json:"input"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if json.Unmarshal(params, &value) != nil {
		return "", params
	}
	toolName = value.ToolName
	if toolName == "" {
		toolName = value.Tool
	}
	if toolName == "" {
		toolName = value.Name
	}
	if toolName == "" {
		toolName = value.Title
	}
	if toolName == "" && value.Command != "" {
		toolName = "command"
	}
	input = value.Input
	if len(input) == 0 {
		input = value.Arguments
	}
	if len(input) == 0 {
		input = params
	}
	return toolName, input
}
