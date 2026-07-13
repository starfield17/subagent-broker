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

// nativePermissionParse is the normalized view of a protocol-native permission event.
type nativePermissionParse struct {
	RequestID string
	ToolName  string
	Input     json.RawMessage
	Options   []message.PermissionOption
}

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

	// Deduplicate: same native request already present (pending or terminal).
	if existing := s.findPermissionByNativeID(string(runtime.Task.TaskID), sessionID, parsed.RequestID); existing != nil {
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
	payload := message.PermissionRequestPayload{
		ToolName:           toolName,
		Input:              input,
		Harness:            harnessName,
		NativeSessionID:    sessionID,
		NativePermissionID: parsed.RequestID,
		NativeOptions:      parsed.Options,
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
			"native_permission_id": parsed.RequestID,
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
// Construction/delivery failures return handled=true with an error so the
// caller never records Answered.
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
	// ACP / option-based harnesses: select opaque optionId before delivery.
	if len(payload.NativeOptions) > 0 {
		optionID, optErr := message.SelectPermissionOptionID(payload.NativeOptions, resolution.Decision.Allowed)
		if optErr != nil {
			return true, optErr
		}
		decision.OptionID = optionID
	}

	if err := harness.RespondPermission(ctx, sessionID, decision); err != nil {
		return true, err
	}
	return true, nil
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
	// Accept request_permission methods; also allow generic server requests with options.
	method := strings.ToLower(strings.TrimSpace(rpc.Method))
	if method != "" && !strings.Contains(method, "permission") && !strings.Contains(method, "request_permission") {
		// Still try if params carry options (some fixtures).
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
	// Top-level object only — avoid recursive nested id capture.
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nativePermissionParse{}, false
	}
	requestID := firstStringField(top, "id", "permissionID", "permission_id", "request_id")
	// If this is a full SSE envelope {type, properties}, unwrap properties once.
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
	// tool may be a string or an object (messageID/callID); never require string.
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
	// Preserve useful metadata without secrets: keep the flat payload as input.
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
		// Numeric ids encoded as JSON numbers.
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

	// JSON-RPC server request: {"id":..., "method":"...", "params":{...}}
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

	// Flat fake payload: tool may be string; do not fail if tool is object.
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
	// Use map so object-valued tool does not fail the whole unmarshal.
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
