package interaction

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/vnai/subagent-broker/internal/message"
)

type permissionHookInput struct {
	ToolName  string          `json:"tool_name"`
	ToolInput json.RawMessage `json:"tool_input"`
}

// RunPermissionHook decodes Claude PreToolUse hook input and resolves it via the Worker plane.
// Prefer RunPermissionHookWithIdentity when process identity has been resolved.
func RunPermissionHook(ctx context.Context, runDir, runID, taskID, workerID string, input io.Reader, output io.Writer) error {
	return RunPermissionHookWithIdentity(ctx, WorkerProcessIdentity{
		RunDir: runDir, RunID: runID, TaskID: taskID, WorkerID: workerID,
	}, input, output)
}

// RunPermissionHookWithIdentity is the production permission-hook path.
func RunPermissionHookWithIdentity(ctx context.Context, id WorkerProcessIdentity, input io.Reader, output io.Writer) error {
	var hook permissionHookInput
	if err := json.NewDecoder(input).Decode(&hook); err != nil {
		return err
	}
	payload := message.PermissionRequestPayload{ToolName: hook.ToolName, Input: sanitizeJSON(hook.ToolInput)}
	raw, _ := json.Marshal(payload)
	params := map[string]any{
		"task_id": id.TaskID, "worker_id": id.WorkerID,
		"type": message.PermissionRequest, "category": message.Permission,
		"payload": json.RawMessage(raw),
	}
	if id.NativeSessionID != "" {
		params["native_session_id"] = id.NativeSessionID
	}
	response, err := callSupervisor(ctx, id.RunDir, id.RunID, "worker_request", params)
	if err != nil {
		return err
	}
	if !response.OK {
		return fmt.Errorf("Broker permission request failed: %s", response.Error)
	}
	data, _ := json.Marshal(response.Result)
	var wrapper struct {
		Resolution json.RawMessage `json:"resolution"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return err
	}
	resolution, err := message.DecodeResolutionForType(message.PermissionRequest, wrapper.Resolution)
	if err != nil {
		return err
	}
	decision := resolution.Decision
	hookOutput := map[string]any{"hookEventName": "PreToolUse", "permissionDecision": "deny", "permissionDecisionReason": decision.Reason}
	if decision.Allowed {
		var original any
		_ = json.Unmarshal(hook.ToolInput, &original)
		hookOutput = map[string]any{"hookEventName": "PreToolUse", "permissionDecision": "allow", "permissionDecisionReason": decision.Reason, "updatedInput": original}
	}
	return json.NewEncoder(output).Encode(map[string]any{"hookSpecificOutput": hookOutput})
}

var secretPattern = regexp.MustCompile(`(?i)(api[_-]?key|access[_-]?token|authorization|cookie|private[_-]?key)\s*[:=]\s*[^\s]+`)

func sanitizeJSON(raw json.RawMessage) json.RawMessage {
	text := string(raw)
	text = secretPattern.ReplaceAllString(text, "$1=[REDACTED]")
	if len(text) > 8192 {
		text = text[:8192] + `"[TRUNCATED]"`
	}
	if !json.Valid([]byte(text)) {
		encoded, _ := json.Marshal(map[string]string{"summary": strings.TrimSpace(text)})
		return encoded
	}
	return json.RawMessage(text)
}
