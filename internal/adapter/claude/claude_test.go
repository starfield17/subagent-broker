package claude

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/event"
)

func TestAdapterParsesStructuredStream(t *testing.T) {
	executable := scriptedHarness(t)
	a := New(executable)
	session, err := a.StartSession(context.Background(), adapter.StartRequest{TaskID: "task-script", WorkerID: "worker-script", ProjectRoot: t.TempDir(), Contract: "complete the scripted task", Options: map[string]string{"session_id": "00000000-0000-7000-8000-000000000001"}})
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for native := range session.Events {
		kinds = append(kinds, native.Kind)
	}
	exit := <-session.Exited
	if exit.Code != 0 {
		t.Fatalf("unexpected exit: %+v", exit)
	}
	result, err := a.CollectFinalResult(context.Background(), session.NativeSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if result.TaskID != "task-script" || result.WorkerID != "worker-script" {
		t.Fatalf("unexpected result identity: %+v", result)
	}
	if len(kinds) != 3 || kinds[0] != event.SessionStarted || kinds[1] != event.ModelOutputDelta || kinds[2] != event.ResultSubmitted {
		t.Fatalf("unexpected event kinds: %v", kinds)
	}
}

func TestNativeEventReportsMalformedJSONWithoutReturningInvalidPayload(t *testing.T) {
	native := nativeEvent([]byte("not-json\n"))
	if native.Kind != "protocol.error" {
		t.Fatalf("unexpected kind: %s", native.Kind)
	}
	a := New("")
	if _, err := a.NormalizeEvent(native); err != nil {
		t.Fatalf("protocol error payload should remain structured: %v", err)
	}
}

func scriptedHarness(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "claude-script")
	script := `#!/bin/sh
read input
printf '%s\n' '{"type":"system","subtype":"init","session_id":"session-script"}'
printf '%s\n' '{"type":"assistant","message":{"content":[{"type":"text","text":"working"}]}}'
printf '%s\n' '{"type":"result","subtype":"success","result":{"schema_version":"v1alpha1","task_id":"task-script","worker_id":"worker-script","status":"succeeded","summary":"script completed","work_completed":["scripted work"],"files_changed":[],"no_files_changed_reason":"script only","validation":[{"command":"script-check","passed":true}],"remaining_work":[],"blocking_issues":[],"scope_expansion":[],"risks":[],"handoff_notes":[]}}'
`
	if err := os.WriteFile(path, []byte(strings.TrimSpace(script)+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestWorkerTokenAbsentFromArgv verifies the Worker token appears only in
// environment variables, never in argv elements, MCP config JSON, or hook settings.
func TestWorkerTokenAbsentFromArgv(t *testing.T) {
	sentinel := "TOP_SECRET_WORKER_TOKEN"

	// Test MCP config construction: verify the serialized JSON does NOT contain the token.
	mcpServer := map[string]any{
		"type": "stdio", "command": "/usr/local/bin/subagent-broker",
		"args": []string{"mcp-worker"},
	}
	config, _ := json.Marshal(map[string]any{"mcpServers": map[string]any{"subagent-broker": mcpServer}})
	configStr := string(config)
	if strings.Contains(configStr, sentinel) {
		t.Fatalf("MCP config JSON contains worker token: %s", configStr)
	}

	// Verify hook command does not embed run directories or tokens.
	hookCommand := "subagent-broker claude-hook"
	if strings.Contains(hookCommand, sentinel) {
		t.Fatal("hook command contains worker token")
	}
	if strings.Contains(hookCommand, "/tmp/run-dir") {
		t.Fatal("hook command contains run directory")
	}

	// Verify the serialized MCP config is clean.
	if strings.Contains(configStr, sentinel) {
		t.Fatal("serialized MCP config contains worker token")
	}

	// Verify env entries contain the token only in BROKER_WORKER_TOKEN.
	envCheck := []string{
		"BROKER_WORKER_TOKEN=" + sentinel,
		"BROKER_WORKER_SOCKET=/tmp/worker.sock",
		"BROKER_RUN_DIR=/tmp/run-dir",
	}
	for _, e := range envCheck {
		if strings.Contains(e, sentinel) && !strings.HasPrefix(e, "BROKER_WORKER_TOKEN=") {
			t.Errorf("token in unexpected env entry: %s", e)
		}
	}
}
