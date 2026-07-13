package grok

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
)

func TestRespondPermissionACPSelectedOutcomeNumericID(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "out.log")
	script := writePermissionFixture(t, logPath)
	a := New(script)
	session, err := a.StartSession(context.Background(), adapter.StartRequest{
		RunID: "r", TaskID: "t", WorkerID: "w", ProjectRoot: t.TempDir(), Contract: "fixture",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Wait for first prompt to complete so stdin is free for the response write.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = a.CollectFinalResult(ctx, session.NativeSessionID)

	if err := a.RespondPermission(ctx, session.NativeSessionID, adapter.PermissionDecision{
		RequestID: "5",
		Allowed:   true,
		OptionID:  "native-allow-42",
	}); err != nil {
		t.Fatal(err)
	}
	line := waitLogContains(t, logPath, "native-allow-42", 2*time.Second)
	assertACPPermissionResponse(t, line, 5, "native-allow-42")
}

func TestRespondPermissionACPSelectedOutcomeStringID(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "out.log")
	script := writePermissionFixture(t, logPath)
	a := New(script)
	session, err := a.StartSession(context.Background(), adapter.StartRequest{
		RunID: "r", TaskID: "t", WorkerID: "w", ProjectRoot: t.TempDir(), Contract: "fixture",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = a.CollectFinalResult(ctx, session.NativeSessionID)

	if err := a.RespondPermission(ctx, session.NativeSessionID, adapter.PermissionDecision{
		RequestID: "req-str-9",
		Allowed:   false,
		OptionID:  "native-reject-42",
	}); err != nil {
		t.Fatal(err)
	}
	line := waitLogContains(t, logPath, "native-reject-42", 2*time.Second)
	var msg struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  struct {
			Outcome struct {
				Outcome  string `json:"outcome"`
				OptionID string `json:"optionId"`
			} `json:"outcome"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		t.Fatalf("unmarshal %q: %v", line, err)
	}
	if msg.JSONRPC != "2.0" {
		t.Fatalf("jsonrpc=%s", msg.JSONRPC)
	}
	var id string
	if err := json.Unmarshal(msg.ID, &id); err != nil || id != "req-str-9" {
		t.Fatalf("id raw=%s parsed=%q err=%v", msg.ID, id, err)
	}
	if msg.Result.Outcome.Outcome != "selected" || msg.Result.Outcome.OptionID != "native-reject-42" {
		t.Fatalf("result=%+v", msg.Result)
	}
	// Must not emit legacy shapes.
	if strings.Contains(line, `"outcome":"allowed"`) || strings.Contains(line, `"outcome":"denied"`) {
		t.Fatalf("legacy outcome shape: %s", line)
	}
}

func TestRespondPermissionRequiresOptionID(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "out.log")
	script := writePermissionFixture(t, logPath)
	a := New(script)
	session, err := a.StartSession(context.Background(), adapter.StartRequest{
		RunID: "r", TaskID: "t", WorkerID: "w", ProjectRoot: t.TempDir(), Contract: "fixture",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = a.CollectFinalResult(ctx, session.NativeSessionID)
	err = a.RespondPermission(ctx, session.NativeSessionID, adapter.PermissionDecision{
		RequestID: "5", Allowed: true,
	})
	if err == nil {
		t.Fatal("expected error without optionId")
	}
}

func writePermissionFixture(t *testing.T, logPath string) string {
	t.Helper()
	// Log every stdin line after session is up so permission responses are captured.
	content := `#!/bin/sh
LOG="` + logPath + `"
while IFS= read -r line; do
  echo "$line" >> "$LOG"
  case "$line" in
    *'"method":"initialize"'*) echo '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}' ;;
    *'"method":"authenticate"'*) echo '{"jsonrpc":"2.0","id":2,"result":{}}' ;;
    *'"method":"session/new"'*) echo '{"jsonrpc":"2.0","id":3,"result":{"sessionId":"session-perm"}}' ;;
    *'"method":"session/prompt"'*)
      echo '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-perm","update":{"sessionUpdate":"agent_message_chunk","delta":"{\"schema_version\":\"v1alpha1\",\"task_id\":\"t\",\"worker_id\":\"w\",\"status\":\"succeeded\",\"summary\":\"done\",\"work_completed\":[\"done\"],\"files_changed\":[],\"no_files_changed_reason\":\"fixture\",\"validation\":[{\"command\":\"fixture\",\"passed\":true}],\"remaining_work\":[],\"blocking_issues\":[],\"risks\":[],\"handoff_notes\":[]}"}}}'
      echo '{"jsonrpc":"2.0","id":4,"result":{}}'
      ;;
  esac
done
`
	return writeExecutable(t, content)
}

func waitLogContains(t *testing.T, path, needle string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.Contains(line, needle) {
					return strings.TrimSpace(line)
				}
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("log %s did not contain %q", path, needle)
	return ""
}

func assertACPPermissionResponse(t *testing.T, line string, wantID int64, wantOption string) {
	t.Helper()
	var msg struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int64  `json:"id"`
		Result  struct {
			Outcome struct {
				Outcome  string `json:"outcome"`
				OptionID string `json:"optionId"`
			} `json:"outcome"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		t.Fatalf("line=%q err=%v", line, err)
	}
	if msg.JSONRPC != "2.0" || msg.ID != wantID {
		t.Fatalf("header=%+v want id=%d", msg, wantID)
	}
	if msg.Result.Outcome.Outcome != "selected" || msg.Result.Outcome.OptionID != wantOption {
		t.Fatalf("outcome=%+v", msg.Result.Outcome)
	}
	// Exact nested shape (no flat allowed/denied).
	var generic map[string]any
	_ = json.Unmarshal([]byte(line), &generic)
	result, _ := generic["result"].(map[string]any)
	outcome, _ := result["outcome"].(map[string]any)
	if outcome["outcome"] != "selected" {
		t.Fatalf("expected nested selected outcome, got %#v", result)
	}
}
