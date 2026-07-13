package codex

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/event"
)

func TestAppServerSessionContract(t *testing.T) {
	script := writeExecutable(t, `#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{"id":1,"result":{"userAgent":"fixture"}}' ;;
    *'"method":"thread/start"'*) echo '{"id":2,"result":{"thread":{"id":"thread-fixture"}}}' ;;
    *'"method":"turn/start"'*)
      echo '{"method":"turn/started","params":{"turn":{"id":"turn-fixture"}}}'
      echo '{"id":3,"result":{"turn":{"id":"turn-fixture"}}}'
      echo '{"method":"item/agentMessage/delta","params":{"delta":"{\"schema_version\":\"v1alpha1\",\"task_id\":\"t\",\"worker_id\":\"w\",\"status\":\"succeeded\",\"summary\":\"done\",\"work_completed\":[\"done\"],\"files_changed\":[],\"no_files_changed_reason\":\"fixture\",\"validation\":[{\"command\":\"fixture\",\"passed\":true}],\"remaining_work\":[],\"blocking_issues\":[],\"risks\":[],\"handoff_notes\":[]}"}}'
      echo '{"method":"turn/completed","params":{"usage":{"inputTokens":1,"outputTokens":2}}}'
      ;;
  esac
done
`)
	a := New(script)
	session, err := a.StartSession(context.Background(), testStartRequest(t, "t", "w"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := a.CollectFinalResult(ctx, session.NativeSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if result.TaskID != "t" || result.WorkerID != "w" {
		t.Fatalf("unexpected result identity: %+v", result)
	}
	// Immediately after CollectFinalResult, terminal history must already be committed.
	// No poll, no sleep — production linearizability.
	history, err := a.ReadHistory(context.Background(), session.NativeSessionID)
	if err != nil {
		t.Fatal(err)
	}
	assertKindsPresent(t, history, event.TurnStarted, event.ModelOutputDelta, event.ResultSubmitted)
	assertExactlyOneKind(t, history, event.ResultSubmitted)
	assertNoKind(t, history, event.TurnFailed)

	usage, err := a.GetUsage(ctx, session.NativeSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if usage.InputTokens != 1 || usage.OutputTokens != 2 {
		t.Fatalf("usage=%+v", usage)
	}
}

func TestCodexTerminalFailed(t *testing.T) {
	script := writeExecutable(t, `#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{"id":1,"result":{"userAgent":"fixture"}}' ;;
    *'"method":"thread/start"'*) echo '{"id":2,"result":{"thread":{"id":"thread-fail"}}}' ;;
    *'"method":"turn/start"'*)
      echo '{"method":"turn/started","params":{"turn":{"id":"turn-fail"}}}'
      echo '{"id":3,"result":{"turn":{"id":"turn-fail"}}}'
      echo '{"method":"turn/failed","params":{"error":"model exploded"}}'
      ;;
  esac
done
`)
	a := New(script)
	session, err := a.StartSession(context.Background(), testStartRequest(t, "t", "w"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = a.CollectFinalResult(ctx, session.NativeSessionID)
	if err == nil {
		t.Fatal("expected CollectFinalResult error on turn/failed")
	}
	history, err := a.ReadHistory(context.Background(), session.NativeSessionID)
	if err != nil {
		t.Fatal(err)
	}
	assertExactlyOneKind(t, history, event.TurnFailed)
	assertNoKind(t, history, event.ResultSubmitted)
	// EOF must not append a second terminal after protocol failure.
	// Wait briefly for process exit path without sleeping for correctness:
	// ReadHistory again after Exited if available.
	select {
	case <-session.Exited:
	case <-time.After(2 * time.Second):
	}
	history2, err := a.ReadHistory(context.Background(), session.NativeSessionID)
	if err != nil {
		t.Fatal(err)
	}
	assertExactlyOneKind(t, history2, event.TurnFailed)
}

func TestCodexTerminalCancelled(t *testing.T) {
	script := writeExecutable(t, `#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{"id":1,"result":{"userAgent":"fixture"}}' ;;
    *'"method":"thread/start"'*) echo '{"id":2,"result":{"thread":{"id":"thread-cancel"}}}' ;;
    *'"method":"turn/start"'*)
      echo '{"method":"turn/started","params":{"turn":{"id":"turn-cancel"}}}'
      echo '{"id":3,"result":{"turn":{"id":"turn-cancel"}}}'
      echo '{"method":"turn/cancelled","params":{"reason":"user"}}'
      ;;
  esac
done
`)
	a := New(script)
	session, err := a.StartSession(context.Background(), testStartRequest(t, "t", "w"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = a.CollectFinalResult(ctx, session.NativeSessionID)
	if err == nil {
		t.Fatal("expected CollectFinalResult error on turn/cancelled")
	}
	history, err := a.ReadHistory(context.Background(), session.NativeSessionID)
	if err != nil {
		t.Fatal(err)
	}
	assertExactlyOneKind(t, history, event.TurnFailed)
	assertNoKind(t, history, event.ResultSubmitted)
	select {
	case <-session.Exited:
	case <-time.After(2 * time.Second):
	}
	history2, _ := a.ReadHistory(context.Background(), session.NativeSessionID)
	assertExactlyOneKind(t, history2, event.TurnFailed)
}

func TestCodexUnexpectedEOF(t *testing.T) {
	// Process exits without any terminal protocol notification.
	script := writeExecutable(t, `#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{"id":1,"result":{"userAgent":"fixture"}}' ;;
    *'"method":"thread/start"'*) echo '{"id":2,"result":{"thread":{"id":"thread-eof"}}}' ;;
    *'"method":"turn/start"'*)
      echo '{"method":"turn/started","params":{"turn":{"id":"turn-eof"}}}'
      echo '{"id":3,"result":{"turn":{"id":"turn-eof"}}}'
      exit 0
      ;;
  esac
done
`)
	a := New(script)
	session, err := a.StartSession(context.Background(), testStartRequest(t, "t", "w"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = a.CollectFinalResult(ctx, session.NativeSessionID)
	if err == nil {
		t.Fatal("unexpected EOF must not look like success")
	}
	history, err := a.ReadHistory(context.Background(), session.NativeSessionID)
	if err != nil {
		t.Fatal(err)
	}
	assertNoKind(t, history, event.ResultSubmitted)
	assertExactlyOneKind(t, history, event.TurnFailed)
}

func writeExecutable(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "harness")
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func testStartRequest(t *testing.T, taskID, workerID string) adapter.StartRequest {
	return adapter.StartRequest{RunID: "run", TaskID: taskID, WorkerID: workerID, ProjectRoot: t.TempDir(), Contract: "fixture"}
}

func assertKindsPresent(t *testing.T, history []adapter.NativeEvent, kinds ...string) {
	t.Helper()
	have := map[string]bool{}
	for _, h := range history {
		have[h.Kind] = true
	}
	for _, k := range kinds {
		if !have[k] {
			t.Fatalf("history missing %q: %+v", k, kindsOf(history))
		}
	}
}

func assertExactlyOneKind(t *testing.T, history []adapter.NativeEvent, kind string) {
	t.Helper()
	n := 0
	for _, h := range history {
		if h.Kind == kind {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("want exactly one %q, got %d in %v", kind, n, kindsOf(history))
	}
}

func assertNoKind(t *testing.T, history []adapter.NativeEvent, kind string) {
	t.Helper()
	for _, h := range history {
		if h.Kind == kind {
			t.Fatalf("history must not contain %q: %v", kind, kindsOf(history))
		}
	}
}

func kindsOf(history []adapter.NativeEvent) []string {
	out := make([]string, 0, len(history))
	for _, h := range history {
		out = append(out, h.Kind)
	}
	return out
}

func TestCodexApprovalPolicyMapping(t *testing.T) {
	cases := []struct {
		mode string
		safe bool
		want string
	}{
		{mode: "default", want: "on-request"},
		{mode: "acceptEdits", want: "on-request"},
		{mode: "plan", want: "on-request"},
		{mode: "", want: "on-request"},
		{mode: "untrusted", want: "untrusted"},
		{mode: "bypassPermissions", want: "never"},
		{mode: "never", want: "never"},
		{mode: "default", safe: true, want: "never"},
	}
	for _, tc := range cases {
		req := adapter.StartRequest{Options: map[string]string{"permission_mode": tc.mode}}
		if tc.safe {
			req.Options["safe_mode"] = "true"
		}
		if got := codexApprovalPolicy(req); got != tc.want {
			t.Fatalf("mode=%q safe=%v got=%q want=%q", tc.mode, tc.safe, got, tc.want)
		}
	}
}

func TestCodexSessionConfigFactNativePermissions(t *testing.T) {
	a := New("codex")
	fact := a.SessionConfigFact(adapter.StartRequest{Options: map[string]string{"permission_mode": "default"}})
	if !fact.NativePermissionEvents || fact.HooksInstalled {
		t.Fatalf("expected native permissions without hooks: %+v", fact)
	}
	bypass := a.SessionConfigFact(adapter.StartRequest{Options: map[string]string{"permission_mode": "bypassPermissions"}})
	if bypass.NativePermissionEvents {
		t.Fatal("bypass mode must not claim native permission events")
	}
}

func TestCodexThreadStartUsesOnRequestPolicy(t *testing.T) {
	script := writeExecutable(t, `#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{"id":1,"result":{"userAgent":"fixture"}}' ;;
    *'"method":"thread/start"'*)
      case "$line" in
        *'"approvalPolicy":"on-request"'*) echo '{"id":2,"result":{"thread":{"id":"thread-policy"}}}' ;;
        *) echo '{"id":2,"error":{"message":"unexpected approvalPolicy"}}' ;;
      esac
      ;;
    *'"method":"turn/start"'*)
      echo '{"method":"turn/started","params":{"turn":{"id":"turn-fixture"}}}'
      echo '{"id":3,"result":{"turn":{"id":"turn-fixture"}}}'
      echo '{"method":"turn/completed","params":{"usage":{"inputTokens":1,"outputTokens":1}}}'
      ;;
  esac
done
`)
	a := New(script)
	req := testStartRequest(t, "t", "w")
	req.Options = map[string]string{"permission_mode": "default"}
	session, err := a.StartSession(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if session.NativeSessionID != "thread-policy" {
		t.Fatalf("session=%+v", session)
	}
}
