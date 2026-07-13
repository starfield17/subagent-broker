package codex

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
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
	history, err := a.ReadHistory(context.Background(), session.NativeSessionID)
	if err != nil || len(history) < 3 {
		t.Fatalf("history err=%v len=%d", err, len(history))
	}
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
