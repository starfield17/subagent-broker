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
