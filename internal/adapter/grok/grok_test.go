package grok

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
)

func TestACPSessionContract(t *testing.T) {
	script := filepath.Join(t.TempDir(), "grok-fixture")
	content := `#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}' ;;
    *'"method":"authenticate"'*) echo '{"jsonrpc":"2.0","id":2,"result":{}}' ;;
    *'"method":"session/new"'*) echo '{"jsonrpc":"2.0","id":3,"result":{"sessionId":"session-fixture"}}' ;;
    *'"method":"session/prompt"'*)
      echo '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-fixture","update":{"sessionUpdate":"agent_message_chunk","delta":"{\"schema_version\":\"v1alpha1\",\"task_id\":\"t\",\"worker_id\":\"w\",\"status\":\"succeeded\",\"summary\":\"done\",\"work_completed\":[\"done\"],\"files_changed\":[],\"no_files_changed_reason\":\"fixture\",\"validation\":[{\"command\":\"fixture\",\"passed\":true}],\"remaining_work\":[],\"blocking_issues\":[],\"risks\":[],\"handoff_notes\":[]}"}}}'
      echo '{"jsonrpc":"2.0","id":4,"result":{}}'
      ;;
  esac
done
`
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	a := New(script)
	session, err := a.StartSession(context.Background(), adapter.StartRequest{RunID: "r", TaskID: "t", WorkerID: "w", ProjectRoot: t.TempDir(), Contract: "fixture"})
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
}
