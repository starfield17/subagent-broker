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

func TestInterruptTurnIsNotificationWithoutResponseWait(t *testing.T) {
	// Fixture never answers session/cancel. If InterruptTurn registered a
	// pending RPC waiter it would block until context timeout (~2s).
	script := writeExecutable(t, `#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}' ;;
    *'"method":"authenticate"'*) echo '{"jsonrpc":"2.0","id":2,"result":{}}' ;;
    *'"method":"session/new"'*) echo '{"jsonrpc":"2.0","id":3,"result":{"sessionId":"session-cancel"}}' ;;
    *'"method":"session/prompt"'*)
      # Hang the prompt so cancel can be issued mid-turn; never reply to cancel.
      sleep 30
      ;;
  esac
done
`)
	a := New(script)
	session, err := a.StartSession(context.Background(), adapter.StartRequest{
		RunID: "r", TaskID: "t", WorkerID: "w", ProjectRoot: t.TempDir(), Contract: "fixture",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Brief pause so the hanging prompt is in flight.
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	if err := a.InterruptTurn(ctx, session.NativeSessionID); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	// Notification path returns as soon as the write completes; a response waiter
	// would block until the 2s context deadline.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("InterruptTurn took %s; likely waited for a nonexistent cancel response", elapsed)
	}
}

func TestMultiTurnKeepsSessionAliveForSendMessage(t *testing.T) {
	script := writeExecutable(t, `#!/bin/sh
n=0
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}' ;;
    *'"method":"authenticate"'*) echo '{"jsonrpc":"2.0","id":2,"result":{}}' ;;
    *'"method":"session/new"'*) echo '{"jsonrpc":"2.0","id":3,"result":{"sessionId":"session-mt"}}' ;;
    *'"method":"session/prompt"'*)
      n=$((n+1))
      echo "{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"sessionId\":\"session-mt\",\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"delta\":\"{\\\"schema_version\\\":\\\"v1alpha1\\\",\\\"task_id\\\":\\\"t\\\",\\\"worker_id\\\":\\\"w\\\",\\\"status\\\":\\\"succeeded\\\",\\\"summary\\\":\\\"turn-$n\\\",\\\"work_completed\\\":[\\\"done\\\"],\\\"files_changed\\\":[],\\\"no_files_changed_reason\\\":\\\"fixture\\\",\\\"validation\\\":[{\\\"command\\\":\\\"fixture\\\",\\\"passed\\\":true}],\\\"remaining_work\\\":[],\\\"blocking_issues\\\":[],\\\"risks\\\":[],\\\"handoff_notes\\\":[]}\"}}}"
      # Echo response with matching id extracted loosely: fixtures use sequential ids.
      case "$line" in
        *'"id":4'*) echo '{"jsonrpc":"2.0","id":4,"result":{}}' ;;
        *'"id":5'*) echo '{"jsonrpc":"2.0","id":5,"result":{}}' ;;
        *) echo '{"jsonrpc":"2.0","id":4,"result":{}}' ;;
      esac
      ;;
  esac
done
`)
	a := New(script)
	session, err := a.StartSession(context.Background(), adapter.StartRequest{
		RunID: "r", TaskID: "t", WorkerID: "w", ProjectRoot: t.TempDir(), Contract: "first",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := a.CollectFinalResult(ctx, session.NativeSessionID); err != nil {
		t.Fatal(err)
	}
	// Session must still accept a follow-up prompt (stdin not closed).
	if _, err := a.SendMessage(ctx, session.NativeSessionID, "second"); err != nil {
		t.Fatalf("SendMessage after first turn: %v", err)
	}
	result, err := a.CollectFinalResult(ctx, session.NativeSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary != "turn-2" {
		t.Fatalf("summary=%q want turn-2", result.Summary)
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
