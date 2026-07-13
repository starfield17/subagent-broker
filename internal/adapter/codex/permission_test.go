package codex

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
)

func TestCodexRespondPermissionAcceptDeclineUnchanged(t *testing.T) {
	// Keep the process alive (no turn/completed exit path required): hang after start.
	logPath := filepath.Join(t.TempDir(), "codex.log")
	script := writeExecutable(t, `#!/bin/sh
LOG="`+logPath+`"
while IFS= read -r line; do
  echo "$line" >> "$LOG"
  case "$line" in
    *'"method":"initialize"'*) echo '{"id":1,"result":{"userAgent":"fixture"}}' ;;
    *'"method":"thread/start"'*) echo '{"id":2,"result":{"thread":{"id":"thread-perm"}}}' ;;
    *'"method":"turn/start"'*)
      echo '{"method":"turn/started","params":{"turn":{"id":"turn-1"}}}'
      echo '{"id":3,"result":{"turn":{"id":"turn-1"}}}'
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := a.RespondPermission(ctx, session.NativeSessionID, adapter.PermissionDecision{
		RequestID: "7", Allowed: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.RespondPermission(ctx, session.NativeSessionID, adapter.PermissionDecision{
		RequestID: "8", Allowed: false,
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var data []byte
	for time.Now().Before(deadline) {
		data, _ = os.ReadFile(logPath)
		if strings.Contains(string(data), `"decision":"accept"`) && strings.Contains(string(data), `"decision":"decline"`) {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	text := string(data)
	if !strings.Contains(text, `"decision":"accept"`) || !strings.Contains(text, `"decision":"decline"`) {
		t.Fatalf("expected Codex accept/decline decisions, log=%s", text)
	}
	if strings.Contains(text, `"optionId"`) {
		t.Fatal("Codex must not emit ACP optionId responses")
	}
}
