package contracttest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/claude"
	"github.com/vnai/subagent-broker/internal/adapter/codex"
	"github.com/vnai/subagent-broker/internal/adapter/grok"
	"github.com/vnai/subagent-broker/internal/adapter/opencode"
	"github.com/vnai/subagent-broker/internal/report"
)

func TestRealNativeAdapter(t *testing.T) {
	if os.Getenv("BROKER_REAL_HARNESS_TEST") != "1" {
		t.Skip("set BROKER_REAL_HARNESS_TEST=1 to run a real native adapter")
	}
	harnessName := os.Getenv("BROKER_TEST_HARNESS")
	var harness adapter.Adapter
	switch harnessName {
	case string(adapter.HarnessClaudeCode):
		harness = claude.New("")
	case string(adapter.HarnessCodex):
		harness = codex.New("")
	case string(adapter.HarnessGrokBuild):
		harness = grok.New("")
	case string(adapter.HarnessOpenCode):
		harness = opencode.New("")
	default:
		t.Skipf("BROKER_TEST_HARNESS=%q is not a Phase 4 native adapter", harnessName)
	}
	dir := t.TempDir()
	request := adapter.StartRequest{
		RunID: "real-run", TaskID: "real-task", WorkerID: "real-worker", ProjectRoot: dir,
		Contract: `Return exactly one JSON Result Envelope and no markdown: {"schema_version":"v1alpha1","task_id":"real-task","worker_id":"real-worker","status":"succeeded","summary":"native adapter smoke","work_completed":["native adapter smoke"],"files_changed":[],"no_files_changed_reason":"smoke does not edit files","validation":[{"command":"smoke","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]}`,
		Model:    os.Getenv("BROKER_REAL_MODEL"), Options: map[string]string{"max_turns": "4", "permission_mode": "bypassPermissions"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	session, err := harness.StartSession(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for range session.Events {
		}
	}()
	result, err := harness.CollectFinalResult(ctx, session.NativeSessionID)
	if err != nil {
		history, _ := harness.ReadHistory(context.Background(), session.NativeSessionID)
		last := ""
		if len(history) > 0 {
			last = string(history[len(history)-1].Payload)
		}
		t.Fatalf("%v; last_event=%s", err, last)
	}
	if err := report.ValidateEnvelope(result); err != nil {
		t.Fatal(err)
	}
	if result.TaskID != request.TaskID || result.WorkerID != request.WorkerID {
		t.Fatalf("result identity mismatch: %+v", result)
	}
	Record(Result{Harness: harnessName, Version: harness.Descriptor().TestedMaxVersion, Contract: "native_session_result", Status: "passed", Evidence: "real StartSession/CollectFinalResult completed", VerifiedAt: time.Now().UTC()})
}
