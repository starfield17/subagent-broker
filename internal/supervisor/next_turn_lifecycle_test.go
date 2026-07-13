package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/state"
)

var errSendBoom = errors.New("send boom")

func validEnvelope(taskID, workerID, summary string) *report.Envelope {
	return &report.Envelope{
		SchemaVersion: report.SchemaVersion, TaskID: taskID, WorkerID: workerID,
		Status: report.StatusSucceeded, Summary: summary, WorkCompleted: []string{"done"},
		NoFilesChangedReason: "fixture", Validation: []report.Validation{{Command: "true", Passed: true}},
	}
}

func newLifecycleService(t *testing.T, harness *fake.Adapter, taskID string) (*Service, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-life", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	service := newCommitService(&fakeEventAppender{}, func(Snapshot) error { return nil })
	service.paths = storagePaths(t)
	service.messages = store
	service.messageIndex = map[string]message.Message{}
	service.router = router
	service.pending = map[string]*pendingWaiter{}
	service.active = map[string]activeWorker{}
	service.config.CancelGrace = 200 * time.Millisecond
	service.snapshot.Run.RunID = "run-life"
	service.snapshot.Tasks = []TaskState{{
		Task: domain.Task{TaskID: domain.TaskID(taskID), WriteScope: []string{"a/**"}, ProjectRoot: t.TempDir(), WaveID: "wave-1"},
		Worker: &domain.WorkerSession{
			WorkerID: "worker-a", TaskID: domain.TaskID(taskID), Attempt: 1, Harness: string(adapter.HarnessFake),
			Capabilities: adapter.CapabilityMap(adapter.Capabilities{
				BidirectionalStream: true, PermissionEvents: true, StructuredStream: true,
			}),
			StatusDimensions: state.Dimensions{
				Process: state.ProcessAlive, Protocol: state.ProtocolThinking,
				Progress: state.ProgressActive, Task: state.TaskRunning,
			},
		},
		ActiveAttempt: 1,
		Dimensions:    state.Dimensions{Task: state.TaskRunning, Process: state.ProcessAlive},
	}}
	_ = harness
	return service, path
}

func TestRunWorkerSessionKeepsAliveForQueuedNextTurn(t *testing.T) {
	first := validEnvelope("task-a", "worker-a", "turn-1")
	second := validEnvelope("task-a", "worker-a", "turn-2")
	harness := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, StructuredFinalOutput: true,
	})
	if err := harness.RegisterScenario(fake.Scenario{
		Name: "multi_turn",
		Events: []adapter.NativeEvent{
			{Kind: event.TurnStarted},
			{Kind: event.ResultSubmitted},
		},
		Final:    first,
		KeepOpen: true,
		FollowUpBatches: [][]adapter.NativeEvent{
			{{Kind: event.TurnStarted}, {Kind: event.ResultSubmitted}},
		},
		FollowUpFinals: []*report.Envelope{second},
	}); err != nil {
		t.Fatal(err)
	}

	service, _ := newLifecycleService(t, harness, "task-a")
	session, err := harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-life", TaskID: "task-a", WorkerID: "worker-a",
		ProjectRoot: t.TempDir(), Contract: "c", Scenario: "multi_turn",
	})
	if err != nil {
		t.Fatal(err)
	}
	service.active["task-a"] = activeWorker{
		adapter: harness, sessionID: session.NativeSessionID, cancel: func() {},
		taskID: "task-a", workerID: "worker-a",
	}
	// Queue one next-turn instruction before the session loop sees ResultSubmitted.
	if _, err := service.SendInstruction(context.Background(), "task-a", "second-turn"); err != nil {
		t.Fatal(err)
	}

	runtime := service.snapshot.Tasks[0]
	runtime.Worker.NativeSessionID = session.NativeSessionID
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := service.runWorkerSession(ctx, &runtime, harness, session, "worker-a", process.Identity{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.ResultSeen {
		t.Fatal("expected final ResultSeen after second turn")
	}
	if len(harness.SentMessages) != 1 || harness.SentMessages[0] != "second-turn" {
		t.Fatalf("sent=%v", harness.SentMessages)
	}
	// Final envelope must be turn-2.
	env, err := harness.CollectFinalResult(context.Background(), session.NativeSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if env.Summary != "turn-2" {
		t.Fatalf("summary=%q want turn-2", env.Summary)
	}
}

func TestRunWorkerSessionNoQueueFirstResultFinal(t *testing.T) {
	harness := fake.New(adapter.Capabilities{StructuredStream: true, BidirectionalStream: true, StructuredFinalOutput: true})
	if err := harness.RegisterScenario(fake.Scenario{
		Name: "single",
		Events: []adapter.NativeEvent{
			{Kind: event.TurnStarted}, {Kind: event.ResultSubmitted},
		},
		Final:    validEnvelope("task-a", "worker-a", "only"),
		KeepOpen: true,
	}); err != nil {
		t.Fatal(err)
	}
	service, _ := newLifecycleService(t, harness, "task-a")
	session, err := harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-life", TaskID: "task-a", WorkerID: "worker-a",
		ProjectRoot: t.TempDir(), Contract: "c", Scenario: "single",
	})
	if err != nil {
		t.Fatal(err)
	}
	service.active["task-a"] = activeWorker{adapter: harness, sessionID: session.NativeSessionID, cancel: func() {}, taskID: "task-a", workerID: "worker-a"}
	runtime := service.snapshot.Tasks[0]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := service.runWorkerSession(ctx, &runtime, harness, session, "worker-a", process.Identity{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.ResultSeen {
		t.Fatal("expected ResultSeen")
	}
	if len(harness.SentMessages) != 0 {
		t.Fatalf("no next turn expected: %v", harness.SentMessages)
	}
}

func TestRunWorkerSessionFIFOOnePerBoundary(t *testing.T) {
	harness := fake.New(adapter.Capabilities{StructuredStream: true, BidirectionalStream: true, StructuredFinalOutput: true})
	if err := harness.RegisterScenario(fake.Scenario{
		Name: "fifo",
		Events: []adapter.NativeEvent{
			{Kind: event.ResultSubmitted},
		},
		Final:    validEnvelope("task-a", "worker-a", "t1"),
		KeepOpen: true,
		FollowUpBatches: [][]adapter.NativeEvent{
			{{Kind: event.ResultSubmitted}},
			{{Kind: event.ResultSubmitted}},
		},
		FollowUpFinals: []*report.Envelope{
			validEnvelope("task-a", "worker-a", "t2"),
			validEnvelope("task-a", "worker-a", "t3"),
		},
	}); err != nil {
		t.Fatal(err)
	}
	service, _ := newLifecycleService(t, harness, "task-a")
	session, err := harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-life", TaskID: "task-a", WorkerID: "worker-a",
		ProjectRoot: t.TempDir(), Contract: "c", Scenario: "fifo",
	})
	if err != nil {
		t.Fatal(err)
	}
	service.active["task-a"] = activeWorker{adapter: harness, sessionID: session.NativeSessionID, cancel: func() {}, taskID: "task-a", workerID: "worker-a"}
	if _, err := service.SendInstruction(context.Background(), "task-a", "first-queued"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SendInstruction(context.Background(), "task-a", "second-queued"); err != nil {
		t.Fatal(err)
	}
	runtime := service.snapshot.Tasks[0]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := service.runWorkerSession(ctx, &runtime, harness, session, "worker-a", process.Identity{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.ResultSeen {
		t.Fatal("expected final result")
	}
	if len(harness.SentMessages) != 2 {
		t.Fatalf("want 2 sequential sends, got %v", harness.SentMessages)
	}
	if harness.SentMessages[0] != "first-queued" || harness.SentMessages[1] != "second-queued" {
		t.Fatalf("FIFO order: %v", harness.SentMessages)
	}
	env, _ := harness.CollectFinalResult(context.Background(), session.NativeSessionID)
	if env.Summary != "t3" {
		t.Fatalf("final summary=%q", env.Summary)
	}
}

func TestRunWorkerSessionNextTurnFailureKeepsFirstFinal(t *testing.T) {
	harness := fake.New(adapter.Capabilities{StructuredStream: true, BidirectionalStream: true, StructuredFinalOutput: true})
	if err := harness.RegisterScenario(fake.Scenario{
		Name:     "fail-next",
		Events:   []adapter.NativeEvent{{Kind: event.ResultSubmitted}},
		Final:    validEnvelope("task-a", "worker-a", "first-final"),
		KeepOpen: true,
	}); err != nil {
		t.Fatal(err)
	}
	harness.FailSendMessage = errSendBoom
	service, _ := newLifecycleService(t, harness, "task-a")
	session, err := harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-life", TaskID: "task-a", WorkerID: "worker-a",
		ProjectRoot: t.TempDir(), Contract: "c", Scenario: "fail-next",
	})
	if err != nil {
		t.Fatal(err)
	}
	service.active["task-a"] = activeWorker{adapter: harness, sessionID: session.NativeSessionID, cancel: func() {}, taskID: "task-a", workerID: "worker-a"}
	queued, err := service.SendInstruction(context.Background(), "task-a", "will-fail")
	if err != nil {
		t.Fatal(err)
	}
	runtime := service.snapshot.Tasks[0]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := service.runWorkerSession(ctx, &runtime, harness, session, "worker-a", process.Identity{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.ResultSeen {
		t.Fatal("first result remains final after next-turn failure")
	}
	got, _ := service.router.Get(queued.MessageID)
	if got.Status != message.Failed {
		t.Fatalf("instruction status=%s", got.Status)
	}
	env, _ := harness.CollectFinalResult(context.Background(), session.NativeSessionID)
	if env.Summary != "first-final" {
		t.Fatalf("summary=%q", env.Summary)
	}
}

func TestRunWorkerSessionTurnFailedDoesNotStartNext(t *testing.T) {
	harness := fake.New(adapter.Capabilities{StructuredStream: true, BidirectionalStream: true})
	if err := harness.RegisterScenario(fake.Scenario{
		Name:     "turn-fail",
		Events:   []adapter.NativeEvent{{Kind: event.TurnFailed}},
		KeepOpen: true,
	}); err != nil {
		t.Fatal(err)
	}
	service, _ := newLifecycleService(t, harness, "task-a")
	session, err := harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-life", TaskID: "task-a", WorkerID: "worker-a",
		ProjectRoot: t.TempDir(), Contract: "c", Scenario: "turn-fail",
	})
	if err != nil {
		t.Fatal(err)
	}
	service.active["task-a"] = activeWorker{adapter: harness, sessionID: session.NativeSessionID, cancel: func() {}, taskID: "task-a", workerID: "worker-a"}
	if _, err := service.SendInstruction(context.Background(), "task-a", "should-not-send"); err != nil {
		t.Fatal(err)
	}
	runtime := service.snapshot.Tasks[0]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := service.runWorkerSession(ctx, &runtime, harness, session, "worker-a", process.Identity{})
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultSeen {
		t.Fatal("TurnFailed must not set ResultSeen")
	}
	if len(harness.SentMessages) != 0 {
		t.Fatalf("must not start next turn on TurnFailed: %v", harness.SentMessages)
	}
}

func TestRunWorkerSessionSecondTurnPermissionNoDeadlock(t *testing.T) {
	// Second turn emits a permission request then result after allow.
	permPayload, _ := json.Marshal(map[string]any{
		"id": "perm-t2", "tool_name": "Bash", "input": map[string]string{"command": "ls"},
	})
	harness := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true, StructuredFinalOutput: true,
	})
	if err := harness.RegisterScenario(fake.Scenario{
		Name: "perm-turn2",
		Events: []adapter.NativeEvent{
			{Kind: event.ResultSubmitted},
		},
		Final:    validEnvelope("task-a", "worker-a", "t1"),
		KeepOpen: true,
		FollowUpBatches: [][]adapter.NativeEvent{
			{
				{Kind: event.PermissionRequested, Payload: permPayload},
				{Kind: event.ResultSubmitted},
			},
		},
		FollowUpFinals: []*report.Envelope{validEnvelope("task-a", "worker-a", "t2-after-perm")},
	}); err != nil {
		t.Fatal(err)
	}
	service, _ := newLifecycleService(t, harness, "task-a")
	session, err := harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-life", TaskID: "task-a", WorkerID: "worker-a",
		ProjectRoot: t.TempDir(), Contract: "c", Scenario: "perm-turn2",
	})
	if err != nil {
		t.Fatal(err)
	}
	service.active["task-a"] = activeWorker{
		adapter: harness, sessionID: session.NativeSessionID, cancel: func() {},
		taskID: "task-a", workerID: "worker-a", attempt: 1,
	}
	service.snapshot.Tasks[0].Worker.NativeSessionID = session.NativeSessionID
	service.snapshot.Tasks[0].Worker.Attempt = 1
	if _, err := service.SendInstruction(context.Background(), "task-a", "go-turn-2"); err != nil {
		t.Fatal(err)
	}

	// Resolve permissions as they appear while the session loop runs.
	done := make(chan workerSessionResult, 1)
	runtime := service.snapshot.Tasks[0]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		res, err := service.runWorkerSession(ctx, &runtime, harness, session, "worker-a", process.Identity{})
		if err != nil {
			t.Errorf("runWorkerSession: %v", err)
		}
		done <- res
	}()

	// Wait for permission message then resolve.
	deadline := time.Now().Add(3 * time.Second)
	var permID string
	for time.Now().Before(deadline) {
		for _, m := range service.router.PendingDecisions("task-a") {
			if m.Type == message.PermissionRequest {
				permID = m.MessageID
				break
			}
		}
		if permID != "" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if permID == "" {
		t.Fatal("permission request not bridged for second turn")
	}
	// SendMessage must already have returned (instruction Delivered).
	if len(harness.SentMessages) != 1 {
		t.Fatalf("expected SendMessage completed, sent=%v", harness.SentMessages)
	}
	if err := service.ResolveMessage(permID, message.NewDecisionResolution(true, "ok", false)); err != nil {
		t.Fatal(err)
	}
	if len(harness.PermissionResponses) != 1 || !harness.PermissionResponses[0].Allowed {
		t.Fatalf("permission responses=%+v", harness.PermissionResponses)
	}

	select {
	case res := <-done:
		if !res.ResultSeen {
			t.Fatal("expected second turn result")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("deadlock: session did not complete after permission")
	}
	env, _ := harness.CollectFinalResult(context.Background(), session.NativeSessionID)
	if env.Summary != "t2-after-perm" {
		t.Fatalf("summary=%q", env.Summary)
	}
}
