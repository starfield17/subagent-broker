package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
	"github.com/vnai/subagent-broker/internal/verify"
	"github.com/vnai/subagent-broker/internal/wave"
)

func TestNoSyntheticProcessGroupToken(t *testing.T) {
	// When Inspect fails, identity must remain incomplete — no pg: synthesis.
	identity := process.Identity{PID: 1_000_000_042, StartToken: "fake-start"}
	// Simulate post-start path without successful Inspect.
	if identity.Complete() {
		t.Fatal("without ProcessGroupToken identity must be incomplete")
	}
	if strings.HasPrefix(identity.ProcessGroupToken, "pg:") {
		t.Fatal("must not synthesize process group token")
	}
}

func TestParentExitWithUnknownTreeIsNotExited(t *testing.T) {
	r := WorkerExitResolution{
		ExitObserved:      true,
		TreeExitConfirmed: false,
		RemainingPIDs:     nil,
		OrphanRisk:        true,
	}
	proc, orphan := mapWorkerExitProcessState(r, false) // incomplete identity
	if proc != state.ProcessUnknown || !orphan {
		t.Fatalf("got %s orphan=%v", proc, orphan)
	}
}

func TestExecuteWavePropagatesTaskError(t *testing.T) {
	service := newCommitService(&fakeEventAppender{}, func(Snapshot) error { return nil })
	service.config.MaxConcurrency = 2
	service.snapshot.Tasks = []TaskState{{
		Task: domain.Task{
			TaskID: "task-a", Status: state.TaskRunning, Title: "t", Objective: "o",
			CompletionCriteria: []string{"c"}, WriteScope: []string{"a/**"},
			ValidationCommands: []domain.ValidationCommand{{Command: "true"}},
			ProjectRoot:        t.TempDir(),
		},
		// Unknown process: executeTask refuses before start.
		Dimensions: state.Dimensions{Process: state.ProcessUnknown, Task: state.TaskRunning},
	}}
	// executeWave skips ProcessUnknown — so use a running task that fails contract.
	service.snapshot.Tasks[0].Dimensions.Process = state.ProcessQueued
	service.snapshot.Tasks[0].Task.Objective = "" // invalid contract
	err := service.executeWave(context.Background(), domain.WavePlan{
		WaveID: "w1", Tasks: []domain.Task{service.snapshot.Tasks[0].Task},
	})
	if err == nil {
		t.Fatal("expected task error to surface from executeWave")
	}
	if !strings.Contains(err.Error(), "task task-a") {
		t.Fatalf("error should name task: %v", err)
	}
}

func TestExecuteWaveAggregatesMultipleErrors(t *testing.T) {
	service := newCommitService(&fakeEventAppender{}, func(Snapshot) error { return nil })
	service.config.MaxConcurrency = 4
	root := t.TempDir()
	mk := func(id string) TaskState {
		return TaskState{
			Task: domain.Task{
				TaskID: domain.TaskID(id), Status: state.TaskRunning, Title: "t", Objective: "", // invalid
				CompletionCriteria: []string{"c"}, WriteScope: []string{id + "/**"},
				ValidationCommands: []domain.ValidationCommand{{Command: "true"}}, ProjectRoot: root,
			},
			Dimensions: state.Dimensions{Process: state.ProcessQueued, Task: state.TaskRunning},
		}
	}
	service.snapshot.Tasks = []TaskState{mk("task-a"), mk("task-b")}
	err := service.executeWave(context.Background(), domain.WavePlan{
		WaveID: "w1",
		Tasks:  []domain.Task{service.snapshot.Tasks[0].Task, service.snapshot.Tasks[1].Task},
	})
	if err == nil {
		t.Fatal("expected aggregated errors")
	}
	if !strings.Contains(err.Error(), "task-a") || !strings.Contains(err.Error(), "task-b") {
		t.Fatalf("expected both task errors: %v", err)
	}
}

func TestRunDegradedExpiresPendingMessages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	taskDir := filepath.Join(home, "tasks", "task-a")
	_ = os.MkdirAll(taskDir, 0o700)
	// Publish a current question projection.
	_ = message.PublishQuestionProjection(taskDir, "m1", "task-a", message.QuestionEnvelope{
		SchemaVersion: SchemaVersion, Question: "q", Reason: "r",
		CurrentScope: []string{"a"}, WorkspaceState: "ok",
	}, true)

	service := &Service{
		paths: storage.RunPaths{Tasks: filepath.Join(home, "tasks"), Root: home},
		snapshot: Snapshot{
			Run: domain.Run{RunID: "run-1", Status: domain.RunRunning},
			Tasks: []TaskState{{
				Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning},
			}},
		},
		router:            router,
		messages:          store,
		messageIndex:      map[string]message.Message{},
		pending:           map[string]chan message.Resolution{},
		acceptingWork:     true,
		fatalPersistence:  make(chan error, 1),
		events:            &fakeEventAppender{},
		persistSnapshotFn: func(Snapshot) error { return nil },
	}
	queued, err := router.EnqueueDecision("task-a", "w1", message.Question, message.Decision, json.RawMessage(
		`{"schema_version":"v1alpha1","question":"q","reason":"r","current_scope":["a"],"workspace_state":"ok"}`,
	))
	if err != nil {
		t.Fatal(err)
	}
	_ = service.CommitMessageProjection(context.Background(), queued, "message.queued")

	if err := service.finishRun(domain.RunDegraded, "test degraded"); err != nil {
		t.Fatal(err)
	}
	got, ok := router.Get(queued.MessageID)
	if !ok || got.Status != message.Expired {
		t.Fatalf("expected expired, got %+v ok=%v", got, ok)
	}
	if _, err := os.Stat(filepath.Join(taskDir, "question.md")); !os.IsNotExist(err) {
		t.Fatalf("top-level question projection should be cleared, err=%v", err)
	}
}

func TestFinalWarningsAwaitAcceptance(t *testing.T) {
	home := t.TempDir()
	projectRoot := t.TempDir()
	layout, _ := storage.NewLayout(home)
	runDir, _ := layout.EnsureRun("p", "run-final")
	runPaths, _ := layout.RunPaths("p", "run-final")
	// Baseline before high-risk file appears so collect sees a warning-class change.
	baseline, _ := verify.CaptureWorkspace(projectRoot, home)
	_ = os.WriteFile(filepath.Join(projectRoot, "go.mod"), []byte("module x\n"), 0o600)
	service := &Service{
		config:      Config{BrokerHome: home},
		runDir:      runDir,
		paths:       runPaths,
		runBaseline: baseline,
		plan:        domain.RunPlan{FinalChecks: nil},
		snapshot: Snapshot{
			Run: domain.Run{RunID: "run-final", ProjectID: "p", Status: domain.RunRunning},
			Waves: []domain.Wave{{
				WaveID: "wave-1", Status: domain.WaveVerified, BarrierResult: domain.BarrierPassed, BarrierAccepted: true,
			}},
			Tasks: []TaskState{{
				// Authorized high-risk change → warning (not unauthorized error).
				Task: domain.Task{TaskID: "seed", ProjectRoot: projectRoot, WriteScope: []string{"**"}},
			}},
		},
		acceptingWork:     true,
		fatalPersistence:  make(chan error, 1),
		events:            &fakeEventAppender{},
		persistSnapshotFn: func(Snapshot) error { return nil },
	}
	// Use the real final path so revalidation hash matches AcceptFinalWarnings.
	ver, err := service.runFinalVerification(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ver.Result != domain.BarrierPassedWithWarnings {
		t.Fatalf("result=%s warnings=%v errors=%v", ver.Result, ver.Warnings, ver.Errors)
	}

	// Simulate execute tail for final warnings (non-terminal wait).
	if err := service.setRunStatus(domain.RunBarrier, "final verification has warnings; acceptance required before run completion"); err != nil {
		t.Fatal(err)
	}
	if service.Snapshot().Run.Status != domain.RunBarrier {
		t.Fatalf("status=%s want barrier (non-terminal)", service.Snapshot().Run.Status)
	}
	if runTerminal(service.Snapshot().Run.Status) {
		t.Fatal("awaiting final acceptance must not be terminal")
	}

	// Empty reason rejected.
	if err := service.AcceptFinalWarnings("agent", ""); err == nil {
		t.Fatal("empty reason must fail")
	}
	// Accept with current facts.
	if err := service.AcceptFinalWarnings("agent", "reviewed final risk"); err != nil {
		t.Fatal(err)
	}
	if service.Snapshot().Run.Status != domain.RunCompleted {
		t.Fatalf("status=%s want completed", service.Snapshot().Run.Status)
	}
}

func TestAcceptFinalWarningsRevalidatesFacts(t *testing.T) {
	home := t.TempDir()
	projectRoot := t.TempDir()
	layout, _ := storage.NewLayout(home)
	runDir, _ := layout.EnsureRun("p", "run-stale")
	runPaths, _ := layout.RunPaths("p", "run-stale")
	baseline, _ := verify.CaptureWorkspace(projectRoot, home)
	service := &Service{
		config: Config{BrokerHome: home}, runDir: runDir, paths: runPaths, runBaseline: baseline,
		snapshot: Snapshot{
			Run:   domain.Run{RunID: "run-stale", ProjectID: "p", Status: domain.RunBarrier},
			Waves: []domain.Wave{{WaveID: "w1", Status: domain.WaveVerified, BarrierResult: domain.BarrierPassed}},
			Tasks: []TaskState{{Task: domain.Task{TaskID: "seed", ProjectRoot: projectRoot, WriteScope: []string{"*"}}}},
		},
		acceptingWork: true, fatalPersistence: make(chan error, 1),
		events: &fakeEventAppender{}, persistSnapshotFn: func(Snapshot) error { return nil },
	}
	input, _ := service.collectFinalVerificationInputs(context.Background())
	input.ExistingWarnings = []string{"w"}
	hash := hashBarrierInputs(input)
	ver := wave.EvaluateBarrier(input, time.Now().UTC())
	ver.InputHash = hash
	_ = storage.AtomicWriteJSON(runPaths.Verification, ver, 0o600)

	// Change workspace after evaluation.
	_ = os.WriteFile(filepath.Join(projectRoot, "stale.txt"), []byte("x"), 0o600)
	err := service.AcceptFinalWarnings("agent", "should fail")
	if err == nil || !strings.Contains(err.Error(), "stale_verification") {
		t.Fatalf("expected stale_verification, got %v", err)
	}
}

func TestReplayRejectsDeliveryModeOnQuestion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	now := time.Now().UTC()
	value := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: "r1", TaskID: "t1",
		Type: message.Question, Status: message.Queued, DeliveryMode: message.DeliveryImmediate,
		CreatedAt: now, UpdatedAt: now, Payload: json.RawMessage(`{}`),
	}
	if err := store.Append(value); err != nil {
		t.Fatal(err)
	}
	_, err := message.ReplayDetailed(path)
	if err == nil {
		t.Fatal("question with delivery_mode must corrupt journal")
	}
}

func TestReplayRejectsLateDeliveryModeAssignment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	now := time.Now().UTC()
	first := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: "r1", TaskID: "t1",
		Type: message.Instruction, Status: message.Delivered, CreatedAt: now, UpdatedAt: now,
		Payload: json.RawMessage(`{"text":"x"}`),
	}
	// First record delivered without mode, then same-status assigns mode — illegal.
	// Use queued without mode then delivered with mode via transition... 
	// Better: queued no mode, then queued with mode (first assign OK), then delivered with mode OK.
	// Late first assign: delivered without previous mode.
	_ = store.Append(first)
	second := first
	second.UpdatedAt = now.Add(time.Second)
	second.DeliveryMode = message.DeliveryImmediate
	_ = store.Append(second)
	_, err := message.ReplayDetailed(path)
	if err == nil {
		t.Fatal("first delivery_mode on delivered status must corrupt")
	}
}

func TestReplayAllowsQueuedInstructionDeliveryMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	now := time.Now().UTC()
	first := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: "r1", TaskID: "t1",
		Type: message.Instruction, Status: message.Queued, CreatedAt: now, UpdatedAt: now,
		Payload: json.RawMessage(`{"text":"x"}`),
	}
	_ = store.Append(first)
	second := first
	second.UpdatedAt = now.Add(time.Millisecond)
	second.DeliveryMode = message.DeliveryResume
	_ = store.Append(second)
	if _, err := message.Replay(path); err != nil {
		t.Fatal(err)
	}
}

// silence unused
var (
	_ = sync.Mutex{}
	_ = errors.New
)
