package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/vnai/subagent-broker/internal/storage"
	"github.com/vnai/subagent-broker/internal/verify"
	"github.com/vnai/subagent-broker/internal/wave"
	workerpkg "github.com/vnai/subagent-broker/internal/worker"
)

// --- Patch 1: Barrier stale acceptance ---

func TestPR7BarrierStaleAcceptAfterWorkspaceChange(t *testing.T) {
	home := t.TempDir()
	projectRoot := t.TempDir()
	layout, err := storage.NewLayout(home)
	if err != nil {
		t.Fatal(err)
	}
	projectID, runID := "proj", "run-stale"
	runDir, err := layout.EnsureRun(projectID, runID)
	if err != nil {
		t.Fatal(err)
	}
	wavePaths, err := layout.WavePaths(projectID, runID, "wave-1")
	if err != nil {
		t.Fatal(err)
	}
	_ = os.MkdirAll(wavePaths.Root, 0o700)
	baseline, err := verify.CaptureWorkspace(projectRoot, home)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.AtomicWriteJSON(wavePaths.Baseline, baseline, 0o600); err != nil {
		t.Fatal(err)
	}
	runPaths, _ := layout.RunPaths(projectID, runID)
	service := &Service{
		config: Config{BrokerHome: home},
		runDir: runDir,
		paths:  runPaths,
		plan:   domain.RunPlan{Waves: []domain.WavePlan{{WaveID: "wave-1"}}},
		snapshot: Snapshot{
			Run:   domain.Run{RunID: domain.RunID(runID), ProjectID: domain.ProjectID(projectID)},
			Waves: []domain.Wave{{WaveID: "wave-1", Status: domain.WaveWaiting, BarrierResult: domain.BarrierPassedWithWarnings}},
			Tasks: []TaskState{{Task: domain.Task{TaskID: "seed", ProjectRoot: projectRoot, WaveID: "other"}}},
		},
		acceptingWork:     true,
		fatalPersistence:  make(chan error, 1),
		events:            &fakeEventAppender{},
		persistSnapshotFn: func(Snapshot) error { return nil },
	}
	input, err := service.collectBarrierInputs(context.Background(), service.plan.Waves[0], baseline)
	if err != nil {
		t.Fatal(err)
	}
	hash := hashBarrierInputs(input)
	eval := input
	eval.ExistingWarnings = []string{"risk"}
	verification := wave.EvaluateBarrier(eval, time.Now().UTC())
	verification.InputHash = hash
	raw, _ := json.Marshal(verification)
	_ = os.WriteFile(wavePaths.Verification, raw, 0o600)

	// Mutate workspace after barrier evaluation.
	if err := os.WriteFile(filepath.Join(projectRoot, "stale.txt"), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = service.AcceptBarrierWarnings("wave-1", "agent", "should fail")
	if err == nil || !containsAll(err.Error(), "stale_verification") {
		t.Fatalf("expected stale_verification, got %v", err)
	}
	if service.Snapshot().Waves[0].BarrierAccepted || service.Snapshot().Waves[0].Status == domain.WaveVerified {
		t.Fatal("stale accept must not advance wave")
	}
}

func TestPR7BarrierStaleAcceptAfterPendingQuestion(t *testing.T) {
	home := t.TempDir()
	projectRoot := t.TempDir()
	layout, _ := storage.NewLayout(home)
	projectID, runID := "proj", "run-pending"
	runDir, _ := layout.EnsureRun(projectID, runID)
	wavePaths, _ := layout.WavePaths(projectID, runID, "wave-1")
	_ = os.MkdirAll(wavePaths.Root, 0o700)
	baseline, _ := verify.CaptureWorkspace(projectRoot, home)
	_ = storage.AtomicWriteJSON(wavePaths.Baseline, baseline, 0o600)
	msgPath := filepath.Join(runDir, "messages.jsonl")
	store := message.NewStore(msgPath)
	router, _ := message.NewRouter(message.NewRouterOptions{RunID: runID, Store: store})
	runPaths, _ := layout.RunPaths(projectID, runID)
	service := &Service{
		config: Config{BrokerHome: home},
		runDir: runDir,
		paths:  runPaths,
		plan: domain.RunPlan{Waves: []domain.WavePlan{{
			WaveID: "wave-1", Tasks: []domain.Task{{TaskID: "task-a", ProjectRoot: projectRoot}},
		}}},
		snapshot: Snapshot{
			Run:   domain.Run{RunID: domain.RunID(runID), ProjectID: domain.ProjectID(projectID)},
			Waves: []domain.Wave{{WaveID: "wave-1", Status: domain.WaveWaiting, BarrierResult: domain.BarrierPassedWithWarnings}},
			Tasks: []TaskState{{
				Task: domain.Task{TaskID: "task-a", ProjectRoot: projectRoot, Status: state.TaskVerifiedSuccess, WaveID: "wave-1"},
			}},
		},
		router:            router,
		messages:          store,
		messageIndex:      map[string]message.Message{},
		acceptingWork:     true,
		fatalPersistence:  make(chan error, 1),
		events:            &fakeEventAppender{},
		persistSnapshotFn: func(Snapshot) error { return nil },
	}
	// Publish a valid current-attempt report so collection is stable before pending mutation.
	taskDir := filepath.Join(runPaths.Tasks, "task-a")
	_ = os.MkdirAll(taskDir, 0o700)
	env := report.Envelope{
		SchemaVersion: report.SchemaVersion, TaskID: "task-a", WorkerID: "w1",
		Status: report.StatusSucceeded, Summary: "done", WorkCompleted: []string{"done"},
		FilesChanged: []string{"a.go"}, Validation: []report.Validation{{Command: "true", Passed: true}},
	}
	service.snapshot.Tasks[0].Worker = &domain.WorkerSession{WorkerID: "w1", TaskID: "task-a", Attempt: 1}
	service.snapshot.Tasks[0].ActiveAttempt = 0
	service.snapshot.Tasks[0].Attempts = []workerpkg.Attempt{{
		Number: 1, Worker: domain.WorkerSession{WorkerID: "w1", TaskID: "task-a", Attempt: 1},
		Outcome: workerpkg.AttemptExited,
	}}
	if err := report.Publish(taskDir, env, 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	meta, _, err := report.VerifyDiskArtifacts(taskDir)
	if err != nil {
		t.Fatal(err)
	}
	service.snapshot.Tasks[0].ReportIdentity = &ReportIdentity{
		TaskID: meta.TaskID, WorkerID: meta.WorkerID, AttemptNumber: meta.AttemptNumber,
		EnvelopeHash: meta.EnvelopeHash, MarkdownHash: meta.MarkdownHash, PublishedAt: meta.PublishedAt,
	}
	service.snapshot.Tasks[0].ReportPath = filepath.Join(taskDir, "report.md")

	input, err := service.collectBarrierInputs(context.Background(), service.plan.Waves[0], baseline)
	if err != nil {
		t.Fatal(err)
	}
	// Force warnings result while hashing clean input may already have report issues —
	// re-hash after ensuring assessments pass.
	hash := hashBarrierInputs(input)
	eval := input
	eval.ExistingWarnings = []string{"w"}
	verification := wave.EvaluateBarrier(eval, time.Now().UTC())
	verification.InputHash = hash
	// If collection already failed, still test pending mutation path against that hash.
	raw, _ := json.Marshal(verification)
	_ = os.WriteFile(wavePaths.Verification, raw, 0o600)

	_, err = router.EnqueueDecision("task-a", "w1", message.Question, message.Decision, json.RawMessage(
		`{"schema_version":"v1alpha1","question":"q","reason":"r","current_scope":["a"],"workspace_state":"ok"}`,
	))
	if err != nil {
		t.Fatal(err)
	}
	err = service.AcceptBarrierWarnings("wave-1", "agent", "nope")
	if err == nil || !containsAll(err.Error(), "stale_verification") {
		t.Fatalf("expected stale after pending question, got %v", err)
	}
}

func TestPR7BarrierAcceptUnchangedSucceeds(t *testing.T) {
	home := t.TempDir()
	projectRoot := t.TempDir()
	layout, _ := storage.NewLayout(home)
	projectID, runID := "proj", "run-ok"
	runDir, _ := layout.EnsureRun(projectID, runID)
	wavePaths, _ := layout.WavePaths(projectID, runID, "wave-1")
	_ = os.MkdirAll(wavePaths.Root, 0o700)
	baseline, _ := verify.CaptureWorkspace(projectRoot, home)
	_ = storage.AtomicWriteJSON(wavePaths.Baseline, baseline, 0o600)
	runPaths, _ := layout.RunPaths(projectID, runID)
	service := &Service{
		config: Config{BrokerHome: home},
		runDir: runDir,
		paths:  runPaths,
		plan:   domain.RunPlan{Waves: []domain.WavePlan{{WaveID: "wave-1"}}},
		snapshot: Snapshot{
			Run:   domain.Run{RunID: domain.RunID(runID), ProjectID: domain.ProjectID(projectID)},
			Waves: []domain.Wave{{WaveID: "wave-1", Status: domain.WaveWaiting, BarrierResult: domain.BarrierPassedWithWarnings}},
			Tasks: []TaskState{{Task: domain.Task{TaskID: "seed", ProjectRoot: projectRoot}}},
		},
		acceptingWork:     true,
		fatalPersistence:  make(chan error, 1),
		events:            &fakeEventAppender{},
		persistSnapshotFn: func(Snapshot) error { return nil },
	}
	input, _ := service.collectBarrierInputs(context.Background(), service.plan.Waves[0], baseline)
	hash := hashBarrierInputs(input)
	eval := input
	eval.ExistingWarnings = []string{"w"}
	verification := wave.EvaluateBarrier(eval, time.Now().UTC())
	verification.InputHash = hash
	raw, _ := json.Marshal(verification)
	_ = os.WriteFile(wavePaths.Verification, raw, 0o600)
	if err := service.AcceptBarrierWarnings("wave-1", "agent", "reviewed"); err != nil {
		t.Fatal(err)
	}
	if !service.Snapshot().Waves[0].BarrierAccepted {
		t.Fatal("expected accept")
	}
}

// --- Patch 2: Cancel incomplete identity ---

type errTerminateAdapter struct {
	*fake.Adapter
}

func (a errTerminateAdapter) TerminateSession(context.Context, string) error {
	return errors.New("terminate failed")
}

func TestPR7CancelIncompleteIdentityNoTreeExited(t *testing.T) {
	inner := fake.New(adapter.Capabilities{})
	session, err := inner.StartSession(context.Background(), adapter.StartRequest{
		RunID: "r", TaskID: "t", WorkerID: "w", ProjectRoot: t.TempDir(), Contract: "c", Scenario: "active_steer",
	})
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{
		snapshot: Snapshot{
			Run: domain.Run{RunID: "r", Status: domain.RunRunning},
			Tasks: []TaskState{{
				Task:       domain.Task{TaskID: "t", Status: state.TaskRunning},
				Worker:     &domain.WorkerSession{WorkerID: "w", TaskID: "t", NativeSessionID: session.NativeSessionID},
				Dimensions: state.Dimensions{Process: state.ProcessAlive, Task: state.TaskRunning},
			}},
		},
		active: map[string]activeWorker{
			"t": {
				adapter:   errTerminateAdapter{Adapter: inner},
				sessionID: session.NativeSessionID,
				// Incomplete identity (no start token).
				identity: process.Identity{PID: 0},
				taskID:   "t",
				workerID: "w",
			},
		},
		acceptingWork:     true,
		fatalPersistence:  make(chan error, 1),
		events:            &fakeEventAppender{},
		persistSnapshotFn: func(Snapshot) error { return nil },
		cancelledTasks:    map[string]bool{},
	}
	result, err := service.terminateActiveWorker(context.Background(), service.active["t"])
	if result.TreeExited {
		t.Fatal("must not claim TreeExited with incomplete identity")
	}
	if !result.OrphanRisk {
		t.Fatal("expected OrphanRisk")
	}
	if err == nil {
		t.Fatal("expected terminate error propagated")
	}
}

func TestPR7CancelIncompleteIdentityNilStillUnconfirmed(t *testing.T) {
	inner := fake.New(adapter.Capabilities{})
	session, err := inner.StartSession(context.Background(), adapter.StartRequest{
		RunID: "r", TaskID: "t", WorkerID: "w", ProjectRoot: t.TempDir(), Contract: "c", Scenario: "active_steer",
	})
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{
		snapshot: Snapshot{
			Run: domain.Run{RunID: "r", Status: domain.RunRunning},
			Tasks: []TaskState{{
				Task:       domain.Task{TaskID: "t", Status: state.TaskRunning},
				Worker:     &domain.WorkerSession{WorkerID: "w", TaskID: "t"},
				Dimensions: state.Dimensions{Process: state.ProcessAlive},
			}},
		},
		active: map[string]activeWorker{
			"t": {
				adapter:   inner,
				sessionID: session.NativeSessionID,
				identity:  process.Identity{}, // incomplete
				taskID:    "t",
				workerID:  "w",
			},
		},
		acceptingWork:     true,
		fatalPersistence:  make(chan error, 1),
		events:            &fakeEventAppender{},
		persistSnapshotFn: func(Snapshot) error { return nil },
	}
	result, _ := service.terminateActiveWorker(context.Background(), service.active["t"])
	if result.TreeExited {
		t.Fatal("nil TerminateSession must not imply TreeExited")
	}
	if !result.OrphanRisk {
		t.Fatal("expected OrphanRisk")
	}
}

// --- Patch 3: Drain orphan ---

func TestPR7DrainUnconfirmedIsNotProcessExited(t *testing.T) {
	resolution := WorkerExitResolution{
		ExitObserved:      false,
		TreeExitConfirmed: false,
		OrphanRisk:        true,
		RemainingPIDs:     []int{4242},
		Errors:            []string{"kill failed"},
	}
	// Map logic mirrors runWorkerSession.
	processState := state.ProcessUnknown
	if len(resolution.RemainingPIDs) > 0 {
		processState = state.ProcessOrphaned
	}
	if processState == state.ProcessExited {
		t.Fatal("must not be exited")
	}
	if processState != state.ProcessOrphaned {
		t.Fatalf("got %s", processState)
	}

	service := newCommitService(&fakeEventAppender{}, func(Snapshot) error { return nil })
	service.snapshot.Tasks = []TaskState{{
		Task:       domain.Task{TaskID: "task-a", Status: state.TaskRunning},
		Worker:     &domain.WorkerSession{WorkerID: "w1", TaskID: "task-a"},
		Dimensions: state.Dimensions{Process: state.ProcessAlive, Task: state.TaskRunning},
	}}
	runtime := service.snapshot.Tasks[0]
	resolution.ProcessState = processState
	if err := service.commitWorkerExitResolution(&runtime, "w1", resolution, adapter.ExitStatus{Code: -1}); err != nil {
		t.Fatal(err)
	}
	got := service.Snapshot().Tasks[0].Dimensions.Process
	if got == state.ProcessExited {
		t.Fatal("must not forge ProcessExited")
	}
	if got != state.ProcessOrphaned {
		t.Fatalf("process=%s", got)
	}
}

// --- Patch 4: Message terminal replay ---

func TestPR7MessageTerminalReplayRejectsResurrection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	now := time.Now().UTC()
	base := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: "r1", TaskID: "t1",
		Type: message.Question, Status: message.Queued, CreatedAt: now, UpdatedAt: now,
		Payload: json.RawMessage(`{}`),
	}
	if err := store.Append(base); err != nil {
		t.Fatal(err)
	}
	answered := base
	answered.Status = message.Answered
	answered.UpdatedAt = now.Add(time.Second)
	answered.Resolution = json.RawMessage(`{"answer":"yes"}`)
	if err := store.Append(answered); err != nil {
		t.Fatal(err)
	}
	// Illegal resurrection.
	zombie := answered
	zombie.Status = message.Queued
	zombie.UpdatedAt = now.Add(2 * time.Second)
	if err := store.Append(zombie); err != nil {
		t.Fatal(err)
	}
	_, err := message.ReplayDetailed(path)
	if err == nil {
		t.Fatal("expected journal corruption on terminal resurrection")
	}
	var corrupt *message.ErrJournalCorrupt
	if !errors.As(err, &corrupt) {
		t.Fatalf("expected ErrJournalCorrupt, got %T %v", err, err)
	}
}

func TestPR7MessageReplayRejectsUpdatedAtAndAttemptsRegression(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	now := time.Now().UTC()
	first := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: "r1", TaskID: "t1",
		Type: message.Instruction, Status: message.Queued, CreatedAt: now, UpdatedAt: now.Add(time.Second),
		DeliveryAttempts: 2, Payload: json.RawMessage(`{}`),
	}
	second := first
	second.UpdatedAt = now // backwards
	writeMessagesRaw(t, path, first, second)
	if _, err := message.Replay(path); err == nil {
		t.Fatal("expected UpdatedAt regression failure")
	}

	path2 := filepath.Join(t.TempDir(), "messages2.jsonl")
	second = first
	second.UpdatedAt = now.Add(2 * time.Second)
	second.DeliveryAttempts = 1 // backwards
	writeMessagesRaw(t, path2, first, second)
	if _, err := message.Replay(path2); err == nil {
		t.Fatal("expected DeliveryAttempts regression failure")
	}
}

func TestPR7MessageReplayAllowsIdempotentSameStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	now := time.Now().UTC()
	first := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: "r1", TaskID: "t1",
		Type: message.Instruction, Status: message.Queued, CreatedAt: now, UpdatedAt: now,
		Payload: json.RawMessage(`{}`),
	}
	second := first
	second.UpdatedAt = now.Add(time.Millisecond)
	writeMessagesRaw(t, path, first, second)
	replayed, err := message.Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if replayed["m1"].Status != message.Queued {
		t.Fatalf("%+v", replayed["m1"])
	}
}

// --- Patch 5: Message Commit failure fail-closed ---

func TestPR7MessageCommitFailureFailClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	persistCalls := 0
	service := &Service{
		snapshot: Snapshot{
			Run: domain.Run{RunID: "run-1"},
			Tasks: []TaskState{{
				Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning},
			}},
		},
		messages:     store,
		messageIndex: map[string]message.Message{},
		router:       router,
		events:       &fakeEventAppender{},
		persistSnapshotFn: func(Snapshot) error {
			persistCalls++
			if persistCalls == 1 {
				return errors.New("snapshot disk full")
			}
			return nil
		},
		acceptingWork:    true,
		fatalPersistence: make(chan error, 1),
	}
	queued, err := router.EnqueueInstruction("task-a", "w1", "hello", message.DeliveryNextTurn)
	if err != nil {
		t.Fatal(err)
	}
	// Router succeeded; Commit must fail-closed.
	err = service.CommitMessageProjection(context.Background(), queued, event.MessageQueued)
	if err == nil {
		t.Fatal("expected commit failure")
	}
	if service.AcceptingWork() {
		t.Fatal("expected fail-closed")
	}
	// Snapshot messages must not have been installed from a successful commit.
	if len(service.Snapshot().Messages) != 0 {
		// After failed snapshot stage, memory keeps previous empty snapshot.
		t.Fatalf("snapshot messages=%+v", service.Snapshot().Messages)
	}
}

// --- Patch 6: Stale report attempt ---

func TestPR7StaleReportAttemptRejected(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "tasks", "task-a")
	_ = os.MkdirAll(taskDir, 0o700)
	env := report.Envelope{
		SchemaVersion: report.SchemaVersion, TaskID: "task-a", WorkerID: "worker-1",
		Status: report.StatusSucceeded, Summary: "done", WorkCompleted: []string{"done"},
		FilesChanged: []string{"a.go"}, Validation: []report.Validation{{Command: "true", Passed: true}},
	}
	// Publish as attempt 1.
	if err := report.Publish(taskDir, env, 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	meta, _, err := report.VerifyDiskArtifacts(taskDir)
	if err != nil {
		t.Fatal(err)
	}
	frozen := ReportIdentity{
		TaskID: meta.TaskID, WorkerID: meta.WorkerID, AttemptNumber: meta.AttemptNumber,
		EnvelopeHash: meta.EnvelopeHash, MarkdownHash: meta.MarkdownHash, PublishedAt: meta.PublishedAt,
	}
	// Tamper markdown while keeping status string.
	md, _ := os.ReadFile(filepath.Join(taskDir, "report.md"))
	_ = os.WriteFile(filepath.Join(taskDir, "report.md"), append(md, []byte("\n// tampered\n")...), 0o600)

	service := &Service{
		paths: storage.RunPaths{Root: root, Tasks: filepath.Join(root, "tasks")},
		snapshot: Snapshot{Tasks: []TaskState{{
			Task:           domain.Task{TaskID: "task-a", Status: state.TaskVerifiedSuccess, ProjectRoot: root},
			ActiveAttempt:  0,
			ReportIdentity: &frozen,
			Attempts: []workerpkg.Attempt{{
				Number: 1, Worker: domain.WorkerSession{WorkerID: "worker-1", TaskID: "task-a", Attempt: 1},
				Outcome: workerpkg.AttemptExited,
			}},
			Worker: &domain.WorkerSession{WorkerID: "worker-1", TaskID: "task-a", Attempt: 1},
		}}},
	}
	assessment := service.assessTaskReport(service.snapshot.Tasks[0])
	if assessment.MarkdownValid || assessment.Error == "" {
		t.Fatalf("tampered markdown must fail: %+v", assessment)
	}

	// Restore valid report for attempt 1, then start attempt 2 which marks identity stale.
	if err := report.Publish(taskDir, env, 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	meta, _, _ = report.VerifyDiskArtifacts(taskDir)
	frozen = ReportIdentity{
		TaskID: meta.TaskID, WorkerID: meta.WorkerID, AttemptNumber: meta.AttemptNumber,
		EnvelopeHash: meta.EnvelopeHash, MarkdownHash: meta.MarkdownHash, PublishedAt: meta.PublishedAt,
		Stale: true, // newer attempt began
	}
	service.snapshot.Tasks[0].ReportIdentity = &frozen
	service.snapshot.Tasks[0].ActiveAttempt = 2
	service.snapshot.Tasks[0].Attempts = append(service.snapshot.Tasks[0].Attempts, workerpkg.Attempt{
		Number: 2, Worker: domain.WorkerSession{WorkerID: "worker-2", TaskID: "task-a", Attempt: 2},
		Outcome: workerpkg.AttemptExited,
	})
	service.snapshot.Tasks[0].Worker = &domain.WorkerSession{WorkerID: "worker-2", TaskID: "task-a", Attempt: 2}
	assessment = service.assessTaskReport(service.snapshot.Tasks[0])
	if assessment.IdentityValid {
		t.Fatalf("stale attempt-1 report must not pass after attempt-2: %+v", assessment)
	}
}

func TestPR7CurrentAttemptReportPasses(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "tasks", "task-a")
	_ = os.MkdirAll(taskDir, 0o700)
	env := report.Envelope{
		SchemaVersion: report.SchemaVersion, TaskID: "task-a", WorkerID: "worker-2",
		Status: report.StatusSucceeded, Summary: "done", WorkCompleted: []string{"done"},
		FilesChanged: []string{"a.go"}, Validation: []report.Validation{{Command: "true", Passed: true}},
	}
	if err := report.Publish(taskDir, env, 2, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	meta, _, err := report.VerifyDiskArtifacts(taskDir)
	if err != nil {
		t.Fatal(err)
	}
	// ActiveAttempt cleared after finish — frozen ReportIdentity still accepts the report.
	service := &Service{
		paths: storage.RunPaths{Root: root, Tasks: filepath.Join(root, "tasks")},
		snapshot: Snapshot{Tasks: []TaskState{{
			Task:          domain.Task{TaskID: "task-a", Status: state.TaskVerifiedSuccess, ProjectRoot: root},
			ActiveAttempt: 0,
			ReportIdentity: &ReportIdentity{
				TaskID: meta.TaskID, WorkerID: meta.WorkerID, AttemptNumber: meta.AttemptNumber,
				EnvelopeHash: meta.EnvelopeHash, MarkdownHash: meta.MarkdownHash, PublishedAt: meta.PublishedAt,
			},
			Attempts: []workerpkg.Attempt{
				{Number: 1, Worker: domain.WorkerSession{WorkerID: "worker-1", TaskID: "task-a", Attempt: 1}, Outcome: workerpkg.AttemptExited},
				{Number: 2, Worker: domain.WorkerSession{WorkerID: "worker-2", TaskID: "task-a", Attempt: 2}, Outcome: workerpkg.AttemptExited},
			},
			Worker: &domain.WorkerSession{WorkerID: "worker-2", TaskID: "task-a", Attempt: 2},
		}}},
	}
	assessment := service.assessTaskReport(service.snapshot.Tasks[0])
	if !assessment.Present || !assessment.MetaValid || !assessment.MarkdownValid || !assessment.IdentityValid {
		t.Fatalf("expected pass: %+v", assessment)
	}
	if assessment.Error != "" {
		t.Fatalf("unexpected error: %s", assessment.Error)
	}
}

// --- Patch 7: Active resume outbox ---

func TestPR7ActiveResumeCreatesAttemptAndDelivers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	harness := fake.New(adapter.Capabilities{
		ResumeSession: true, StructuredStream: true, BidirectionalStream: true, SteerActiveTurn: true,
	})
	projectRoot := t.TempDir()
	// Prior session with final result so full resume lifecycle can CollectFinalResult.
	prior, err := harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-1", TaskID: "task-a", WorkerID: "worker-old",
		ProjectRoot: projectRoot, Contract: "contract", Scenario: "normal_stream",
	})
	if err != nil {
		t.Fatal(err)
	}
	for range prior.Events {
	}
	if prior.Exited != nil {
		<-prior.Exited
	}
	registry := adapter.NewRegistry()
	_ = registry.Register(harness)
	home := t.TempDir()
	layout, _ := storage.NewLayout(home)
	runDir, _ := layout.EnsureRun("p1", "run-1")
	runPaths, _ := layout.RunPaths("p1", "run-1")
	_ = os.MkdirAll(filepath.Join(runPaths.Tasks, "task-a"), 0o700)
	service := &Service{
		config: func() Config {
			c := Config{Harness: string(adapter.HarnessFake), MaxTurns: 4, BrokerHome: home, CancelGrace: 200 * time.Millisecond}
			c.Normalize()
			return c
		}(),
		registry: registry,
		runDir:   runDir,
		paths:    runPaths,
		snapshot: Snapshot{
			SchemaVersion: SchemaVersion,
			Run:           domain.Run{RunID: "run-1", ProjectID: "p1", Status: domain.RunRunning},
			Tasks: []TaskState{{
				Task: domain.Task{
					TaskID: "task-a", Status: state.TaskRunning, ProjectRoot: projectRoot,
					Title: "t", Objective: "o", CompletionCriteria: []string{"c"},
					WriteScope: []string{"a/**"}, ValidationCommands: []domain.ValidationCommand{{Command: "true"}},
				},
				Worker: &domain.WorkerSession{
					WorkerID: "worker-old", TaskID: "task-a", Attempt: 1,
					NativeSessionID: prior.NativeSessionID,
					Capabilities:    adapter.CapabilityMap(adapter.Capabilities{ResumeSession: true, SteerActiveTurn: true, BidirectionalStream: true, StructuredStream: true}),
				},
				Attempts: []workerpkg.Attempt{{
					Number: 1, Mode: workerpkg.AttemptFresh,
					Worker: domain.WorkerSession{
						WorkerID: "worker-old", TaskID: "task-a", Attempt: 1,
						NativeSessionID: prior.NativeSessionID,
						Capabilities:    adapter.CapabilityMap(adapter.Capabilities{ResumeSession: true, SteerActiveTurn: true}),
					},
					Outcome: workerpkg.AttemptExited,
				}},
				ActiveAttempt: 0,
				Dimensions:    state.Dimensions{Process: state.ProcessExited, Task: state.TaskRunning},
			}},
		},
		messages:          store,
		messageIndex:      map[string]message.Message{},
		router:            router,
		active:            map[string]activeWorker{},
		acceptingWork:     true,
		fatalPersistence:  make(chan error, 1),
		events:            &fakeEventAppender{},
		persistSnapshotFn: func(Snapshot) error { return nil },
	}

	result, err := service.SendInstruction(context.Background(), "task-a", "resume please")
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID == "" {
		t.Fatal("expected message id")
	}
	snap := service.Snapshot()
	if len(snap.Tasks[0].Attempts) < 2 {
		t.Fatalf("expected recovery_resume attempt, attempts=%+v", snap.Tasks[0].Attempts)
	}
	last := snap.Tasks[0].Attempts[len(snap.Tasks[0].Attempts)-1]
	if last.Mode != workerpkg.AttemptRecoveryResume {
		t.Fatalf("mode=%s", last.Mode)
	}
	if last.Outcome == workerpkg.AttemptRunning {
		t.Fatal("attempt must finish after full session driver")
	}
	got, _ := router.Get(result.MessageID)
	if got.Status != message.Delivered {
		t.Fatalf("expected delivered, got %+v", got)
	}
	service.mu.Lock()
	_, ok := service.active["task-a"]
	service.mu.Unlock()
	if ok {
		t.Fatal("active worker must unregister after resume lifecycle")
	}
}

func TestPR7ResumeNoNativeSessionFailsUnsupported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, _ := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	harness := fake.New(adapter.Capabilities{ResumeSession: true, StructuredStream: true})
	registry := adapter.NewRegistry()
	_ = registry.Register(harness)
	service := &Service{
		config:   Config{Harness: string(adapter.HarnessFake)},
		registry: registry,
		snapshot: Snapshot{
			Run: domain.Run{RunID: "run-1"},
			Tasks: []TaskState{{
				Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning},
				Worker: &domain.WorkerSession{
					WorkerID: "w1", TaskID: "task-a", Attempt: 1,
					// No NativeSessionID
					Capabilities: adapter.CapabilityMap(adapter.Capabilities{ResumeSession: true}),
				},
				Attempts: []workerpkg.Attempt{{
					Number: 1, Worker: domain.WorkerSession{WorkerID: "w1", TaskID: "task-a"},
					Outcome: workerpkg.AttemptExited,
				}},
				Dimensions: state.Dimensions{Process: state.ProcessExited, Task: state.TaskRunning},
			}},
		},
		messages:          store,
		messageIndex:      map[string]message.Message{},
		router:            router,
		active:            map[string]activeWorker{},
		acceptingWork:     true,
		fatalPersistence:  make(chan error, 1),
		events:            &fakeEventAppender{},
		persistSnapshotFn: func(Snapshot) error { return nil },
	}
	_, err := service.SendInstruction(context.Background(), "task-a", "no session")
	if err == nil {
		t.Fatal("expected failure")
	}
}

func TestPR7ConcurrentResumeSingleAttempt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, _ := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	harness := fake.New(adapter.Capabilities{
		ResumeSession: true, StructuredStream: true, SteerActiveTurn: true, BidirectionalStream: true,
	})
	projectRoot := t.TempDir()
	prior, err := harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-1", TaskID: "task-a", WorkerID: "worker-old",
		ProjectRoot: projectRoot, Contract: "c", Scenario: "normal_stream",
	})
	if err != nil {
		t.Fatal(err)
	}
	for range prior.Events {
	}
	if prior.Exited != nil {
		<-prior.Exited
	}
	registry := adapter.NewRegistry()
	_ = registry.Register(harness)
	home := t.TempDir()
	layout, _ := storage.NewLayout(home)
	runDir, _ := layout.EnsureRun("p", "run-1")
	runPaths, _ := layout.RunPaths("p", "run-1")
	_ = os.MkdirAll(filepath.Join(runPaths.Tasks, "task-a"), 0o700)
	service := &Service{
		config: func() Config {
			c := Config{Harness: string(adapter.HarnessFake), MaxTurns: 2, BrokerHome: home, CancelGrace: 200 * time.Millisecond}
			c.Normalize()
			return c
		}(),
		registry: registry,
		runDir:   runDir,
		paths:    runPaths,
		snapshot: Snapshot{
			Run: domain.Run{RunID: "run-1", ProjectID: "p"},
			Tasks: []TaskState{{
				Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning, ProjectRoot: projectRoot, Title: "t", Objective: "o", CompletionCriteria: []string{"c"}, WriteScope: []string{"a/**"}, ValidationCommands: []domain.ValidationCommand{{Command: "true"}}},
				Worker: &domain.WorkerSession{
					WorkerID: "worker-old", TaskID: "task-a", Attempt: 1, NativeSessionID: prior.NativeSessionID,
					Capabilities: adapter.CapabilityMap(adapter.Capabilities{ResumeSession: true, SteerActiveTurn: true}),
				},
				Attempts: []workerpkg.Attempt{{
					Number: 1, Mode: workerpkg.AttemptFresh,
					Worker:  domain.WorkerSession{WorkerID: "worker-old", TaskID: "task-a", Attempt: 1, NativeSessionID: prior.NativeSessionID},
					Outcome: workerpkg.AttemptExited,
				}},
				Dimensions: state.Dimensions{Process: state.ProcessExited, Task: state.TaskRunning},
			}},
		},
		messages:          store,
		messageIndex:      map[string]message.Message{},
		router:            router,
		active:            map[string]activeWorker{},
		acceptingWork:     true,
		fatalPersistence:  make(chan error, 1),
		events:            &fakeEventAppender{},
		persistSnapshotFn: func(Snapshot) error { return nil },
	}

	// Enqueue two resume instructions first.
	_, _ = router.EnqueueInstructionWithAttempt("task-a", "worker-old", 1, "one", message.DeliveryResume)
	_, _ = router.EnqueueInstructionWithAttempt("task-a", "worker-old", 1, "two", message.DeliveryResume)

	var wg sync.WaitGroup
	var success atomic.Int32
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := service.EnsureResumeAndFlushOutbox(context.Background(), "task-a"); err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	if success.Load() < 1 {
		t.Fatal("expected at least one successful resume")
	}
	// Only one new attempt beyond the original.
	attempts := service.Snapshot().Tasks[0].Attempts
	resumeCount := 0
	for _, a := range attempts {
		if a.Mode == workerpkg.AttemptRecoveryResume {
			resumeCount++
		}
	}
	if resumeCount != 1 {
		t.Fatalf("expected exactly one recovery_resume attempt, got %d (attempts=%+v)", resumeCount, attempts)
	}
}

func writeMessagesRaw(t *testing.T, path string, values ...message.Message) {
	t.Helper()
	store := message.NewStore(path)
	for _, value := range values {
		if err := store.Append(value); err != nil {
			t.Fatal(err)
		}
	}
}

func containsAll(s, sub string) bool {
	return strings.Contains(s, sub)
}
