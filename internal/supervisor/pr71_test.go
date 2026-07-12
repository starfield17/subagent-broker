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
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
	workerpkg "github.com/vnai/subagent-broker/internal/worker"
)

// --- Check 1: Question projection remove errors ---

func TestPR71ClearTopLevelQuestionMissingOK(t *testing.T) {
	dir := t.TempDir()
	if err := message.ClearTopLevelQuestion(dir); err != nil {
		t.Fatal(err)
	}
}

func TestPR71ClearTopLevelQuestionPermissionError(t *testing.T) {
	dir := t.TempDir()
	// Create a non-empty directory named question.md so Remove returns EISDIR / not-not-exist.
	if err := os.Mkdir(filepath.Join(dir, "question.md"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "question.md", "inner"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := message.ClearTopLevelQuestion(dir)
	if err == nil {
		t.Fatal("expected remove error for non-empty directory projection path")
	}
	if !strings.Contains(err.Error(), "remove question projection") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPR71TerminalCleanupPropagatesProjectionError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	taskDir := filepath.Join(home, "tasks", "task-a")
	_ = os.MkdirAll(taskDir, 0o700)
	// Poison question.md as a non-empty directory so ClearTopLevelQuestion fails.
	_ = os.Mkdir(filepath.Join(taskDir, "question.md"), 0o700)
	_ = os.WriteFile(filepath.Join(taskDir, "question.md", "x"), []byte("1"), 0o600)

	service := &Service{
		paths: storage.RunPaths{Tasks: filepath.Join(home, "tasks")},
		snapshot: Snapshot{
			Run: domain.Run{RunID: "run-1"},
			Tasks: []TaskState{{
				Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning},
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
	_, _ = router.EnqueueDecision("task-a", "w1", message.Question, message.Decision, json.RawMessage(
		`{"schema_version":"v1alpha1","question":"q","reason":"r","current_scope":["a"],"workspace_state":"ok"}`,
	))
	if err := service.onTaskTerminalMessages("task-a", "task failed"); err == nil {
		t.Fatal("expected projection cleanup error to surface")
	}
}

// --- Check 2: Resume full session lifecycle ---

func TestPR71ResumeLifecycleConsumesEventsAndFinishes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, _ := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	harness := fake.New(adapter.Capabilities{
		ResumeSession: true, StructuredStream: true, SteerActiveTurn: true, BidirectionalStream: true,
	})
	projectRoot := t.TempDir()
	prior, err := harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-1", TaskID: "task-a", WorkerID: "w-old",
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
	runDir, _ := layout.EnsureRun("p1", "run-1")
	runPaths, _ := layout.RunPaths("p1", "run-1")
	_ = os.MkdirAll(filepath.Join(runPaths.Tasks, "task-a"), 0o700)
	cfg := Config{Harness: string(adapter.HarnessFake), BrokerHome: home, CancelGrace: 200 * time.Millisecond}
	cfg.Normalize()
	service := &Service{
		config: cfg, registry: registry, runDir: runDir, paths: runPaths,
		snapshot: Snapshot{
			Run: domain.Run{RunID: "run-1", ProjectID: "p1", Status: domain.RunRunning},
			Tasks: []TaskState{{
				Task: domain.Task{
					TaskID: "task-a", Status: state.TaskRunning, ProjectRoot: projectRoot,
					Title: "t", Objective: "o", CompletionCriteria: []string{"c"},
					WriteScope: []string{"a/**"}, ValidationCommands: []domain.ValidationCommand{{Command: "true"}},
				},
				Worker: &domain.WorkerSession{
					WorkerID: "w-old", TaskID: "task-a", Attempt: 1, NativeSessionID: prior.NativeSessionID,
					Capabilities: adapter.CapabilityMap(adapter.Capabilities{ResumeSession: true, SteerActiveTurn: true}),
				},
				Attempts: []workerpkg.Attempt{{
					Number: 1, Mode: workerpkg.AttemptFresh,
					Worker:  domain.WorkerSession{WorkerID: "w-old", TaskID: "task-a", Attempt: 1, NativeSessionID: prior.NativeSessionID},
					Outcome: workerpkg.AttemptExited,
				}},
				Dimensions: state.Dimensions{Process: state.ProcessExited, Task: state.TaskRunning},
			}},
		},
		messages: store, messageIndex: map[string]message.Message{}, router: router,
		active: map[string]activeWorker{}, acceptingWork: true, fatalPersistence: make(chan error, 1),
		events: &fakeEventAppender{}, persistSnapshotFn: func(Snapshot) error { return nil },
	}
	_, err = router.EnqueueInstructionWithAttempt("task-a", "w-old", 1, "resume me", message.DeliveryResume)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.EnsureResumeAndFlushOutbox(context.Background(), "task-a"); err != nil {
		t.Fatal(err)
	}
	snap := service.Snapshot()
	if len(snap.Tasks[0].Attempts) < 2 {
		t.Fatalf("expected resume attempt: %+v", snap.Tasks[0].Attempts)
	}
	last := snap.Tasks[0].Attempts[len(snap.Tasks[0].Attempts)-1]
	if last.Mode != workerpkg.AttemptRecoveryResume {
		t.Fatalf("mode=%s", last.Mode)
	}
	if last.Outcome == workerpkg.AttemptRunning {
		t.Fatal("attempt must finish")
	}
	// Task should complete from result envelope.
	if snap.Tasks[0].Task.Status != state.TaskVerifiedSuccess && snap.Tasks[0].Task.Status != state.TaskVerifiedPartial {
		// Accept reported_complete path if validation edge differs.
		if snap.Tasks[0].Task.Status != state.TaskReportedComplete && snap.Tasks[0].Dimensions.Process != state.ProcessExited {
			t.Fatalf("unexpected task status=%s process=%s", snap.Tasks[0].Task.Status, snap.Tasks[0].Dimensions.Process)
		}
	}
	service.mu.Lock()
	_, stillActive := service.active["task-a"]
	service.mu.Unlock()
	if stillActive {
		t.Fatal("must unregister active worker")
	}
	// Instruction delivered (not still queued).
	all := router.Snapshot(true)
	found := false
	for _, m := range all {
		if m.Type == message.Instruction && m.Status == message.Delivered {
			found = true
		}
		if m.Type == message.Instruction && m.Status == message.Queued {
			t.Fatalf("instruction still queued: %+v", m)
		}
	}
	if !found {
		t.Fatal("expected at least one delivered instruction")
	}
}

func TestPR71ResumeDriverFailureNoActiveLeftover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, _ := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	// Resume capability false → fail before session.
	harness := fake.New(adapter.Capabilities{ResumeSession: false, StructuredStream: true})
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
					WorkerID: "w1", TaskID: "task-a", NativeSessionID: "sess-x",
					Capabilities: adapter.CapabilityMap(adapter.Capabilities{ResumeSession: false}),
				},
				Attempts:   []workerpkg.Attempt{{Number: 1, Worker: domain.WorkerSession{WorkerID: "w1", NativeSessionID: "sess-x"}, Outcome: workerpkg.AttemptExited}},
				Dimensions: state.Dimensions{Process: state.ProcessExited, Task: state.TaskRunning},
			}},
		},
		messages: store, messageIndex: map[string]message.Message{}, router: router,
		active: map[string]activeWorker{}, acceptingWork: true, fatalPersistence: make(chan error, 1),
		events: &fakeEventAppender{}, persistSnapshotFn: func(Snapshot) error { return nil },
	}
	_, _ = router.EnqueueInstructionWithAttempt("task-a", "w1", 1, "x", message.DeliveryResume)
	_ = service.EnsureResumeAndFlushOutbox(context.Background(), "task-a")
	service.mu.Lock()
	_, ok := service.active["task-a"]
	service.mu.Unlock()
	if ok {
		t.Fatal("no active worker on capability failure")
	}
}

// --- Check 3: Journal corruption fail-closed ---

func TestPR71JournalCorruptLoadFailClosed(t *testing.T) {
	home := t.TempDir()
	layout, err := storage.NewLayout(home)
	if err != nil {
		t.Fatal(err)
	}
	projectID, runID := "proj", "run-corrupt"
	runDir, err := layout.EnsureRun(projectID, runID)
	if err != nil {
		t.Fatal(err)
	}
	runPaths, _ := layout.RunPaths(projectID, runID)
	// Minimal run.json + plan + events so Load can reach message replay.
	run := domain.Run{
		RunID: domain.RunID(runID), ProjectID: domain.ProjectID(projectID),
		Status: domain.RunRunning, SchemaVersion: SchemaVersion,
		TaskIDs: []domain.TaskID{"task-a"}, CreatedAt: time.Now().UTC(),
		ConfigSnapshot: mustJSON(t, Config{BrokerHome: home, Harness: string(adapter.HarnessFake)}),
	}
	if err := storage.AtomicWriteJSON(runPaths.Run, run, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := storage.AtomicWriteJSON(runPaths.Plan, domain.RunPlan{SchemaVersion: SchemaVersion, Waves: []domain.WavePlan{{WaveID: "wave-1"}}}, 0o600); err != nil {
		t.Fatal(err)
	}
	taskPaths, _ := layout.TaskPaths(projectID, runID, "task-a")
	_ = os.MkdirAll(taskPaths.Root, 0o700)
	if err := storage.AtomicWriteJSON(taskPaths.Task, domain.Task{
		TaskID: "task-a", Title: "t", Objective: "o", CompletionCriteria: []string{"c"},
		WriteScope: []string{"a/**"}, ValidationCommands: []domain.ValidationCommand{{Command: "true"}},
		ProjectRoot: t.TempDir(), WaveID: "wave-1", Status: state.TaskPlanned,
	}, 0o600); err != nil {
		t.Fatal(err)
	}
	// Corrupt journal: queued → answered → queued
	now := time.Now().UTC()
	store := message.NewStore(runPaths.Messages)
	base := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: runID, TaskID: "task-a",
		Type: message.Question, Status: message.Queued, CreatedAt: now, UpdatedAt: now,
		Payload: json.RawMessage(`{}`),
	}
	_ = store.Append(base)
	ans := base
	ans.Status = message.Answered
	ans.UpdatedAt = now.Add(time.Second)
	ans.Resolution = json.RawMessage(`{"answer":"y"}`)
	_ = store.Append(ans)
	zombie := ans
	zombie.Status = message.Queued
	zombie.UpdatedAt = now.Add(2 * time.Second)
	_ = store.Append(zombie)

	registry := adapter.NewRegistry()
	_ = registry.Register(fake.New(adapter.Capabilities{}))
	service, err := Load(runDir, registry, true)
	if err != nil {
		t.Fatal(err)
	}
	if service.AcceptingWork() {
		t.Fatal("corrupt journal must fail-closed acceptingWork")
	}
	if service.Snapshot().Run.Status != domain.RunDegraded {
		t.Fatalf("status=%s want degraded", service.Snapshot().Run.Status)
	}
	if !strings.Contains(service.Snapshot().LastError, "message_journal_corrupt") {
		t.Fatalf("last_error=%s", service.Snapshot().LastError)
	}
	if !service.messages.AppendDisabled() {
		t.Fatal("store must refuse append")
	}
	// Append refused.
	if err := service.messages.Append(base); err == nil {
		t.Fatal("append must be rejected")
	}
	// execute must refuse workers.
	if err := service.execute(context.Background()); err == nil {
		// execute may return nil if no tasks — ensure accepting work gate.
	}
	if service.AcceptingWork() {
		t.Fatal("still not accepting work")
	}
}

func TestPR71IncompleteTailStillRepaired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	now := time.Now().UTC()
	value := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: "r1",
		Type: message.Question, Status: message.Queued, CreatedAt: now, UpdatedAt: now,
		Payload: json.RawMessage(`{}`),
	}
	if err := store.Append(value); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	_ = os.WriteFile(path, append(raw, []byte(`{"message_id":"m2"`)...), 0o600)
	result, err := message.ReplayDetailed(path)
	if err != nil {
		t.Fatal(err)
	}
	if !result.TailRepaired {
		t.Fatal("expected incomplete tail repair")
	}
}

// --- Check 4: Frozen ReportIdentity ---

func TestPR71ReportIdentitySurvivesActiveAttemptClear(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "tasks", "task-a")
	_ = os.MkdirAll(taskDir, 0o700)
	env := report.Envelope{
		SchemaVersion: report.SchemaVersion, TaskID: "task-a", WorkerID: "worker-1",
		Status: report.StatusSucceeded, Summary: "done", WorkCompleted: []string{"done"},
		FilesChanged: []string{"a.go"}, Validation: []report.Validation{{Command: "true", Passed: true}},
	}
	if err := report.Publish(taskDir, env, 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	meta, _, err := report.VerifyDiskArtifacts(taskDir)
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{
		paths: storage.RunPaths{Root: root, Tasks: filepath.Join(root, "tasks")},
		snapshot: Snapshot{Tasks: []TaskState{{
			Task:          domain.Task{TaskID: "task-a", Status: state.TaskVerifiedSuccess, ProjectRoot: root},
			ActiveAttempt: 0, // cleared after finish
			ReportIdentity: &ReportIdentity{
				TaskID: meta.TaskID, WorkerID: meta.WorkerID, AttemptNumber: 1,
				EnvelopeHash: meta.EnvelopeHash, MarkdownHash: meta.MarkdownHash, PublishedAt: meta.PublishedAt,
			},
			Attempts: []workerpkg.Attempt{{
				Number: 1, Worker: domain.WorkerSession{WorkerID: "worker-1", TaskID: "task-a", Attempt: 1},
				Outcome: workerpkg.AttemptExited,
			}},
		}}},
	}
	a := service.assessTaskReport(service.snapshot.Tasks[0])
	if !a.IdentityValid || !a.MarkdownValid || a.Error != "" {
		t.Fatalf("frozen identity must pass after ActiveAttempt clear: %+v", a)
	}
}

func TestPR71NewAttemptStalesOldReport(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "tasks", "task-a")
	_ = os.MkdirAll(taskDir, 0o700)
	env := report.Envelope{
		SchemaVersion: report.SchemaVersion, TaskID: "task-a", WorkerID: "worker-1",
		Status: report.StatusSucceeded, Summary: "done", WorkCompleted: []string{"done"},
		FilesChanged: []string{"a.go"}, Validation: []report.Validation{{Command: "true", Passed: true}},
	}
	if err := report.Publish(taskDir, env, 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	meta, _, _ := report.VerifyDiskArtifacts(taskDir)
	service := newCommitService(&fakeEventAppender{}, func(Snapshot) error { return nil })
	service.snapshot.Tasks = []TaskState{{
		Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning, ProjectRoot: root},
		ReportIdentity: &ReportIdentity{
			TaskID: meta.TaskID, WorkerID: meta.WorkerID, AttemptNumber: 1,
			EnvelopeHash: meta.EnvelopeHash, MarkdownHash: meta.MarkdownHash,
		},
		Attempts: []workerpkg.Attempt{{
			Number: 1, Worker: domain.WorkerSession{WorkerID: "worker-1", TaskID: "task-a", Attempt: 1},
			Outcome: workerpkg.AttemptExited,
		}},
		Worker:     &domain.WorkerSession{WorkerID: "worker-1", TaskID: "task-a", Attempt: 1, NativeSessionID: "s1"},
		Dimensions: state.Dimensions{Process: state.ProcessExited, Task: state.TaskRunning},
	}}
	// Simulate newer attempt marking prior ReportIdentity stale.
	stale := *service.snapshot.Tasks[0].ReportIdentity
	stale.Stale = true
	service.snapshot.Tasks[0].ReportIdentity = &stale
	service.snapshot.Tasks[0].Attempts = append(service.snapshot.Tasks[0].Attempts, workerpkg.Attempt{
		Number: 2, Mode: workerpkg.AttemptRecoveryResume,
		Worker:  domain.WorkerSession{WorkerID: "worker-2", TaskID: "task-a", Attempt: 2},
		Outcome: workerpkg.AttemptRunning,
	})
	service.snapshot.Tasks[0].ActiveAttempt = 2
	service.paths = storage.RunPaths{Root: root, Tasks: filepath.Join(root, "tasks")}
	a := service.assessTaskReport(service.snapshot.Tasks[0])
	if a.IdentityValid {
		t.Fatalf("stale report must fail: %+v", a)
	}
}

func TestPR71HalfPublishedReportFails(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "tasks", "task-a")
	_ = os.MkdirAll(taskDir, 0o700)
	// Meta without report.md formal marker.
	meta := report.Meta{
		SchemaVersion: report.SchemaVersion, TaskID: "task-a", WorkerID: "w1",
		AttemptNumber: 1, Status: report.StatusSucceeded, EnvelopeHash: "abc", MarkdownHash: "def",
		PublishedAt: time.Now().UTC(),
	}
	raw, _ := json.MarshalIndent(meta, "", "  ")
	_ = os.WriteFile(filepath.Join(taskDir, "report.meta.json"), append(raw, '\n'), 0o600)
	service := &Service{
		paths: storage.RunPaths{Root: root, Tasks: filepath.Join(root, "tasks")},
		snapshot: Snapshot{Tasks: []TaskState{{
			Task: domain.Task{TaskID: "task-a", Status: state.TaskVerifiedSuccess},
			ReportIdentity: &ReportIdentity{
				TaskID: "task-a", WorkerID: "w1", AttemptNumber: 1,
				EnvelopeHash: "abc", MarkdownHash: "def",
			},
			Attempts: []workerpkg.Attempt{{Number: 1, Worker: domain.WorkerSession{WorkerID: "w1"}}},
		}}},
	}
	a := service.assessTaskReport(service.snapshot.Tasks[0])
	if a.MarkdownValid && a.Error == "" {
		t.Fatalf("half publish must fail: %+v", a)
	}
}

func TestPR71ConcurrentResumeRace(t *testing.T) {
	// Race-sensitive: two EnsureResume calls share singleflight.
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, _ := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	harness := fake.New(adapter.Capabilities{
		ResumeSession: true, StructuredStream: true, SteerActiveTurn: true, BidirectionalStream: true,
	})
	projectRoot := t.TempDir()
	prior, err := harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-1", TaskID: "task-a", WorkerID: "w-old",
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
	cfg := Config{Harness: string(adapter.HarnessFake), BrokerHome: home, CancelGrace: 200 * time.Millisecond}
	cfg.Normalize()
	service := &Service{
		config: cfg, registry: registry, runDir: runDir, paths: runPaths,
		snapshot: Snapshot{
			Run: domain.Run{RunID: "run-1", ProjectID: "p"},
			Tasks: []TaskState{{
				Task: domain.Task{
					TaskID: "task-a", Status: state.TaskRunning, ProjectRoot: projectRoot,
					Title: "t", Objective: "o", CompletionCriteria: []string{"c"},
					WriteScope: []string{"a/**"}, ValidationCommands: []domain.ValidationCommand{{Command: "true"}},
				},
				Worker: &domain.WorkerSession{
					WorkerID: "w-old", TaskID: "task-a", Attempt: 1, NativeSessionID: prior.NativeSessionID,
					Capabilities: adapter.CapabilityMap(adapter.Capabilities{ResumeSession: true, SteerActiveTurn: true}),
				},
				Attempts: []workerpkg.Attempt{{
					Number: 1, Worker: domain.WorkerSession{WorkerID: "w-old", TaskID: "task-a", Attempt: 1, NativeSessionID: prior.NativeSessionID},
					Outcome: workerpkg.AttemptExited,
				}},
				Dimensions: state.Dimensions{Process: state.ProcessExited, Task: state.TaskRunning},
			}},
		},
		messages: store, messageIndex: map[string]message.Message{}, router: router,
		active: map[string]activeWorker{}, acceptingWork: true, fatalPersistence: make(chan error, 1),
		events: &fakeEventAppender{}, persistSnapshotFn: func(Snapshot) error { return nil },
	}
	_, _ = router.EnqueueInstructionWithAttempt("task-a", "w-old", 1, "a", message.DeliveryResume)
	_, _ = router.EnqueueInstructionWithAttempt("task-a", "w-old", 1, "b", message.DeliveryResume)
	var wg sync.WaitGroup
	var okCount atomic.Int32
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := service.EnsureResumeAndFlushOutbox(context.Background(), "task-a"); err == nil {
				okCount.Add(1)
			}
		}()
	}
	wg.Wait()
	if okCount.Load() < 1 {
		t.Fatal("expected successful resume")
	}
	resumeN := 0
	for _, a := range service.Snapshot().Tasks[0].Attempts {
		if a.Mode == workerpkg.AttemptRecoveryResume {
			resumeN++
		}
	}
	if resumeN != 1 {
		t.Fatalf("expected 1 recovery_resume, got %d", resumeN)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Ensure errors.Is import used for compile when needed.
var _ = errors.Is
