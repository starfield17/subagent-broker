package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
	workerpkg "github.com/vnai/subagent-broker/internal/worker"
)

// --- 1. Report meta/envelope binding ---

func TestReportMetaStatusTamperRejected(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "tasks", "task-a")
	_ = os.MkdirAll(taskDir, 0o700)
	env := report.Envelope{
		SchemaVersion: report.SchemaVersion, TaskID: "task-a", WorkerID: "w1",
		Status: report.StatusSucceeded, Summary: "done", WorkCompleted: []string{"done"},
		FilesChanged: []string{"a.go"}, Validation: []report.Validation{{Command: "true", Passed: true}},
	}
	if err := report.Publish(taskDir, env, 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Tamper meta.status only.
	meta, _, err := report.VerifyDiskArtifacts(taskDir)
	if err != nil {
		t.Fatal(err)
	}
	meta.Status = report.StatusFailed
	raw, _ := json.MarshalIndent(meta, "", "  ")
	_ = os.WriteFile(filepath.Join(taskDir, "report.meta.json"), append(raw, '\n'), 0o600)

	_, _, err = report.VerifyDiskArtifacts(taskDir)
	if err == nil || !errors.Is(err, report.ErrReportIdentityMismatch) {
		t.Fatalf("expected identity mismatch, got %v", err)
	}

	// Barrier path also fails.
	service := &Service{
		paths: storage.RunPaths{Root: root, Tasks: filepath.Join(root, "tasks")},
		snapshot: Snapshot{Tasks: []TaskState{{
			Task: domain.Task{TaskID: "task-a", Status: state.TaskVerifiedSuccess, ProjectRoot: root},
			ReportIdentity: &ReportIdentity{
				TaskID: "task-a", WorkerID: "w1", AttemptNumber: 1,
				EnvelopeHash: meta.EnvelopeHash, MarkdownHash: meta.MarkdownHash,
			},
			Attempts: []workerpkg.Attempt{{Number: 1, Worker: domain.WorkerSession{WorkerID: "w1", TaskID: "task-a", Attempt: 1}}},
		}}},
	}
	// Restore hashes from a clean re-publish for identity fields while meta status is still wrong.
	a := service.assessTaskReport(service.snapshot.Tasks[0])
	if a.IdentityValid || a.Error == "" {
		t.Fatalf("tampered meta status must fail barrier: %+v", a)
	}
}

func TestReportMetaTaskWorkerTamperRejected(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "tasks", "task-a")
	_ = os.MkdirAll(taskDir, 0o700)
	env := report.Envelope{
		SchemaVersion: report.SchemaVersion, TaskID: "task-a", WorkerID: "w1",
		Status: report.StatusSucceeded, Summary: "done", WorkCompleted: []string{"done"},
		FilesChanged: []string{"a.go"}, Validation: []report.Validation{{Command: "true", Passed: true}},
	}
	if err := report.Publish(taskDir, env, 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	meta, _, _ := report.VerifyDiskArtifacts(taskDir)
	meta.WorkerID = "other-worker"
	raw, _ := json.MarshalIndent(meta, "", "  ")
	_ = os.WriteFile(filepath.Join(taskDir, "report.meta.json"), append(raw, '\n'), 0o600)
	_, _, err := report.VerifyDiskArtifacts(taskDir)
	if err == nil {
		t.Fatal("expected worker_id mismatch")
	}
}

func TestReportConsistentPasses(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "tasks", "task-a")
	_ = os.MkdirAll(taskDir, 0o700)
	env := report.Envelope{
		SchemaVersion: report.SchemaVersion, TaskID: "task-a", WorkerID: "w1",
		Status: report.StatusSucceeded, Summary: "done", WorkCompleted: []string{"done"},
		FilesChanged: []string{"a.go"}, Validation: []report.Validation{{Command: "true", Passed: true}},
	}
	if err := report.Publish(taskDir, env, 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	meta, envelope, err := report.VerifyDiskArtifacts(taskDir)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Status != report.StatusSucceeded {
		t.Fatalf("status=%s", envelope.Status)
	}
	service := &Service{
		paths: storage.RunPaths{Root: root, Tasks: filepath.Join(root, "tasks")},
		snapshot: Snapshot{Tasks: []TaskState{{
			Task:          domain.Task{TaskID: "task-a", Status: state.TaskVerifiedSuccess, ProjectRoot: root},
			ActiveAttempt: 0,
			ReportIdentity: &ReportIdentity{
				TaskID: meta.TaskID, WorkerID: meta.WorkerID, AttemptNumber: meta.AttemptNumber,
				EnvelopeHash: meta.EnvelopeHash, MarkdownHash: meta.MarkdownHash, PublishedAt: meta.PublishedAt,
			},
			Attempts: []workerpkg.Attempt{{Number: 1, Worker: domain.WorkerSession{WorkerID: "w1", TaskID: "task-a", Attempt: 1}, Outcome: workerpkg.AttemptExited}},
		}}},
	}
	a := service.assessTaskReport(service.snapshot.Tasks[0])
	if !a.IdentityValid || !a.MarkdownValid || a.EnvelopeStatus != report.StatusSucceeded {
		t.Fatalf("%+v", a)
	}
}

// --- 2. Replay immutability ---

func TestReplayRejectsSameStatusPayloadMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	now := time.Now().UTC()
	first := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: "r1", TaskID: "t1",
		Type: message.Instruction, Status: message.Queued, CreatedAt: now, UpdatedAt: now,
		Payload: json.RawMessage(`{"text":"A"}`),
	}
	if err := store.Append(first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.UpdatedAt = now.Add(time.Second)
	second.Payload = json.RawMessage(`{"text":"B"}`)
	if err := store.Append(second); err != nil {
		t.Fatal(err)
	}
	_, err := message.ReplayDetailed(path)
	if err == nil {
		t.Fatal("expected corruption on payload mutation")
	}
	var corrupt *message.ErrJournalCorrupt
	if !errors.As(err, &corrupt) {
		t.Fatalf("got %T %v", err, err)
	}
}

func TestReplayAllowsIdenticalSameStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	now := time.Now().UTC()
	first := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: "r1", TaskID: "t1",
		Type: message.Instruction, Status: message.Queued, CreatedAt: now, UpdatedAt: now,
		Payload: json.RawMessage(`{"text":"A"}`),
	}
	if err := store.Append(first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.UpdatedAt = now.Add(time.Millisecond)
	if err := store.Append(second); err != nil {
		t.Fatal(err)
	}
	if _, err := message.Replay(path); err != nil {
		t.Fatal(err)
	}
}

func TestReplayRejectsWorkerIDClearAndResolutionRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m.jsonl")
	now := time.Now().UTC()
	first := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: "r1", TaskID: "t1",
		WorkerID: "w1", Type: message.Question, Status: message.Queued, CreatedAt: now, UpdatedAt: now,
		Payload: json.RawMessage(`{}`),
	}
	second := first
	second.UpdatedAt = now.Add(time.Second)
	second.WorkerID = ""
	writeMessagesRaw(t, path, first, second)
	if _, err := message.Replay(path); err == nil {
		t.Fatal("expected worker_id clear corruption")
	}

	path2 := filepath.Join(t.TempDir(), "m2.jsonl")
	ans := first
	ans.Status = message.Answered
	ans.UpdatedAt = now.Add(time.Second)
	ans.Resolution = json.RawMessage(`{"answer":"yes"}`)
	rewritten := ans
	rewritten.UpdatedAt = now.Add(2 * time.Second)
	rewritten.Resolution = json.RawMessage(`{"answer":"no"}`)
	writeMessagesRaw(t, path2, first, ans, rewritten)
	if _, err := message.Replay(path2); err == nil {
		t.Fatal("expected resolution rewrite corruption")
	}
}

// --- 3. Delivery pending ---

func TestDeliveredInstructionNotPendingForDelivery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := router.EnqueueInstruction("task-a", "w1", "hello", message.DeliveryResume)
	if err != nil {
		t.Fatal(err)
	}
	pending := router.PendingInstructions("task-a", message.DeliveryResume)
	if len(pending) != 1 {
		t.Fatalf("queued should be pending: %d", len(pending))
	}
	delivered, err := router.RecordDeliveryAttempt(queued.MessageID, message.Delivered, nil)
	if err != nil {
		t.Fatal(err)
	}
	if delivered.Status != message.Delivered {
		t.Fatal(delivered.Status)
	}
	pending = router.PendingInstructions("task-a", message.DeliveryResume)
	if len(pending) != 0 {
		t.Fatalf("delivered must not be delivery-pending: %+v", pending)
	}
	if message.IsDeliveryPending(delivered) {
		t.Fatal("IsDeliveryPending true for delivered")
	}
}

// --- 4. Session driver observes Exited with open streams ---

func TestRunWorkerSessionObservesExitWithOpenStreams(t *testing.T) {
	events := make(chan adapter.NativeEvent) // never closed
	stderr := make(chan adapter.OutputChunk) // never closed
	exited := make(chan adapter.ExitStatus, 1)
	exited <- adapter.ExitStatus{Code: 0}
	close(exited)

	session := adapter.Session{
		NativeSessionID: "sess-open",
		Events:          events,
		Stderr:          stderr,
		Exited:          exited,
	}
	service := newCommitService(&fakeEventAppender{}, func(Snapshot) error { return nil })
	service.config.CancelGrace = 50 * time.Millisecond
	service.snapshot.Tasks = []TaskState{{
		Task:       domain.Task{TaskID: "task-a", Status: state.TaskRunning, ProjectRoot: t.TempDir()},
		Worker:     &domain.WorkerSession{WorkerID: "w1", TaskID: "task-a"},
		Dimensions: state.Dimensions{Process: state.ProcessAlive, Task: state.TaskRunning},
	}}
	runtime := service.snapshot.Tasks[0]
	// Use a no-op adapter for terminate.
	harness := &exitOnlyAdapter{}

	done := make(chan workerSessionResult, 1)
	go func() {
		result, err := service.runWorkerSession(context.Background(), &runtime, harness, session, "w1", process.Identity{})
		if err != nil {
			t.Errorf("runWorkerSession: %v", err)
		}
		done <- result
	}()
	select {
	case result := <-done:
		if !result.Resolution.ExitObserved {
			t.Fatal("expected ExitObserved with open streams")
		}
		if result.Resolution.ProcessState != state.ProcessExited && result.Resolution.ProcessState != state.ProcessUnknown {
			// Incomplete identity → may be unknown; exit was observed so should be exited.
			if result.Exit.Code != 0 {
				t.Fatalf("exit=%+v resolution=%+v", result.Exit, result.Resolution)
			}
		}
		if result.Exit.Code != 0 {
			t.Fatalf("exit code=%d", result.Exit.Code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("driver hung with open streams after Exited")
	}
}

type exitOnlyAdapter struct{}

func (exitOnlyAdapter) Descriptor() adapter.Descriptor { return adapter.Descriptor{Name: "exit-only"} }
func (exitOnlyAdapter) Probe(context.Context, adapter.ProbeRequest) (adapter.ProbeResult, error) {
	return adapter.ProbeResult{}, nil
}
func (exitOnlyAdapter) StartSession(context.Context, adapter.StartRequest) (adapter.Session, error) {
	return adapter.Session{}, adapter.ErrUnsupported
}
func (exitOnlyAdapter) ResumeSession(context.Context, adapter.ResumeRequest) (adapter.Session, error) {
	return adapter.Session{}, adapter.ErrUnsupported
}
func (exitOnlyAdapter) SendMessage(context.Context, string, string) (adapter.DeliveryResult, error) {
	return adapter.DeliveryResult{}, adapter.ErrUnsupported
}
func (exitOnlyAdapter) SteerActiveTurn(context.Context, string, string) (adapter.DeliveryResult, error) {
	return adapter.DeliveryResult{}, adapter.ErrUnsupported
}
func (exitOnlyAdapter) InterruptTurn(context.Context, string) error { return nil }
func (exitOnlyAdapter) TerminateSession(context.Context, string) error {
	return nil
}
func (exitOnlyAdapter) ReadHistory(context.Context, string) ([]adapter.NativeEvent, error) {
	return nil, adapter.ErrUnsupported
}
func (exitOnlyAdapter) RespondPermission(context.Context, string, adapter.PermissionDecision) error {
	return adapter.ErrUnsupported
}
func (exitOnlyAdapter) GetDiff(context.Context, string) ([]string, error) {
	return nil, adapter.ErrUnsupported
}
func (exitOnlyAdapter) GetUsage(context.Context, string) (adapter.Usage, error) {
	return adapter.Usage{}, adapter.ErrUnsupported
}
func (exitOnlyAdapter) NormalizeEvent(adapter.NativeEvent) (event.Input, error) {
	return event.Input{}, nil
}
func (exitOnlyAdapter) CollectFinalResult(context.Context, string) (report.Envelope, error) {
	return report.Envelope{}, adapter.ErrUnsupported
}

// --- 5. Cancel persistence errors ---

type failingEventAppender struct {
	failOn string
	seq    uint64
}

func (f *failingEventAppender) Append(input event.Input) (event.Event, error) {
	if f.failOn != "" && strings.Contains(string(input.Type), f.failOn) {
		return event.Event{}, errors.New("event append failed: " + f.failOn)
	}
	if f.failOn == "any" {
		return event.Event{}, errors.New("event append failed")
	}
	f.seq++
	return event.Event{Seq: f.seq, Type: input.Type, Source: input.Source}, nil
}

func TestRequestCancelPropagatesPersistenceFailure(t *testing.T) {
	appender := &failingEventAppender{failOn: "cancel.tree"}
	service := newCommitService(appender, func(Snapshot) error { return nil })
	service.snapshot.Run.Status = domain.RunRunning
	service.active = map[string]activeWorker{
		"t1": {taskID: "t1", workerID: "w1", sessionID: "s1", adapter: exitOnlyAdapter{}, identity: process.Identity{}},
	}
	err := service.RequestCancel(context.Background())
	if err == nil {
		t.Fatal("expected cancel persistence error")
	}
	if !strings.Contains(err.Error(), "event append failed") {
		t.Fatalf("got %v", err)
	}
}

func TestRequestCancelPropagatesOrphanRiskCommitFailure(t *testing.T) {
	// Snapshot persist fails on ProcessOrphaned commit during incomplete-identity cancel.
	persistN := 0
	service := newCommitService(&fakeEventAppender{}, func(Snapshot) error {
		persistN++
		// Fail after first successful commits if any; fail orphan-risk commit.
		if persistN >= 1 {
			return errors.New("snapshot disk full")
		}
		return nil
	})
	service.snapshot.Run.Status = domain.RunRunning
	service.snapshot.Tasks = []TaskState{{
		Task:       domain.Task{TaskID: "t1", Status: state.TaskRunning},
		Worker:     &domain.WorkerSession{WorkerID: "w1", TaskID: "t1"},
		Dimensions: state.Dimensions{Process: state.ProcessAlive},
	}}
	service.active = map[string]activeWorker{
		"t1": {taskID: "t1", workerID: "w1", sessionID: "s1", adapter: exitOnlyAdapter{}, identity: process.Identity{}},
	}
	err := service.RequestCancel(context.Background())
	if err == nil {
		t.Fatal("expected orphan-risk commit failure to surface")
	}
}

// --- 6. Corrupt journal status persistence ---

func TestCorruptJournalStatusPersistenceFailure(t *testing.T) {
	home := t.TempDir()
	layout, err := storage.NewLayout(home)
	if err != nil {
		t.Fatal(err)
	}
	projectID, runID := "proj", "run-persist-fail"
	runDir, err := layout.EnsureRun(projectID, runID)
	if err != nil {
		t.Fatal(err)
	}
	runPaths, _ := layout.RunPaths(projectID, runID)
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
	_ = storage.AtomicWriteJSON(taskPaths.Task, domain.Task{
		TaskID: "task-a", Title: "t", Objective: "o", CompletionCriteria: []string{"c"},
		WriteScope: []string{"a/**"}, ValidationCommands: []domain.ValidationCommand{{Command: "true"}},
		ProjectRoot: t.TempDir(), WaveID: "wave-1", Status: state.TaskPlanned,
	}, 0o600)

	// Corrupt journal.
	now := time.Now().UTC()
	store := message.NewStore(runPaths.Messages)
	base := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: runID, TaskID: "task-a",
		Type: message.Question, Status: message.Queued, CreatedAt: now, UpdatedAt: now, Payload: json.RawMessage(`{}`),
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

	// Make status.md path a directory so fail-closed status write fails.
	// state.json remains writable; errors.Join still surfaces status write failure.
	_ = os.Remove(runPaths.Status)
	if err := os.Mkdir(runPaths.Status, 0o700); err != nil {
		t.Fatal(err)
	}

	registry := adapter.NewRegistry()
	service, err := Load(runDir, registry, true)
	if err == nil {
		t.Fatal("expected Load error when fail-closed status cannot persist")
	}
	if !strings.Contains(err.Error(), "persist fail-closed") && !strings.Contains(err.Error(), "message journal corrupt") {
		t.Fatalf("unexpected error: %v", err)
	}
	// Marker must still disable append even when Load returns error.
	fresh := message.NewStore(runPaths.Messages)
	if err := fresh.Append(base); err == nil {
		t.Fatal("append must be refused via corruption marker")
	}
	// If a service was returned alongside the error, it must not accept work.
	if service != nil {
		if service.AcceptingWork() {
			t.Fatal("AcceptingWork must be false")
		}
		if !service.messages.AppendDisabled() {
			t.Fatal("store must be disabled")
		}
	}
}
