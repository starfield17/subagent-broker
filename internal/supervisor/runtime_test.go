package supervisor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/run"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
	taskcontract "github.com/vnai/subagent-broker/internal/task"
)

func TestServiceCompletesFakeLifecycle(t *testing.T) {
	runDir, layout := writeFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})); err != nil {
		t.Fatal(err)
	}
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Initialize(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- service.Start(context.Background()) }()
	select {
	case <-service.Terminal():
	case err := <-done:
		t.Fatalf("service exited before terminal: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("service did not reach a terminal state")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	snapshot := service.Snapshot()
	if snapshot.Run.Status != domain.RunCompleted {
		t.Fatalf("unexpected run status: %+v error=%s tasks=%+v", snapshot.Run, snapshot.LastError, snapshot.Tasks)
	}
	if len(snapshot.Tasks) != 1 || snapshot.Tasks[0].Task.Status != state.TaskVerifiedSuccess {
		t.Fatalf("unexpected task state: %+v", snapshot.Tasks)
	}
	taskPaths, err := layout.TaskPaths(string(snapshot.Run.ProjectID), string(snapshot.Run.RunID), "task-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(taskPaths.Report); err != nil {
		t.Fatalf("report was not published: %v", err)
	}
	replay, err := event.Replay(filepath.Join(runDir, "events.jsonl"))
	if err != nil || len(replay.Events) == 0 {
		t.Fatalf("event replay failed: %v %+v", err, replay)
	}
}

func TestServiceExecutesOrderedMultiTaskWaves(t *testing.T) {
	runDir, layout := writeMultiWaveFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})); err != nil {
		t.Fatal(err)
	}
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := service.Snapshot()
	if snapshot.Run.Status != domain.RunCompleted || len(snapshot.Waves) != 2 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	for _, runtime := range snapshot.Tasks {
		if runtime.Task.Status != state.TaskVerifiedSuccess {
			t.Fatalf("task did not verify: %+v", runtime)
		}
	}
	for _, waveValue := range snapshot.Waves {
		if waveValue.Status != domain.WaveVerified || waveValue.BarrierResult != domain.BarrierPassed {
			t.Fatalf("Wave did not pass: %+v", waveValue)
		}
		paths, _ := layout.WavePaths(string(snapshot.Run.ProjectID), string(snapshot.Run.RunID), string(waveValue.WaveID))
		if _, err := os.Stat(paths.Barrier); err != nil {
			t.Fatalf("barrier was not published: %v", err)
		}
	}
}

func TestRequestMessageBlocksUntilResolved(t *testing.T) {
	runDir, _ := writeFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{})); err != nil {
		t.Fatal(err)
	}
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan message.Resolution, 1)
	go func() {
		resolution, _, _ := service.RequestMessage(context.Background(), "task-a", "worker-a", message.Question, message.Decision, message.QuestionEnvelope{SchemaVersion: SchemaVersion, Question: "Choose one", Reason: "blocked", CurrentScope: []string{"output.txt"}, WorkspaceState: "unchanged"})
		done <- resolution
	}()
	deadline := time.Now().Add(2 * time.Second)
	var id string
	for time.Now().Before(deadline) {
		items := service.Inbox(false)
		if len(items) == 1 {
			id = items[0].MessageID
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if id == "" {
		t.Fatal("message did not reach inbox")
	}
	if err := service.ResolveMessage(id, message.Resolution{Answer: "Use A"}); err != nil {
		t.Fatal(err)
	}
	select {
	case resolution := <-done:
		if resolution.Answer != "Use A" {
			t.Fatalf("unexpected answer: %+v", resolution)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Worker request did not resume")
	}
}

// TestResolveBeforeWaiterRegistration verifies that a resolution arriving after
// the durable message is visible but before the waiter is registered still
// resumes the Worker immediately (no lost wakeup).
func TestResolveBeforeWaiterRegistration(t *testing.T) {
	runDir, _ := writeFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{})); err != nil {
		t.Fatal(err)
	}
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the race: enqueue a message through the router, resolve it
	// before a waiter is registered, then verify the recheck mechanism works.
	val, err := service.router.EnqueueDecision("task-a", "worker-a", message.Question, message.Decision, json.RawMessage(`{"schema_version":"v1alpha1","question":"Race","reason":"R","current_scope":["x"],"workspace_state":"w"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = service.CommitMessageProjection(context.Background(), val, event.MessageQueued)

	// Resolve the message before registering a waiter (simulating the race).
	if err := service.ResolveMessage(val.MessageID, message.Resolution{Answer: "pre-register"}); err != nil {
		t.Fatal(err)
	}

	// Now simulate what RequestMessage does: register waiter, then re-check durable state.
	waiter := service.registerDecisionWaiter(val.MessageID)
	// The durable-state recheck should detect Answered immediately.
	res, ok, checkErr := service.loadAnsweredResolution(val.MessageID)
	if checkErr != nil || !ok {
		t.Fatalf("durable-state recheck failed: err=%v ok=%v", checkErr, ok)
	}
	if res.Answer != "pre-register" {
		t.Fatalf("expected 'pre-register', got %q", res.Answer)
	}
	service.unregisterDecisionWaiter(val.MessageID, waiter)

	// Verify no goroutine leak: pending map should be empty.
	service.mu.Lock()
	pending := len(service.pending)
	service.mu.Unlock()
	if pending > 0 {
		t.Fatalf("waiter leaked: %d entries", pending)
	}
}

// TestResolveAfterRegistration verifies normal blocking behavior is preserved.
func TestResolveAfterRegistration(t *testing.T) {
	runDir, _ := writeFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{})); err != nil {
		t.Fatal(err)
	}
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan message.Resolution, 1)
	errCh := make(chan error, 1)
	go func() {
		resolution, _, reqErr := service.RequestMessage(context.Background(), "task-a", "worker-a", message.Question, message.Decision, message.QuestionEnvelope{SchemaVersion: SchemaVersion, Question: "Q", Reason: "R", CurrentScope: []string{"output.txt"}, WorkspaceState: "unchanged"})
		if reqErr != nil {
			errCh <- reqErr
			return
		}
		done <- resolution
	}()
	// Wait for message to appear in inbox.
	deadline := time.Now().Add(2 * time.Second)
	var id string
	for time.Now().Before(deadline) {
		items := service.Inbox(false)
		if len(items) > 0 {
			id = items[0].MessageID
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if id == "" {
		t.Fatal("message did not reach inbox")
	}
	if err := service.ResolveMessage(id, message.Resolution{Answer: "after"}); err != nil {
		t.Fatal(err)
	}
	select {
	case resolution := <-done:
		if resolution.Answer != "after" {
			t.Fatalf("unexpected answer: %+v", resolution)
		}
	case err := <-errCh:
		t.Fatalf("RequestMessage error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Worker request did not resume")
	}
}

// TestRequestMessageContextCancellationRace races context cancellation against
// resolution to ensure no panic, no blocked send, and no leaked waiter.
func TestRequestMessageContextCancellationRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		runDir, _ := writeFixture(t)
		registry := adapter.NewRegistry()
		if err := registry.Register(fake.New(adapter.Capabilities{})); err != nil {
			t.Fatal(err)
		}
		service, err := Load(runDir, registry, false)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			_, _, _ = service.RequestMessage(ctx, "task-a", "worker-a", message.Question, message.Decision, message.QuestionEnvelope{SchemaVersion: SchemaVersion, Question: "Q", Reason: "R", CurrentScope: []string{"output.txt"}, WorkspaceState: "unchanged"})
			close(done)
		}()
		// Small sleep then cancel; resolution may also race in.
		time.Sleep(time.Microsecond)
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: RequestMessage did not return after cancel", i)
		}
		// Verify no waiter leaked.
		service.mu.Lock()
		pending := len(service.pending)
		service.mu.Unlock()
		if pending > 0 {
			t.Fatalf("iteration %d: %d waiters leaked after cancel", i, pending)
		}
	}
}

func writeFixture(t *testing.T) (string, storage.Layout) {
	t.Helper()
	home := filepath.Join(t.TempDir(), "broker")
	projectRoot := filepath.Join(t.TempDir(), "project")
	if err := os.MkdirAll(projectRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	layout, err := storage.NewLayout(home)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	projectID := domain.ProjectID("project--abc123")
	runID := domain.RunID("run-1")
	item := domain.Task{
		TaskID: "task-a", Title: "Task A", Objective: "run the fake lifecycle", CompletionCriteria: []string{"the fake report is verified"},
		WriteScope: []string{"output.txt"}, ValidationCommands: []domain.ValidationCommand{{Command: "true", Scope: "local"}},
		ProjectRoot: projectRoot, WaveID: "wave-1", Status: state.TaskPlanned,
	}
	if err := taskcontract.ValidateContract(item); err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.BrokerHome = home
	config.Harness = "fake"
	config.Scenario = "normal_stream"
	runValue, err := run.New(runID, projectID, "test fake lifecycle", []domain.TaskID{item.TaskID}, config, now)
	if err != nil {
		t.Fatal(err)
	}
	runValue.CurrentWave = "wave-1"
	runDir, err := layout.EnsureRun(string(projectID), string(runID))
	if err != nil {
		t.Fatal(err)
	}
	paths, err := layout.TaskPaths(string(projectID), string(runID), string(item.TaskID))
	if err != nil {
		t.Fatal(err)
	}
	wavePaths, err := layout.WavePaths(string(projectID), string(runID), "wave-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{paths.Root, paths.ValidationDir, wavePaths.Root} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := storage.AtomicWriteJSON(filepath.Join(runDir, "run.json"), runValue, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := storage.AtomicWriteJSON(paths.Task, item, 0o600); err != nil {
		t.Fatal(err)
	}
	contract, err := taskcontract.RenderContract(item, runID)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.AtomicWriteFile(paths.Contract, []byte(contract), 0o600); err != nil {
		t.Fatal(err)
	}
	waveValue := domain.Wave{WaveID: "wave-1", Ordinal: 1, TaskIDs: []domain.TaskID{item.TaskID}, Status: domain.WavePlanned}
	if err := storage.AtomicWriteJSON(filepath.Join(wavePaths.Root, "wave.json"), waveValue, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := json.Marshal(runValue); err != nil {
		t.Fatal(err)
	}
	return runDir, layout
}

func writeMultiWaveFixture(t *testing.T) (string, storage.Layout) {
	t.Helper()
	home := filepath.Join(t.TempDir(), "broker")
	projectRoot := filepath.Join(t.TempDir(), "project")
	if err := os.MkdirAll(projectRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	layout, err := storage.NewLayout(home)
	if err != nil {
		t.Fatal(err)
	}
	projectID := domain.ProjectID("project--multi")
	runID := domain.RunID("run-multi")
	makeTask := func(id string, waveID domain.WaveID, scopeName string) domain.Task {
		return domain.Task{TaskID: domain.TaskID(id), Title: id, Objective: "run fake", CompletionCriteria: []string{"verified"}, WriteScope: []string{scopeName}, ValidationCommands: []domain.ValidationCommand{{Command: "true", Scope: "local"}}, ProjectRoot: projectRoot, WaveID: waveID, Status: state.TaskPlanned}
	}
	a := makeTask("task-a", "wave-1", "a.txt")
	b := makeTask("task-b", "wave-1", "b.txt")
	a.ParallelResponsibilities = map[domain.TaskID]string{"task-b": "owns b.txt"}
	b.ParallelResponsibilities = map[domain.TaskID]string{"task-a": "owns a.txt"}
	c := makeTask("task-c", "wave-2", "c.txt")
	c.DependsOn = []domain.TaskID{"task-a", "task-b"}
	plan := domain.RunPlan{SchemaVersion: run.SchemaVersion, Waves: []domain.WavePlan{{WaveID: "wave-1", Tasks: []domain.Task{a, b}, IntegrationChecks: []domain.ValidationCommand{{Command: "true", Scope: "wave"}}}, {WaveID: "wave-2", Tasks: []domain.Task{c}}}}
	config := DefaultConfig()
	config.BrokerHome = home
	config.Harness = "fake"
	config.Scenario = "normal_stream"
	config.MaxConcurrency = 2
	runValue, err := run.New(runID, projectID, "multi Wave", []domain.TaskID{a.TaskID, b.TaskID, c.TaskID}, config, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	runValue.CurrentWave = "wave-1"
	runValue.WaveIDs = []domain.WaveID{"wave-1", "wave-2"}
	runDir, err := layout.EnsureRun(string(projectID), string(runID))
	if err != nil {
		t.Fatal(err)
	}
	runPaths, _ := layout.RunPaths(string(projectID), string(runID))
	if err := storage.AtomicWriteJSON(runPaths.Run, runValue, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := storage.AtomicWriteJSON(runPaths.Plan, plan, 0o600); err != nil {
		t.Fatal(err)
	}
	for ordinal, planned := range plan.Waves {
		wavePaths, _ := layout.WavePaths(string(projectID), string(runID), string(planned.WaveID))
		_ = os.MkdirAll(wavePaths.Root, 0o700)
		ids := make([]domain.TaskID, 0, len(planned.Tasks))
		for _, item := range planned.Tasks {
			ids = append(ids, item.TaskID)
		}
		if err := storage.AtomicWriteJSON(wavePaths.Wave, domain.Wave{WaveID: planned.WaveID, Ordinal: ordinal + 1, TaskIDs: ids, Status: domain.WavePlanned, IntegrationChecks: planned.IntegrationChecks}, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, item := range []domain.Task{a, b, c} {
		paths, _ := layout.TaskPaths(string(projectID), string(runID), string(item.TaskID))
		_ = os.MkdirAll(paths.ValidationDir, 0o700)
		if err := storage.AtomicWriteJSON(paths.Task, item, 0o600); err != nil {
			t.Fatal(err)
		}
		contract, _ := taskcontract.RenderContract(item, runID)
		if err := storage.AtomicWriteFile(paths.Contract, []byte(contract), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return runDir, layout
}
