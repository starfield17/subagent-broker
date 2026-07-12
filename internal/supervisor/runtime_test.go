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
