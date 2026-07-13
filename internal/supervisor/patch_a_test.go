package supervisor

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
)

func failureRegistry(t *testing.T) *adapter.Registry {
	t.Helper()
	harness := fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})
	if err := harness.RegisterScenario(fake.Scenario{
		Name:   "normal_stream",
		Events: []adapter.NativeEvent{{Kind: event.ResultSubmitted}},
	}); err != nil {
		t.Fatal(err)
	}
	registry := adapter.NewRegistry()
	if err := registry.Register(harness); err != nil {
		t.Fatal(err)
	}
	return registry
}

func runCollectionFailure(t *testing.T) (Snapshot, string, storage.Layout) {
	t.Helper()
	runDir, layout := writeMultiWaveFixture(t)
	service, err := Load(runDir, failureRegistry(t), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Initialize(); err != nil {
		service.Close()
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatalf("ordinary Task failure must finish Supervisor cleanly: %v", err)
	}
	return service.Snapshot(), runDir, layout
}

func TestTaskFailureTerminalizesWaveAndRun(t *testing.T) {
	snapshot, runDir, layout := runCollectionFailure(t)

	if snapshot.Run.Status != domain.RunFailed || !runTerminal(snapshot.Run.Status) {
		t.Fatalf("run=%s terminal=%v error=%q", snapshot.Run.Status, runTerminal(snapshot.Run.Status), snapshot.LastError)
	}
	if snapshot.Wave.Status != domain.WaveFailed || snapshot.Waves[0].Status != domain.WaveFailed {
		t.Fatalf("current Wave=%+v waves=%+v", snapshot.Wave, snapshot.Waves)
	}
	if !strings.Contains(snapshot.LastError, "task task-a") {
		t.Fatalf("joined failure reason=%q", snapshot.LastError)
	}
	if snapshot.Tasks[2].Task.Status != state.TaskPlanned || snapshot.Tasks[2].Worker != nil || len(snapshot.Tasks[2].Attempts) != 0 {
		t.Fatalf("later Wave Task was started or mutated: %+v", snapshot.Tasks[2])
	}
	runPaths, err := layout.RunPaths(string(snapshot.Run.ProjectID), string(snapshot.Run.RunID))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{runPaths.Status, filepath.Join(runDir, "summary.json"), filepath.Join(runDir, "summary.md")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("terminal artifact %s missing: %v", path, err)
		}
	}
}

func TestMultipleTaskFailuresRemainAggregated(t *testing.T) {
	snapshot, runDir, layout := runCollectionFailure(t)

	for _, taskID := range []string{"task-a", "task-b"} {
		if !strings.Contains(snapshot.LastError, "task "+taskID) || !strings.Contains(snapshot.Wave.FailureReason, "task "+taskID) {
			t.Fatalf("Task %s missing from persisted failure reasons: run=%q wave=%q", taskID, snapshot.LastError, snapshot.Wave.FailureReason)
		}
		var found bool
		for _, runtime := range snapshot.Tasks {
			if string(runtime.Task.TaskID) == taskID {
				found = true
				if runtime.Task.Status != state.TaskFailed {
					t.Fatalf("Task %s status=%s", taskID, runtime.Task.Status)
				}
			}
		}
		if !found {
			t.Fatalf("Task %s missing from snapshot", taskID)
		}
	}
	paths, err := layout.WavePaths(string(snapshot.Run.ProjectID), string(snapshot.Run.RunID), "wave-2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(paths.Barrier); !os.IsNotExist(err) {
		t.Fatalf("later Wave barrier should not exist, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "summary.json")); err != nil {
		t.Fatal(err)
	}
}

func TestClassifyRecoveryNeverStarted(t *testing.T) {
	runtime := TaskState{
		Task:       domain.Task{TaskID: "planned", Status: state.TaskPlanned},
		Dimensions: state.Dimensions{Process: state.ProcessQueued, Task: state.TaskPlanned},
	}
	decision := ClassifyRecovery(runtime, process.Identity{}, nil, true)
	if decision.Class != RecoveryNeverStarted {
		t.Fatalf("class=%s want %s", decision.Class, RecoveryNeverStarted)
	}
}

func TestStartedTaskWithoutWorkerIsMissingIdentity(t *testing.T) {
	runtime := TaskState{
		Task:       domain.Task{TaskID: "started", Status: state.TaskRunning},
		Dimensions: state.Dimensions{Process: state.ProcessStarting, Task: state.TaskRunning},
	}
	decision := ClassifyRecovery(runtime, process.Identity{}, nil, true)
	if decision.Class != RecoveryMissingIdentity {
		t.Fatalf("class=%s want %s", decision.Class, RecoveryMissingIdentity)
	}
}

func TestRecoveryNeverStartedLeavesTaskUnchanged(t *testing.T) {
	service := newCommitService(&fakeEventAppender{}, func(Snapshot) error { return nil })
	service.snapshot.Tasks = []TaskState{{
		Task:       domain.Task{TaskID: "planned", Status: state.TaskPlanned},
		Dimensions: state.Dimensions{Process: state.ProcessQueued, Task: state.TaskPlanned},
	}}
	before := service.Snapshot().Tasks[0]
	decision := ClassifyRecovery(before, process.Identity{}, nil, true)
	if decision.Class != RecoveryNeverStarted {
		t.Fatalf("class=%s", decision.Class)
	}
	if err := service.applyRecoveryDecision(context.Background(), decision); err != nil {
		t.Fatal(err)
	}
	if after := service.Snapshot().Tasks[0]; !reflect.DeepEqual(after, before) {
		t.Fatalf("never-started Task changed: before=%+v after=%+v", before, after)
	}
}

func TestMixedRecoveryPreservesLaterNeverStartedTasksAndIsIdempotent(t *testing.T) {
	runDir, layout := writeMultiWaveFixture(t)
	service, err := Load(runDir, failureRegistry(t), false)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	if err := service.Initialize(); err != nil {
		t.Fatal(err)
	}

	service.mu.Lock()
	service.snapshot.Run.Status = domain.RunRunning
	service.snapshot.Run.CurrentWave = "wave-1"
	service.snapshot.Wave = service.snapshot.Waves[0]
	service.snapshot.Wave.Status = domain.WaveRunning
	service.snapshot.Waves[0].Status = domain.WaveRunning
	service.snapshot.Wave = service.snapshot.Waves[0]
	service.snapshot.Tasks[0].Task.Status = state.TaskRunning
	service.snapshot.Tasks[0].Dimensions.Task = state.TaskRunning
	service.snapshot.Tasks[0].Dimensions.Process = state.ProcessStarting
	service.mu.Unlock()

	beforeLater := service.Snapshot().Tasks[2]
	if err := service.reconcileRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := service.Snapshot()
	if first.Run.Status != domain.RunFailed || first.Wave.Status != domain.WaveFailed {
		t.Fatalf("recovery status run=%s wave=%s", first.Run.Status, first.Wave.Status)
	}
	if first.Tasks[0].Task.Status != state.TaskFailed {
		t.Fatalf("started broken Task status=%s", first.Tasks[0].Task.Status)
	}
	if !reflect.DeepEqual(first.Tasks[2], beforeLater) {
		t.Fatalf("later never-started Task changed: before=%+v after=%+v", beforeLater, first.Tasks[2])
	}

	replay, err := event.Replay(filepath.Join(runDir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	eventsBefore := len(replay.Events)

	if err := service.reconcileRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := service.Snapshot()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("second recovery mutated terminal snapshot: first=%+v second=%+v", first, second)
	}
	replay, err = event.Replay(filepath.Join(runDir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(replay.Events) != eventsBefore {
		t.Fatalf("second recovery appended events: before=%d after=%d", eventsBefore, len(replay.Events))
	}

	paths, err := layout.TaskPaths(string(first.Run.ProjectID), string(first.Run.RunID), "task-c")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(paths.Report); !os.IsNotExist(err) {
		t.Fatalf("never-started Task report should not exist, err=%v", err)
	}
}
