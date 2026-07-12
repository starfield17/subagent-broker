package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/state"
	workerpkg "github.com/vnai/subagent-broker/internal/worker"
)

func TestRecoveryInspectUnknownDoesNotResume(t *testing.T) {
	base := TaskState{
		Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning},
		Worker: &domain.WorkerSession{
			WorkerID: "w1", TaskID: "task-a", PID: 42, ProcessStartToken: "tok",
			NativeSessionID: "sess-1",
		},
		Dimensions: state.Dimensions{Process: state.ProcessAlive, Task: state.TaskRunning},
	}
	d := ClassifyRecovery(base, process.Identity{}, errors.New("permission denied"), true)
	if d.Class != RecoveryInspectUnknown {
		t.Fatalf("class=%s want inspect_unknown", d.Class)
	}
	if d.Process != state.ProcessUnknown {
		t.Fatalf("process=%s", d.Process)
	}
	if d.ResumeSessionID != "" {
		t.Fatal("must not set ResumeSessionID")
	}
}

func TestRecoveryIdentityIncompleteDoesNotResume(t *testing.T) {
	base := TaskState{
		Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning},
		Worker: &domain.WorkerSession{
			WorkerID: "w1", TaskID: "task-a", PID: 0, ProcessStartToken: "",
			NativeSessionID: "sess-1",
		},
		Dimensions: state.Dimensions{Process: state.ProcessAlive, Task: state.TaskRunning},
	}
	d := ClassifyRecovery(base, process.Identity{}, nil, true)
	if d.Class != RecoveryIdentityIncomplete {
		t.Fatalf("class=%s want identity_incomplete", d.Class)
	}
	if d.Process != state.ProcessUnknown {
		t.Fatalf("process=%s", d.Process)
	}
	if d.ResumeSessionID != "" {
		t.Fatal("must not resume")
	}
}

func TestRecoveryExplicitNotExistIsExited(t *testing.T) {
	base := TaskState{
		Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning},
		Worker: &domain.WorkerSession{
			WorkerID: "w1", TaskID: "task-a", PID: 42, ProcessStartToken: "tok",
			NativeSessionID: "sess-1",
		},
	}
	// Prefer typed sentinel from process package.
	d := ClassifyRecovery(base, process.Identity{}, fmt.Errorf("%w: pid 42", process.ErrProcessNotFound), true)
	if d.Class != RecoveryExitedResumable {
		t.Fatalf("class=%s", d.Class)
	}
	if d.Process != state.ProcessExited {
		t.Fatalf("process=%s", d.Process)
	}
	// os.ErrNotExist remains accepted via process.IsProcessNotFound for Inspect unwrap.
	d = ClassifyRecovery(base, process.Identity{}, os.ErrNotExist, true)
	if d.Class != RecoveryExitedResumable {
		t.Fatalf("os.ErrNotExist class=%s", d.Class)
	}
}

func TestRecoveryStringErrorIsInspectUnknown(t *testing.T) {
	base := TaskState{
		Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning},
		Worker: &domain.WorkerSession{
			WorkerID: "w1", TaskID: "task-a", PID: 42, ProcessStartToken: "tok",
			NativeSessionID: "sess-1",
		},
	}
	// Free-form "no such process" text is NOT proof of exit.
	d := ClassifyRecovery(base, process.Identity{}, errors.New("no such process"), true)
	if d.Class != RecoveryInspectUnknown {
		t.Fatalf("class=%s want inspect_unknown", d.Class)
	}
	if d.ResumeSessionID != "" {
		t.Fatal("must not resume on string-only error")
	}
}

func TestRecoveryUnknownAggregatesRunDegraded(t *testing.T) {
	service := newCommitService(&fakeEventAppender{}, func(Snapshot) error { return nil })
	service.snapshot.Run.Status = domain.RunRecovering
	root := t.TempDir()
	service.snapshot.Tasks = []TaskState{{
		Task: domain.Task{
			TaskID: "task-a", Status: state.TaskRunning, Title: "t", Objective: "o",
			CompletionCriteria: []string{"c"}, WriteScope: []string{"a/**"},
			ValidationCommands: []domain.ValidationCommand{{Command: "true"}}, ProjectRoot: root,
		},
		Worker: &domain.WorkerSession{
			WorkerID: "w1", TaskID: "task-a", PID: 99, ProcessStartToken: "tok",
			NativeSessionID: "sess",
		},
		Dimensions:    state.Dimensions{Process: state.ProcessAlive, Task: state.TaskRunning},
		ActiveAttempt: 1,
		Attempts: []workerpkg.Attempt{{
			Number: 1, Mode: workerpkg.AttemptFresh, Outcome: workerpkg.AttemptRunning,
			Worker:    domain.WorkerSession{WorkerID: "w1", TaskID: "task-a", Attempt: 1},
			StartedAt: time.Now().UTC(),
		}},
	}}
	d := RecoveryDecision{
		TaskID: "task-a", Class: RecoveryInspectUnknown, WorkerID: "w1",
		Reason: "permission denied", Process: state.ProcessUnknown,
	}
	if err := service.applyRecoveryDecision(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if service.Snapshot().Tasks[0].Dimensions.Process != state.ProcessUnknown {
		t.Fatalf("process=%s", service.Snapshot().Tasks[0].Dimensions.Process)
	}
	if err := service.setRunStatus(domain.RunDegraded, "recovery finished with unknown or orphaned worker processes"); err != nil {
		t.Fatal(err)
	}
	if service.Snapshot().Run.Status != domain.RunDegraded {
		t.Fatalf("run=%s", service.Snapshot().Run.Status)
	}
	rt := service.Snapshot().Tasks[0]
	err := service.executeTask(context.Background(), &rt)
	if err == nil || !containsAll(err.Error(), "unknown process") {
		t.Fatalf("expected refuse unknown process, got %v", err)
	}
}

func TestWorkerExitObservedWithoutTreeConfirmationIsUnknown(t *testing.T) {
	r := WorkerExitResolution{
		ExitObserved:      true,
		TreeExitConfirmed: false,
		RemainingPIDs:     nil,
		OrphanRisk:        true,
	}
	proc, orphan := mapWorkerExitProcessState(r, false)
	if proc != state.ProcessUnknown || !orphan {
		t.Fatalf("got proc=%s orphan=%v", proc, orphan)
	}
}

func TestWorkerExitWithRemainingChildrenIsOrphaned(t *testing.T) {
	r := WorkerExitResolution{
		ExitObserved:      true,
		TreeExitConfirmed: false,
		RemainingPIDs:     []int{100, 101},
	}
	proc, orphan := mapWorkerExitProcessState(r, true)
	if proc != state.ProcessOrphaned || !orphan {
		t.Fatalf("got proc=%s orphan=%v", proc, orphan)
	}
}

func TestWorkerExitTreeConfirmedIsExited(t *testing.T) {
	r := WorkerExitResolution{
		ExitObserved:      false,
		TreeExitConfirmed: true,
	}
	proc, orphan := mapWorkerExitProcessState(r, true)
	if proc != state.ProcessExited || orphan {
		t.Fatalf("got proc=%s orphan=%v", proc, orphan)
	}
}

func TestReplayRejectsDeliveryModeMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	now := time.Now().UTC()
	first := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: "r1", TaskID: "t1",
		Type: message.Instruction, Status: message.Queued, DeliveryMode: message.DeliveryResume,
		CreatedAt: now, UpdatedAt: now, Payload: json.RawMessage(`{"text":"x"}`),
	}
	if err := store.Append(first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.UpdatedAt = now.Add(time.Second)
	second.DeliveryMode = message.DeliveryImmediate
	if err := store.Append(second); err != nil {
		t.Fatal(err)
	}
	_, err := message.ReplayDetailed(path)
	if err == nil {
		t.Fatal("expected corruption on delivery_mode mutation")
	}
	var corrupt *message.ErrJournalCorrupt
	if !errors.As(err, &corrupt) {
		t.Fatalf("got %T %v", err, err)
	}
}

func TestReplayRejectsPrematureResolution(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	now := time.Now().UTC()
	bad := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m1", RunID: "r1", TaskID: "t1",
		Type: message.Question, Status: message.Queued, CreatedAt: now, UpdatedAt: now,
		Payload: json.RawMessage(`{}`), Resolution: json.RawMessage(`{"answer":"nope"}`),
	}
	if err := store.Append(bad); err != nil {
		t.Fatal(err)
	}
	_, err := message.ReplayDetailed(path)
	if err == nil {
		t.Fatal("expected premature resolution corruption")
	}

	path2 := filepath.Join(t.TempDir(), "m2.jsonl")
	store2 := message.NewStore(path2)
	first := message.Message{
		SchemaVersion: message.SchemaVersion, MessageID: "m2", RunID: "r1", TaskID: "t1",
		Type: message.Question, Status: message.Queued, CreatedAt: now, UpdatedAt: now,
		Payload: json.RawMessage(`{}`),
	}
	_ = store2.Append(first)
	second := first
	second.UpdatedAt = now.Add(time.Second)
	second.Resolution = json.RawMessage(`{"answer":"sneak"}`)
	_ = store2.Append(second)
	if _, err := message.ReplayDetailed(path2); err == nil {
		t.Fatal("expected corruption when resolution injected on queued")
	}
}

func TestReplayAllowsFirstDeliveryModeAssignment(t *testing.T) {
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
	second.UpdatedAt = now.Add(time.Second)
	second.DeliveryMode = message.DeliveryResume
	_ = store.Append(second)
	if _, err := message.Replay(path); err != nil {
		t.Fatal(err)
	}
}
