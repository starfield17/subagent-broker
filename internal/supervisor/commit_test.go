package supervisor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/state"
)

type fakeEventAppender struct {
	mu     sync.Mutex
	err    error
	inputs []event.Input
	count  int
}

func (f *fakeEventAppender) Append(input event.Input) (event.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return event.Event{}, f.err
	}
	f.count++
	f.inputs = append(f.inputs, input)
	return event.Event{
		SchemaVersion: SchemaVersion,
		Seq:           uint64(f.count),
		EventID:       "evt",
		RunID:         "run-1",
		Source:        input.Source,
		Type:          input.Type,
		Severity:      input.Severity,
		Timestamp:     time.Now().UTC(),
	}, nil
}

func (f *fakeEventAppender) len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count
}

func newCommitService(appender eventAppender, persist func(Snapshot) error) *Service {
	return &Service{
		snapshot: Snapshot{
			SchemaVersion: SchemaVersion,
			Run: domain.Run{
				SchemaVersion: SchemaVersion,
				RunID:         "run-1",
				ProjectID:     "project-1",
				Status:        domain.RunRunning,
				TaskIDs:       []domain.TaskID{"task-a"},
			},
			Wave: domain.Wave{WaveID: "wave-1", Ordinal: 1, Status: domain.WaveRunning},
			Waves: []domain.Wave{
				{WaveID: "wave-1", Ordinal: 1, Status: domain.WaveRunning},
			},
			Tasks: []TaskState{
				{
					Task: domain.Task{
						TaskID:     "task-a",
						WaveID:     "wave-1",
						Title:      "A",
						WriteScope: []string{"internal/a/**"},
					},
				},
			},
			UpdatedAt: time.Now().UTC(),
		},
		events:            appender,
		fatalPersistence:  make(chan error, 1),
		acceptingWork:     true,
		persistSnapshotFn: persist,
	}
}

func TestCommitMutateFailureDoesNotAppendOrPersist(t *testing.T) {
	appender := &fakeEventAppender{}
	persistCalls := 0
	service := newCommitService(appender, func(Snapshot) error {
		persistCalls++
		return nil
	})
	beforeStatus := service.snapshot.Run.Status

	_, err := service.Commit(context.Background(), CommitRequest{
		Event: event.Input{Source: "test", Type: "run.mutated"},
		Mutate: func(candidate *Snapshot) error {
			candidate.Run.Status = domain.RunFailed
			return errors.New("mutate rejected")
		},
	})
	var commitErr *CommitError
	if !errors.As(err, &commitErr) || commitErr.Stage != CommitStageValidate {
		t.Fatalf("expected validate CommitError, got %v", err)
	}
	if appender.len() != 0 {
		t.Fatalf("event was appended on mutate failure: %d", appender.len())
	}
	if persistCalls != 0 {
		t.Fatalf("snapshot was persisted on mutate failure: %d", persistCalls)
	}
	if service.snapshot.Run.Status != beforeStatus {
		t.Fatalf("memory changed on mutate failure: %s", service.snapshot.Run.Status)
	}
	if !service.AcceptingWork() {
		t.Fatal("mutate failure must not fail-close")
	}
}

func TestCommitValidateFailureDoesNotAppendOrPersist(t *testing.T) {
	appender := &fakeEventAppender{}
	persistCalls := 0
	service := newCommitService(appender, func(Snapshot) error {
		persistCalls++
		return nil
	})

	_, err := service.Commit(context.Background(), CommitRequest{
		Event: event.Input{Source: "test", Type: "run.mutated"},
		Mutate: func(candidate *Snapshot) error {
			candidate.LastError = "changed"
			return nil
		},
		Validate: func(before, candidate Snapshot) error {
			if candidate.LastError == "" {
				t.Fatal("validate should see mutated candidate")
			}
			if before.LastError != "" {
				t.Fatal("before snapshot must remain unchanged")
			}
			return errors.New("validate rejected")
		},
	})
	var commitErr *CommitError
	if !errors.As(err, &commitErr) || commitErr.Stage != CommitStageValidate {
		t.Fatalf("expected validate CommitError, got %v", err)
	}
	if appender.len() != 0 || persistCalls != 0 {
		t.Fatalf("side effects on validate failure: appends=%d persists=%d", appender.len(), persistCalls)
	}
	if service.snapshot.LastError != "" {
		t.Fatalf("memory changed on validate failure: %+v", service.snapshot)
	}
}

func TestCommitEventAppendFailureFailCloses(t *testing.T) {
	appender := &fakeEventAppender{err: errors.New("disk full")}
	persistCalls := 0
	service := newCommitService(appender, func(Snapshot) error {
		persistCalls++
		return nil
	})
	before := service.snapshot.Run.Status

	_, err := service.Commit(context.Background(), CommitRequest{
		Event: event.Input{Source: "test", Type: "run.mutated"},
		Mutate: func(candidate *Snapshot) error {
			candidate.Run.Status = domain.RunFailed
			return nil
		},
	})
	var commitErr *CommitError
	if !errors.As(err, &commitErr) || commitErr.Stage != CommitStageEvent {
		t.Fatalf("expected event CommitError, got %v", err)
	}
	if persistCalls != 0 {
		t.Fatal("snapshot must not persist after event failure")
	}
	if service.snapshot.Run.Status != before {
		t.Fatal("memory must not install candidate after event failure")
	}
	if service.AcceptingWork() {
		t.Fatal("expected fail-closed after event append failure")
	}
	select {
	case fatal := <-service.FatalPersistenceErrors():
		if fatal == nil {
			t.Fatal("expected fatal error")
		}
	case <-time.After(time.Second):
		t.Fatal("fatal channel did not receive error")
	}
}

func TestCommitSnapshotPersistFailureFailClosesAfterEvent(t *testing.T) {
	appender := &fakeEventAppender{}
	service := newCommitService(appender, func(Snapshot) error {
		return errors.New("state write failed")
	})
	before := service.snapshot.Run.Status

	_, err := service.Commit(context.Background(), CommitRequest{
		Event: event.Input{Source: "test", Type: "run.mutated"},
		Mutate: func(candidate *Snapshot) error {
			candidate.Run.Status = domain.RunFailed
			return nil
		},
	})
	var commitErr *CommitError
	if !errors.As(err, &commitErr) || commitErr.Stage != CommitStageSnapshot {
		t.Fatalf("expected snapshot CommitError, got %v", err)
	}
	if appender.len() != 1 {
		t.Fatalf("event should have been appended once, got %d", appender.len())
	}
	if service.snapshot.Run.Status != before {
		t.Fatal("memory must not install candidate after snapshot failure")
	}
	if service.AcceptingWork() {
		t.Fatal("expected fail-closed after snapshot persistence failure")
	}
	select {
	case <-service.FatalPersistenceErrors():
	case <-time.After(time.Second):
		t.Fatal("fatal channel did not receive error")
	}
}

func TestCommitSuccessInstallsCandidate(t *testing.T) {
	appender := &fakeEventAppender{}
	persistCalls := 0
	var persisted Snapshot
	service := newCommitService(appender, func(value Snapshot) error {
		persistCalls++
		persisted = value
		return nil
	})

	got, err := service.Commit(context.Background(), CommitRequest{
		Event: event.Input{Source: "supervisor", Type: "run.completed", Severity: "info"},
		Mutate: func(candidate *Snapshot) error {
			candidate.Run.Status = domain.RunCompleted
			candidate.LastError = ""
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != "run.completed" || got.Seq != 1 {
		t.Fatalf("unexpected event: %+v", got)
	}
	if appender.len() != 1 || persistCalls != 1 {
		t.Fatalf("expected one event and one persist, got appends=%d persists=%d", appender.len(), persistCalls)
	}
	if service.snapshot.Run.Status != domain.RunCompleted {
		t.Fatalf("memory was not installed: %s", service.snapshot.Run.Status)
	}
	if persisted.Run.Status != domain.RunCompleted {
		t.Fatalf("persisted snapshot mismatch: %+v", persisted)
	}
	if !service.AcceptingWork() {
		t.Fatal("successful commit must keep accepting work")
	}
}

func TestCommitMutateDoesNotAliasNestedSlices(t *testing.T) {
	appender := &fakeEventAppender{}
	service := newCommitService(appender, func(Snapshot) error { return nil })
	originalScope := service.snapshot.Tasks[0].Task.WriteScope

	_, err := service.Commit(context.Background(), CommitRequest{
		Event: event.Input{Source: "test", Type: "scope.expanded"},
		Mutate: func(candidate *Snapshot) error {
			candidate.Tasks[0].Task.WriteScope = append(candidate.Tasks[0].Task.WriteScope, "go.mod")
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(originalScope) != 1 || originalScope[0] != "internal/a/**" {
		t.Fatalf("before snapshot nested slice was mutated in place: %v", originalScope)
	}
	if len(service.snapshot.Tasks[0].Task.WriteScope) != 2 {
		t.Fatalf("installed candidate missing expansion: %v", service.snapshot.Tasks[0].Task.WriteScope)
	}
}

func TestCommitErrorExposesStageViaErrorsAs(t *testing.T) {
	service := newCommitService(&fakeEventAppender{}, func(Snapshot) error { return nil })
	_, err := service.Commit(context.Background(), CommitRequest{
		Event:  event.Input{Source: "test", Type: "x"},
		Mutate: func(*Snapshot) error { return errors.New("nope") },
	})
	var commitErr *CommitError
	if !errors.As(err, &commitErr) {
		t.Fatalf("errors.As failed for %v", err)
	}
	if commitErr.Stage != CommitStageValidate {
		t.Fatalf("stage=%s", commitErr.Stage)
	}
	if !errors.Is(err, commitErr.Err) {
		t.Fatalf("unwrap mismatch: %v", err)
	}
}

func TestCommitIllegalTransitionDoesNotAppend(t *testing.T) {
	// Covered via transitionTask validator: mutate that fails validation never reaches append.
	appender := &fakeEventAppender{}
	service := newCommitService(appender, func(Snapshot) error { return nil })
	service.snapshot.Tasks = []TaskState{{
		Task: domain.Task{TaskID: "task-a", Status: state.TaskFailed},
	}}
	err := service.transitionTask(&service.snapshot.Tasks[0], state.TaskRunning)
	if err == nil {
		t.Fatal("expected illegal transition")
	}
	if appender.len() != 0 {
		t.Fatal("illegal transition must not append")
	}
	if service.snapshot.Tasks[0].Task.Status != state.TaskFailed {
		t.Fatal("memory must remain failed")
	}
}

func TestCommitConcurrentTaskUpdatesDoNotClobber(t *testing.T) {
	appender := &fakeEventAppender{}
	service := newCommitService(appender, func(Snapshot) error { return nil })
	service.snapshot.Tasks = []TaskState{
		{Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning, Title: "A"}},
		{Task: domain.Task{TaskID: "task-b", Status: state.TaskRunning, Title: "B"}},
	}
	if err := service.transitionTask(&TaskState{Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning}}, state.TaskReportedComplete); err != nil {
		t.Fatal(err)
	}
	if err := service.transitionTask(&TaskState{Task: domain.Task{TaskID: "task-b", Status: state.TaskRunning}}, state.TaskReportedComplete); err != nil {
		t.Fatal(err)
	}
	snap := service.Snapshot()
	if snap.Tasks[0].Task.Status != state.TaskReportedComplete || snap.Tasks[1].Task.Status != state.TaskReportedComplete {
		t.Fatalf("tasks clobbered: %+v", snap.Tasks)
	}
	if snap.Tasks[0].Task.Title != "A" || snap.Tasks[1].Task.Title != "B" {
		t.Fatalf("task fields clobbered: %+v", snap.Tasks)
	}
}

func TestCommitRejectedAfterFailClosed(t *testing.T) {
	appender := &fakeEventAppender{err: errors.New("event boom")}
	service := newCommitService(appender, func(Snapshot) error { return nil })
	_, _ = service.Commit(context.Background(), CommitRequest{
		Event:  event.Input{Source: "test", Type: "first"},
		Mutate: func(*Snapshot) error { return nil },
	})
	appender.err = nil
	_, err := service.Commit(context.Background(), CommitRequest{
		Event:  event.Input{Source: "test", Type: "second"},
		Mutate: func(candidate *Snapshot) error { candidate.LastError = "should not apply"; return nil },
	})
	var commitErr *CommitError
	if !errors.As(err, &commitErr) || commitErr.Stage != CommitStageValidate {
		t.Fatalf("expected reject after fail-closed, got %v", err)
	}
	if service.snapshot.LastError == "should not apply" {
		t.Fatal("rejected commit must not mutate memory")
	}
	if appender.len() != 0 {
		t.Fatalf("rejected commit must not append, got %d", appender.len())
	}
}
