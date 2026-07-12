package worker

import (
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
)

func sampleTask() domain.Task {
	return domain.Task{TaskID: "task-a", Title: "A"}
}

func TestNextNumberStartsAtOneAndIncrements(t *testing.T) {
	if got := NextNumber(nil); got != 1 {
		t.Fatalf("NextNumber(nil)=%d", got)
	}
	existing := []Attempt{{Number: 1}, {Number: 3}, {Number: 2}}
	if got := NextNumber(existing); got != 4 {
		t.Fatalf("NextNumber=%d", got)
	}
}

func TestBeginFreshAndFinish(t *testing.T) {
	now := time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC)
	attempt, err := Begin(sampleTask(), nil, AttemptFresh, domain.WorkerSession{WorkerID: "w1"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Number != 1 || attempt.Mode != AttemptFresh || attempt.Outcome != AttemptRunning {
		t.Fatalf("unexpected attempt: %+v", attempt)
	}
	if attempt.Worker.TaskID != "task-a" || attempt.Worker.Attempt != 1 || attempt.Worker.AttemptMode != string(AttemptFresh) {
		t.Fatalf("worker fields: %+v", attempt.Worker)
	}

	finished, err := Finish(attempt, AttemptExited, "ok", now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if finished.Outcome != AttemptExited || finished.Reason != "ok" || finished.EndedAt == nil {
		t.Fatalf("unexpected finished: %+v", finished)
	}
	if _, err := Finish(finished, AttemptExited, "again", now); err == nil {
		t.Fatal("expected error finishing an already finished attempt")
	}
}

func TestBeginFreshRejectsExistingHistory(t *testing.T) {
	now := time.Now().UTC()
	existing := []Attempt{{Number: 1, Outcome: AttemptExited, Worker: domain.WorkerSession{TaskID: "task-a"}}}
	if _, err := Begin(sampleTask(), existing, AttemptFresh, domain.WorkerSession{}, now); err == nil {
		t.Fatal("expected fresh to reject existing attempts")
	}
}

func TestBeginRecoveryResumeRequiresHistoryAndPreservesNativeSession(t *testing.T) {
	now := time.Now().UTC()
	if _, err := Begin(sampleTask(), nil, AttemptRecoveryResume, domain.WorkerSession{}, now); err == nil {
		t.Fatal("expected recovery_resume to require history")
	}
	existing := []Attempt{{
		Number:  1,
		Outcome: AttemptOrphaned,
		Worker:  domain.WorkerSession{TaskID: "task-a", NativeSessionID: "native-1"},
	}}
	attempt, err := Begin(sampleTask(), existing, AttemptRecoveryResume, domain.WorkerSession{WorkerID: "w2"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Number != 2 || attempt.Mode != AttemptRecoveryResume {
		t.Fatalf("unexpected attempt: %+v", attempt)
	}
	if attempt.Worker.NativeSessionID != "native-1" {
		t.Fatalf("native session was cleared: %+v", attempt.Worker)
	}
}

func TestBeginExplicitRetryRequiresHistory(t *testing.T) {
	now := time.Now().UTC()
	if _, err := Begin(sampleTask(), nil, AttemptExplicitRetry, domain.WorkerSession{}, now); err == nil {
		t.Fatal("expected explicit_retry to require history")
	}
	existing := []Attempt{{Number: 1, Outcome: AttemptFailedStart, Worker: domain.WorkerSession{TaskID: "task-a"}}}
	attempt, err := Begin(sampleTask(), existing, AttemptExplicitRetry, domain.WorkerSession{WorkerID: "w3"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Number != 2 || attempt.Mode != AttemptExplicitRetry {
		t.Fatalf("unexpected attempt: %+v", attempt)
	}
}

func TestBeginRejectsTaskIDMismatchAndMultipleActive(t *testing.T) {
	now := time.Now().UTC()
	if _, err := Begin(sampleTask(), nil, AttemptFresh, domain.WorkerSession{TaskID: "other"}, now); err == nil {
		t.Fatal("expected task id mismatch error")
	}

	running := []Attempt{{Number: 1, Outcome: AttemptRunning, Worker: domain.WorkerSession{TaskID: "task-a"}}}
	if _, err := Begin(sampleTask(), running, AttemptExplicitRetry, domain.WorkerSession{}, now); err == nil {
		t.Fatal("expected rejection when a running attempt exists")
	}

	if _, err := Active([]Attempt{
		{Number: 1, Outcome: AttemptRunning},
		{Number: 2, Outcome: AttemptRunning},
	}); err == nil {
		t.Fatal("expected Active to reject multiple running attempts")
	}
}

func TestActiveReturnsCopy(t *testing.T) {
	existing := []Attempt{{
		Number:  1,
		Outcome: AttemptRunning,
		Worker: domain.WorkerSession{
			TaskID:       "task-a",
			Capabilities: map[string]bool{"x": true},
		},
	}}
	active, err := Active(existing)
	if err != nil || active == nil {
		t.Fatalf("active=%v err=%v", active, err)
	}
	active.Worker.Capabilities["x"] = false
	active.Worker.TaskID = "mutated"
	if existing[0].Worker.Capabilities["x"] != true || existing[0].Worker.TaskID != "task-a" {
		t.Fatal("Active must return a defensive copy")
	}
}

func TestFinishRejectsRunningOutcome(t *testing.T) {
	attempt := Attempt{Number: 1, Outcome: AttemptRunning}
	if _, err := Finish(attempt, AttemptRunning, "", time.Now().UTC()); err == nil {
		t.Fatal("expected error")
	}
}
