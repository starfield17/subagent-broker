package supervisor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/state"
)

func TestApplyEventRunStateChanged(t *testing.T) {
	snap := Snapshot{
		SchemaVersion: SchemaVersion,
		Run:           domain.Run{RunID: "run-1", Status: domain.RunStarting},
	}
	payload, _ := json.Marshal(map[string]any{"from": "starting", "to": "running", "reason": ""})
	ev := event.Event{Seq: 1, RunID: "run-1", Type: event.RunStateChanged, Source: "supervisor", Payload: payload}
	next, err := ApplyEvent(snap, ev)
	if err != nil {
		t.Fatal(err)
	}
	if next.Run.Status != domain.RunRunning || next.AppliedEventSeq != 1 {
		t.Fatalf("next=%+v", next)
	}
}

func TestApplyEventRejectsIllegalTaskTransition(t *testing.T) {
	snap := Snapshot{
		Run: domain.Run{RunID: "run-1", Status: domain.RunRunning},
		Tasks: []TaskState{{
			Task: domain.Task{TaskID: "task-a", Status: state.TaskFailed},
		}},
		AppliedEventSeq: 1,
	}
	payload, _ := json.Marshal(map[string]any{"from": "failed", "to": "running", "task_id": "task-a"})
	ev := event.Event{Seq: 2, RunID: "run-1", TaskID: "task-a", Type: event.TaskStateChanged, Source: "supervisor", Payload: payload}
	if _, err := ApplyEvent(snap, ev); err == nil {
		t.Fatal("expected illegal transition error")
	}
}

func TestApplyEventRejectsRunIDMismatchAndSeqGap(t *testing.T) {
	snap := Snapshot{Run: domain.Run{RunID: "run-1"}, AppliedEventSeq: 2}
	payload, _ := json.Marshal(map[string]any{"from": "running", "to": "completed"})
	if _, err := ApplyEvent(snap, event.Event{Seq: 3, RunID: "other", Type: event.RunStateChanged, Payload: payload}); err == nil {
		t.Fatal("expected run id mismatch")
	}
	if _, err := ApplyEvent(snap, event.Event{Seq: 4, RunID: "run-1", Type: event.RunStateChanged, Payload: payload}); err == nil {
		t.Fatal("expected seq gap")
	}
}

func TestApplyEventStallAssessmentAndClear(t *testing.T) {
	snapshot := Snapshot{
		Run:             domain.Run{RunID: "run-1"},
		Tasks:           []TaskState{{Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning}}},
		AppliedEventSeq: 1,
	}
	payload, _ := json.Marshal(map[string]any{
		"state": "suspected_stall", "confidence": "medium", "reason": "no protocol or output progress",
		"quiet_for": "95s", "evidence": []string{"process still alive"},
	})
	next, err := ApplyEvent(snapshot, event.Event{Seq: 2, RunID: "run-1", TaskID: "task-a", Type: event.WorkerStallAssessed, Payload: payload, Timestamp: time.Now().UTC()})
	if err != nil || next.Tasks[0].Stall == nil || next.Tasks[0].Stall.Confidence != "medium" {
		t.Fatalf("assessment=%+v err=%v", next.Tasks[0].Stall, err)
	}
	next, err = ApplyEvent(next, event.Event{Seq: 3, RunID: "run-1", TaskID: "task-a", Type: event.WorkerStallCleared, Payload: json.RawMessage(`{"reason":"progress resumed"}`)})
	if err != nil || next.Tasks[0].Stall != nil {
		t.Fatalf("clear assessment=%+v err=%v", next.Tasks[0].Stall, err)
	}
}

func TestReplayEventsCatchesUpAfterSnapshotFailure(t *testing.T) {
	// Snapshot never received the last event (seq=2) after event append succeeded.
	snap := Snapshot{
		SchemaVersion:   SchemaVersion,
		Run:             domain.Run{RunID: "run-1", Status: domain.RunRunning},
		AppliedEventSeq: 1,
		Tasks: []TaskState{{
			Task:       domain.Task{TaskID: "task-a", Status: state.TaskRunning},
			Dimensions: state.Dimensions{Task: state.TaskRunning, Process: state.ProcessAlive, Protocol: state.ProtocolThinking, Progress: state.ProgressActive},
		}},
	}
	p1, _ := json.Marshal(map[string]any{"from": "running", "to": "reported_complete", "task_id": "task-a"})
	p2, _ := json.Marshal(map[string]any{"from": "reported_complete", "to": "verifying", "task_id": "task-a"})
	events := []event.Event{
		{Seq: 1, RunID: "run-1", Type: "telemetry.noise", Source: "x"},
		{Seq: 2, RunID: "run-1", TaskID: "task-a", Type: event.TaskStateChanged, Source: "supervisor", Payload: p1, Timestamp: time.Now().UTC()},
		{Seq: 3, RunID: "run-1", TaskID: "task-a", Type: event.TaskStateChanged, Source: "supervisor", Payload: p2, Timestamp: time.Now().UTC()},
	}
	// First need seq 2 from reported_complete - but task is running, so from must be running
	p1, _ = json.Marshal(map[string]any{"from": "running", "to": "reported_complete", "task_id": "task-a"})
	events[1].Payload = p1
	next, err := ReplayEvents(snap, events)
	if err != nil {
		t.Fatal(err)
	}
	if next.AppliedEventSeq != 3 {
		t.Fatalf("applied=%d", next.AppliedEventSeq)
	}
	if next.Tasks[0].Task.Status != state.TaskVerifying {
		t.Fatalf("task status=%s", next.Tasks[0].Task.Status)
	}
}

func TestCommitSetsAppliedEventSeq(t *testing.T) {
	appender := &fakeEventAppender{}
	service := newCommitService(appender, func(Snapshot) error { return nil })
	got, err := service.Commit(context.Background(), CommitRequest{
		Event: event.Input{Source: "supervisor", Type: event.RunStateChanged, Payload: map[string]any{"from": "running", "to": "completed"}},
		Mutate: func(candidate *Snapshot) error {
			candidate.Run.Status = domain.RunCompleted
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Seq != 1 || service.snapshot.AppliedEventSeq != 1 {
		t.Fatalf("event=%+v applied=%d", got, service.snapshot.AppliedEventSeq)
	}
}
