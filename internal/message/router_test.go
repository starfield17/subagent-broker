package message

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestValidateTransitionAndTerminal(t *testing.T) {
	if err := ValidateTransition(Queued, Delivered); err != nil {
		t.Fatal(err)
	}
	if err := ValidateTransition(Answered, Queued); err == nil {
		t.Fatal("terminal must not return to queued")
	}
	if err := ValidateTransition(Failed, Queued); err == nil {
		t.Fatal("failed must not return to queued")
	}
	if err := ValidateTransition(Queued, Queued); err != nil {
		t.Fatal(err)
	}
	if !IsTerminal(Answered) || IsPending(Answered) || !IsPending(Queued) {
		t.Fatal("terminal/pending helpers mismatch")
	}
}

func TestEnqueuePersistsBeforeMemory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := NewStore(path)
	router, err := NewRouter(NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	value, err := router.EnqueueInstruction("task-a", "worker-a", "do it", DeliveryImmediate)
	if err != nil {
		t.Fatal(err)
	}
	if value.Status != Queued || value.MessageID == "" {
		t.Fatalf("unexpected message: %+v", value)
	}
	// Journal must already contain the queued record.
	replayed, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if replayed[value.MessageID].Status != Queued {
		t.Fatalf("journal missing queued message: %+v", replayed)
	}
	got, ok := router.Get(value.MessageID)
	if !ok || got.Status != Queued {
		t.Fatalf("memory missing queued message: ok=%v got=%+v", ok, got)
	}
}

func TestEnqueueAppendFailureLeavesMemoryEmpty(t *testing.T) {
	dir := t.TempDir()
	blocked := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(blocked, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewStore(filepath.Join(blocked, "messages.jsonl"))
	router, err := NewRouter(NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.EnqueueInstruction("task-a", "", "hello", DeliveryImmediate); err == nil {
		t.Fatal("expected append failure")
	}
	if len(router.Snapshot(true)) != 0 {
		t.Fatal("memory must stay empty when append fails")
	}
}

func TestTransitionRejectsIllegalAndDoesNotAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := NewStore(path)
	router, err := NewRouter(NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	value, err := router.EnqueueInstruction("task-a", "", "hello", DeliveryImmediate)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Transition(value.MessageID, Created, "", nil, nil); err == nil {
		t.Fatal("expected illegal transition error")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("illegal transition must not append")
	}
	got, _ := router.Get(value.MessageID)
	if got.Status != Queued {
		t.Fatalf("memory changed after illegal transition: %+v", got)
	}
}

func TestTerminalCannotReturnToQueued(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	router, err := NewRouter(NewRouterOptions{RunID: "run-1", Store: NewStore(path)})
	if err != nil {
		t.Fatal(err)
	}
	value, err := router.EnqueueInstruction("task-a", "", "hello", DeliveryImmediate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Transition(value.MessageID, Failed, DeliveryImmediate, nil, errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	if _, err := router.Transition(value.MessageID, Queued, "", nil, nil); err == nil {
		t.Fatal("expected terminal -> queued rejection")
	}
}

func TestPendingInstructionsStableOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	clock := time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC)
	seq := 0
	router, err := NewRouter(NewRouterOptions{
		RunID: "run-1",
		Store: NewStore(path),
		Now: func() time.Time {
			return clock
		},
		NewID: func(now time.Time) (string, error) {
			seq++
			// Reverse lexical order vs creation sequence to prove secondary key.
			return []string{"msg-c", "msg-a", "msg-b"}[seq-1], nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.EnqueueInstruction("task-a", "", "one", DeliveryNextTurn); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(time.Second)
	if _, err := router.EnqueueInstruction("task-a", "", "two", DeliveryResume); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(time.Second)
	if _, err := router.EnqueueInstruction("task-a", "", "three", DeliveryNextTurn); err != nil {
		t.Fatal(err)
	}
	// Same timestamp branch: force two ids at equal time via direct index is unnecessary;
	// CreatedAt order should already be stable.
	pending := router.PendingInstructions("task-a", DeliveryNextTurn)
	if len(pending) != 2 {
		t.Fatalf("pending=%+v", pending)
	}
	if pending[0].MessageID != "msg-c" || pending[1].MessageID != "msg-b" {
		t.Fatalf("order=%v %v", pending[0].MessageID, pending[1].MessageID)
	}
	// Equal CreatedAt secondary sort by MessageID.
	sameTimePath := filepath.Join(t.TempDir(), "same.jsonl")
	seq = 0
	sameClock := time.Date(2026, 7, 12, 1, 0, 0, 0, time.UTC)
	same, err := NewRouter(NewRouterOptions{
		RunID: "run-1",
		Store: NewStore(sameTimePath),
		Now:   func() time.Time { return sameClock },
		NewID: func(time.Time) (string, error) {
			seq++
			return []string{"msg-z", "msg-m", "msg-a"}[seq-1], nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := same.EnqueueInstruction("task-a", "", "x", DeliveryImmediate); err != nil {
			t.Fatal(err)
		}
	}
	ordered := same.PendingInstructions("task-a")
	if ordered[0].MessageID != "msg-a" || ordered[1].MessageID != "msg-m" || ordered[2].MessageID != "msg-z" {
		t.Fatalf("equal-time order: %v %v %v", ordered[0].MessageID, ordered[1].MessageID, ordered[2].MessageID)
	}
}

func TestExpireTaskOnlyPending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	router, err := NewRouter(NewRouterOptions{RunID: "run-1", Store: NewStore(path)})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := router.EnqueueInstruction("task-a", "", "keep-expiring", DeliveryNextTurn)
	if err != nil {
		t.Fatal(err)
	}
	answered, err := router.EnqueueInstruction("task-a", "", "done", DeliveryImmediate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Transition(answered.MessageID, Delivered, DeliveryImmediate, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := router.Transition(answered.MessageID, Answered, DeliveryImmediate, json.RawMessage(`{"ok":true}`), nil); err != nil {
		t.Fatal(err)
	}
	failed, err := router.EnqueueInstruction("task-a", "", "failed", DeliveryImmediate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Transition(failed.MessageID, Failed, DeliveryImmediate, nil, errors.New("nope")); err != nil {
		t.Fatal(err)
	}
	other, err := router.EnqueueInstruction("task-b", "", "other", DeliveryResume)
	if err != nil {
		t.Fatal(err)
	}

	expired, err := router.ExpireTask("task-a", "task ended")
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 || expired[0].MessageID != queued.MessageID || expired[0].Status != Expired {
		t.Fatalf("expired=%+v", expired)
	}
	if got, _ := router.Get(answered.MessageID); got.Status != Answered {
		t.Fatalf("answered changed: %+v", got)
	}
	if got, _ := router.Get(failed.MessageID); got.Status != Failed {
		t.Fatalf("failed changed: %+v", got)
	}
	if got, _ := router.Get(other.MessageID); got.Status != Queued {
		t.Fatalf("other task changed: %+v", got)
	}
}

func TestTransitionAppendFailureKeepsMemory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "messages.jsonl")
	store := NewStore(path)
	router, err := NewRouter(NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	value, err := router.EnqueueInstruction("task-a", "", "hello", DeliveryImmediate)
	if err != nil {
		t.Fatal(err)
	}
	// Break further appends by replacing the journal path's parent with a file after first write.
	// Use a new router bound to a broken store while seeding memory via Initial.
	blocked := filepath.Join(dir, "blocked")
	if err := os.WriteFile(blocked, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	broken, err := NewRouter(NewRouterOptions{
		RunID:   "run-1",
		Store:   NewStore(filepath.Join(blocked, "messages.jsonl")),
		Initial: map[string]Message{value.MessageID: value},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := broken.Transition(value.MessageID, Delivered, DeliveryImmediate, nil, nil); err == nil {
		t.Fatal("expected append failure")
	}
	got, ok := broken.Get(value.MessageID)
	if !ok || got.Status != Queued {
		t.Fatalf("memory must remain queued: ok=%v got=%+v", ok, got)
	}
}
