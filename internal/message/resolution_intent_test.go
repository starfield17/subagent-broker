package message

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestRecordResolutionIntentIdempotentAndConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := NewStore(path)
	router, err := NewRouter(NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(PermissionRequestPayload{
		ToolName: "Bash", Input: json.RawMessage(`{}`),
		Harness: "fake", NativeSessionID: "s1", NativePermissionID: "p1",
	})
	msg, err := router.EnqueueDecisionWithAttempt("task-a", "w1", 1, PermissionRequest, Permission, payload)
	if err != nil {
		t.Fatal(err)
	}
	res, _ := json.Marshal(Resolution{Decision: DecisionPayload{Allowed: true, Reason: "ok"}})
	frozen, err := router.RecordResolutionIntent(msg.MessageID, res)
	if err != nil {
		t.Fatal(err)
	}
	if frozen.Status != Queued || len(frozen.Resolution) == 0 {
		t.Fatalf("%+v", frozen)
	}
	// Idempotent same decision.
	again, err := router.RecordResolutionIntent(msg.MessageID, res)
	if err != nil {
		t.Fatal(err)
	}
	if again.DeliveryAttempts != 0 {
		t.Fatal("intent must not increment attempts")
	}
	// Conflict.
	deny, _ := json.Marshal(Resolution{Decision: DecisionPayload{Allowed: false}})
	if _, err := router.RecordResolutionIntent(msg.MessageID, deny); err == nil {
		t.Fatal("expected conflict")
	}
	// Failed delivery keeps Queued + resolution.
	failed, err := router.RecordDeliveryAttempt(msg.MessageID, "", errTest)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != Queued || failed.DeliveryAttempts != 1 || failed.Error == "" {
		t.Fatalf("%+v", failed)
	}
	// Success clears error.
	ok, err := router.RecordDeliveryAttempt(msg.MessageID, Answered, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok.Status != Answered || ok.Error != "" || ok.DeliveryAttempts != 2 {
		t.Fatalf("%+v", ok)
	}
}

var errTest = errString("delivery failed")

type errString string

func (e errString) Error() string { return string(e) }
