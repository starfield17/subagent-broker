package message

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreReplaysLatestMessageStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := NewStore(path)
	now := time.Now().UTC()
	value := Message{SchemaVersion: "v1alpha1", MessageID: "m1", RunID: "r1", Type: Question, Status: Queued, CreatedAt: now, UpdatedAt: now, Payload: json.RawMessage(`{}`)}
	if err := store.Append(value); err != nil {
		t.Fatal(err)
	}
	value.Status = Answered
	value.UpdatedAt = now.Add(time.Second)
	if err := store.Append(value); err != nil {
		t.Fatal(err)
	}
	replayed, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if replayed["m1"].Status != Answered {
		t.Fatalf("unexpected replay: %+v", replayed)
	}
}
