package message

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/storage"
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
	value.Resolution = json.RawMessage(`{"answer":"yes"}`)
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

func TestReplayDoesNotSilentlyIgnoreIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := NewStore(path)
	now := time.Now().UTC()
	value := Message{SchemaVersion: "v1alpha1", MessageID: "m1", RunID: "r1", Type: Question, Status: Queued, CreatedAt: now, UpdatedAt: now, Payload: json.RawMessage(`{}`)}
	if err := store.Append(value); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(mustRead(t, path), []byte(`{"message_id":"m2"`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := ReplayDetailed(path)
	if err != nil {
		t.Fatal(err)
	}
	if !result.TailRepaired || result.QuarantinePath == "" {
		t.Fatalf("expected incomplete tail repair, got %+v", result)
	}
	if result.Messages["m1"].MessageID != "m1" {
		t.Fatalf("expected complete message retained: %+v", result.Messages)
	}
	if _, ok := result.Messages["m2"]; ok {
		t.Fatal("incomplete message must not be treated as a complete record")
	}
	// Unrepaired incomplete tail is rejected by the storage layer.
	broken := filepath.Join(t.TempDir(), "broken.jsonl")
	if err := os.WriteFile(broken, []byte(`{"message_id":"m1","run_id":"r1","type":"question","status":"queued"}`+"\n"+`{"message_id":"m2"`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = storage.ReplayJSONL(broken, storage.JSONLReplayOptions{RepairIncompleteTail: false}, nil, nil)
	if !errors.Is(err, storage.ErrIncompleteJSONLTail) {
		t.Fatalf("expected ErrIncompleteJSONLTail without repair, got %v", err)
	}
}

func TestReplayRejectsMessageIdentityDrift(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	now := time.Now().UTC()
	first := Message{SchemaVersion: "v1alpha1", MessageID: "m1", RunID: "r1", TaskID: "t1", Type: Question, Status: Queued, CreatedAt: now, UpdatedAt: now, Payload: json.RawMessage(`{}`)}
	second := Message{SchemaVersion: "v1alpha1", MessageID: "m1", RunID: "r1", TaskID: "t2", Type: Question, Status: Answered, CreatedAt: now, UpdatedAt: now.Add(time.Second), Payload: json.RawMessage(`{}`)}
	writeMessages(t, path, first, second)
	_, err := Replay(path)
	if err == nil {
		t.Fatal("expected identity drift error")
	}
}

func writeMessages(t *testing.T, path string, values ...Message) {
	t.Helper()
	store := NewStore(path)
	for _, value := range values {
		if err := store.Append(value); err != nil {
			t.Fatal(err)
		}
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
