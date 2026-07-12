package event

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vnai/subagent-broker/internal/storage"
)

func TestAppendUsesMonotonicRunSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	store := NewStore(path, "run-1", 0)
	first, err := store.Append(Input{Source: "fake", Type: "run.started", Payload: map[string]bool{"ok": true}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Append(Input{Source: "fake", Type: "task.reported_complete"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Seq != 1 || second.Seq != 2 {
		t.Fatalf("unexpected seqs: %d, %d", first.Seq, second.Seq)
	}
	replayed, err := Replay(path)
	if err != nil || len(replayed.Events) != 2 {
		t.Fatalf("replay: %+v err=%v", replayed, err)
	}
}

func TestReplayRepairsIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	store := NewStore(path, "run-1", 0)
	if _, err := store.Append(Input{Source: "fake", Type: "run.started"}); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(`{"schema_version":"v1alpha1"`); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if !result.IncompleteTail || !result.TailRepaired || result.QuarantinePath == "" || len(result.Events) != 1 {
		t.Fatalf("unexpected replay result: %+v", result)
	}
	if _, err := store.Append(Input{Source: "fake", Type: "run.completed"}); err != nil {
		t.Fatal(err)
	}
	replayed, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed.Events) != 2 {
		t.Fatalf("expected append after repair to succeed, got %d events", len(replayed.Events))
	}
}

func TestReplayRejectsNonMonotonicSeq(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	first := Event{SchemaVersion: SchemaVersion, Seq: 2, EventID: "e1", RunID: "run-1", Source: "fake", Type: "run.started"}
	second := Event{SchemaVersion: SchemaVersion, Seq: 2, EventID: "e2", RunID: "run-1", Source: "fake", Type: "run.completed"}
	writeEvents(t, path, first, second)
	_, err := Replay(path)
	if err == nil {
		t.Fatal("expected non-monotonic seq error")
	}
}

func TestReplayRejectsSeqRegression(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	first := Event{SchemaVersion: SchemaVersion, Seq: 3, EventID: "e1", RunID: "run-1", Source: "fake", Type: "run.started"}
	second := Event{SchemaVersion: SchemaVersion, Seq: 1, EventID: "e2", RunID: "run-1", Source: "fake", Type: "run.completed"}
	writeEvents(t, path, first, second)
	_, err := Replay(path)
	if err == nil {
		t.Fatal("expected seq regression error")
	}
}

func TestReplayRejectsRunIDChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	first := Event{SchemaVersion: SchemaVersion, Seq: 1, EventID: "e1", RunID: "run-1", Source: "fake", Type: "run.started"}
	second := Event{SchemaVersion: SchemaVersion, Seq: 2, EventID: "e2", RunID: "run-2", Source: "fake", Type: "run.completed"}
	writeEvents(t, path, first, second)
	_, err := Replay(path)
	if err == nil {
		t.Fatal("expected run_id change error")
	}
}

func TestAppendRefusesUnrepairedIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	store := NewStore(path, "run-1", 0)
	if _, err := store.Append(Input{Source: "fake", Type: "run.started"}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(mustRead(t, path), []byte(`{"schema_version":"v1alpha1"`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := store.Append(Input{Source: "fake", Type: "run.completed"})
	if !errors.Is(err, storage.ErrIncompleteJSONLTail) {
		t.Fatalf("expected ErrIncompleteJSONLTail, got %v", err)
	}
}

func writeEvents(t *testing.T, path string, events ...Event) {
	t.Helper()
	for _, event := range events {
		line, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if err := storage.AppendJSONL(path, line, 0o600); err != nil {
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
