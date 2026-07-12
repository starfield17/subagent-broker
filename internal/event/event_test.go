package event

import (
	"os"
	"path/filepath"
	"testing"
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

func TestReplayIgnoresIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	store := NewStore(path, "run-1", 0)
	if _, err := store.Append(Input{Source: "fake", Type: "run.started"}); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString(`{"schema_version":"v1alpha1"`)
	_ = file.Close()
	result, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if !result.IncompleteTail || len(result.Events) != 1 {
		t.Fatalf("unexpected replay result: %+v", result)
	}
}
