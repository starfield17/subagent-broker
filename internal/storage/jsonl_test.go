package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReplayJSONLMissingAndEmpty(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.jsonl")
	result, err := ReplayJSONL(missing, JSONLReplayOptions{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.RecordCount != 0 || result.IncompleteTail || result.Repaired {
		t.Fatalf("unexpected missing result: %+v", result)
	}

	empty := filepath.Join(dir, "empty.jsonl")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	result, err = ReplayJSONL(empty, JSONLReplayOptions{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.RecordCount != 0 || result.IncompleteTail {
		t.Fatalf("unexpected empty result: %+v", result)
	}
}

func TestReplayJSONLTwoValidRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ok.jsonl")
	content := []byte("{\"id\":1}\n{\"id\":2}\n")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	var ids []int
	result, err := ReplayJSONL(path, JSONLReplayOptions{}, func(line []byte, _ int) error {
		var value struct {
			ID int `json:"id"`
		}
		if err := json.Unmarshal(line, &value); err != nil {
			return err
		}
		return nil
	}, func(line []byte, _ int) error {
		var value struct {
			ID int `json:"id"`
		}
		if err := json.Unmarshal(line, &value); err != nil {
			return err
		}
		ids = append(ids, value.ID)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RecordCount != 2 || result.LastValidOffset != int64(len(content)) {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("unexpected ids: %v", ids)
	}
}

func TestReplayJSONLIncompleteTailWithoutRepair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial.jsonl")
	original := []byte("{\"id\":1}\n{\"id\":2")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := ReplayJSONL(path, JSONLReplayOptions{RepairIncompleteTail: false}, nil, nil)
	if !errors.Is(err, ErrIncompleteJSONLTail) {
		t.Fatalf("expected ErrIncompleteJSONLTail, got %v", err)
	}
	if !result.IncompleteTail || result.RecordCount != 1 || result.Repaired {
		t.Fatalf("unexpected result: %+v", result)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("file was modified without repair: %q", got)
	}
}

func TestReplayJSONLRepairsIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial.jsonl")
	tail := []byte(`{"id":2,"broken":true`)
	original := append([]byte("{\"id\":1}\n"), tail...)
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	fixed := time.Date(2026, 7, 12, 15, 4, 5, 0, time.UTC)
	result, err := ReplayJSONL(path, JSONLReplayOptions{
		RepairIncompleteTail: true,
		Now:                  func() time.Time { return fixed },
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.IncompleteTail || !result.Repaired || result.RecordCount != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	wantQuarantine := path + ".corrupt-tail.20260712T150405.000Z"
	if result.QuarantinePath != wantQuarantine {
		t.Fatalf("quarantine path=%q want=%q", result.QuarantinePath, wantQuarantine)
	}
	quarantine, err := os.ReadFile(result.QuarantinePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(quarantine, tail) {
		t.Fatalf("quarantine=%q want=%q", quarantine, tail)
	}
	repaired, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(repaired, []byte("{\"id\":1}\n")) {
		t.Fatalf("repaired log=%q", repaired)
	}
}

func TestAppendJSONLAfterRepair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.jsonl")
	if err := os.WriteFile(path, []byte("{\"id\":1}\n{\"partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReplayJSONL(path, JSONLReplayOptions{RepairIncompleteTail: true}, nil, nil); err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(map[string]int{"id": 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := AppendJSONL(path, second, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("{\"id\":1}\n{\"id\":2}\n")) {
		t.Fatalf("unexpected content after append: %q", got)
	}
}

func TestReplayJSONLCorruptCompleteLineLeavesFileUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.jsonl")
	original := []byte("{\"id\":1}\n{not-json}\n{\"id\":3}\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := ReplayJSONL(path, JSONLReplayOptions{RepairIncompleteTail: true}, func(line []byte, _ int) error {
		var value map[string]any
		return json.Unmarshal(line, &value)
	}, nil)
	if err == nil {
		t.Fatal("expected validation error for corrupt complete line")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("corrupt complete line must not modify file: %q", got)
	}
}

func TestAppendJSONLRejectsIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken.jsonl")
	if err := os.WriteFile(path, []byte("{\"id\":1}\n{\"partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := AppendJSONL(path, []byte(`{"id":2}`), 0o600)
	if !errors.Is(err, ErrIncompleteJSONLTail) {
		t.Fatalf("expected ErrIncompleteJSONLTail, got %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("{\"id\":1}\n{\"partial")) {
		t.Fatalf("append must not write after incomplete tail: %q", got)
	}
}
