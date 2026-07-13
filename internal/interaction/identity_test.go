package interaction

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveWorkerProcessIdentityFromEnv(t *testing.T) {
	runDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(runDir, "run.json"), []byte(`{"run_id":"run-1"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"BROKER_RUN_DIR":           runDir,
		"BROKER_RUN_ID":            "run-1",
		"BROKER_TASK_ID":           "task-a",
		"BROKER_WORKER_ID":         "worker-1",
		"BROKER_NATIVE_SESSION_ID": "sess-1",
	}
	id, err := ResolveWorkerProcessIdentity("", "", "", func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if id.RunDir != runDir || id.RunID != "run-1" || id.TaskID != "task-a" || id.WorkerID != "worker-1" || id.NativeSessionID != "sess-1" {
		t.Fatalf("%+v", id)
	}
}

func TestResolveWorkerProcessIdentityMismatchFailsClosed(t *testing.T) {
	runDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(runDir, "run.json"), []byte(`{"run_id":"run-1"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"BROKER_RUN_DIR":   runDir,
		"BROKER_RUN_ID":    "run-1",
		"BROKER_TASK_ID":   "task-a",
		"BROKER_WORKER_ID": "worker-1",
	}
	_, err := ResolveWorkerProcessIdentity(runDir, "task-OTHER", "", func(k string) string { return env[k] })
	if err == nil {
		t.Fatal("flag/env task mismatch must fail closed")
	}
	env["BROKER_RUN_ID"] = "run-OTHER"
	_, err = ResolveWorkerProcessIdentity(runDir, "task-a", "worker-1", func(k string) string { return env[k] })
	if err == nil {
		t.Fatal("BROKER_RUN_ID vs run.json mismatch must fail closed")
	}
}

func TestResolveWorkerProcessIdentityIncomplete(t *testing.T) {
	_, err := ResolveWorkerProcessIdentity("", "", "", func(string) string { return "" })
	if err == nil {
		t.Fatal("empty identity must fail")
	}
}

func TestLoadRunID(t *testing.T) {
	runDir := t.TempDir()
	raw, _ := json.Marshal(map[string]string{"run_id": "abc"})
	if err := os.WriteFile(filepath.Join(runDir, "run.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	id, err := LoadRunID(runDir)
	if err != nil || id != "abc" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}
