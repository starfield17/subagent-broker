package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLayoutIsProjectFirstAndOutsideRepository(t *testing.T) {
	layout, err := NewLayout(filepath.Join(t.TempDir(), "broker"))
	if err != nil {
		t.Fatal(err)
	}
	runDir, err := layout.EnsureRun("repo--abc123", "run-1")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(layout.Home, "projects", "repo--abc123", "runs", "run-1")
	if runDir != want {
		t.Fatalf("run dir=%s want=%s", runDir, want)
	}
}

func TestAtomicWriteReplacesCompleteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := AtomicWriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := AtomicWriteFile(path, []byte("new-complete"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new-complete" {
		t.Fatalf("got %q", got)
	}
}

func TestLayoutExposesManualArtifactPaths(t *testing.T) {
	layout, err := NewLayout(filepath.Join(t.TempDir(), "broker"))
	if err != nil {
		t.Fatal(err)
	}
	paths, err := layout.TaskPaths("repo--abc", "run-1", "task-a")
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"contract": paths.Contract, "question": paths.Question, "report": paths.Report, "validation": paths.ValidationDir} {
		if value == "" {
			t.Fatalf("%s path is empty", name)
		}
	}
}
