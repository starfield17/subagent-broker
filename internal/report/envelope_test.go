package report

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func validSuccess() Envelope {
	return Envelope{SchemaVersion: SchemaVersion, TaskID: "task-a", WorkerID: "worker-a", Status: StatusSucceeded, Summary: "implemented", WorkCompleted: []string{"implemented feature"}, FilesChanged: []string{"internal/a/a.go"}, Validation: []Validation{{Command: "go test ./internal/a", Passed: true}}}
}

func TestPublishCreatesFormalReportOnlyAfterValidation(t *testing.T) {
	dir := t.TempDir()
	invalid := validSuccess()
	invalid.Summary = ""
	if err := Publish(dir, invalid, time.Unix(0, 0)); err == nil {
		t.Fatal("invalid envelope should fail")
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); !os.IsNotExist(err) {
		t.Fatal("formal report must not exist after validation failure")
	}
	if err := Publish(dir, validSuccess(), time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatalf("formal report should exist: %v", err)
	}
}

func TestSucceededIsNotAllowedToHideRemainingWork(t *testing.T) {
	e := validSuccess()
	e.RemainingWork = []string{"integration"}
	if err := ValidateEnvelope(e); err == nil {
		t.Fatal("succeeded with remaining work must be rejected")
	}
}
