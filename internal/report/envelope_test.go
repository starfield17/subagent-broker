package report

import (
	"encoding/json"
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
	if err := Publish(dir, invalid, 1, time.Unix(0, 0)); err == nil {
		t.Fatal("invalid envelope should fail")
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); !os.IsNotExist(err) {
		t.Fatal("formal report must not exist after validation failure")
	}
	if err := Publish(dir, validSuccess(), 1, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatalf("formal report should exist: %v", err)
	}
}

func TestEnvelopeAcceptsTextValidationItems(t *testing.T) {
	var envelope Envelope
	data := []byte(`{"schema_version":"v1alpha1","task_id":"task-a","worker_id":"worker-a","status":"succeeded","summary":"implemented","work_completed":["implemented feature"],"files_changed":["internal/a/a.go"],"validation":["go test ./internal/a — PASSED"],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":["ready"]}`)
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if len(envelope.Validation) != 1 || !envelope.Validation[0].Passed {
		t.Fatalf("unexpected validation: %+v", envelope.Validation)
	}
}

func TestSucceededIsNotAllowedToHideRemainingWork(t *testing.T) {
	e := validSuccess()
	e.RemainingWork = []string{"integration"}
	if err := ValidateEnvelope(e); err == nil {
		t.Fatal("succeeded with remaining work must be rejected")
	}
}
