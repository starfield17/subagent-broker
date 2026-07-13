package protocol

import (
	"testing"

	"github.com/vnai/subagent-broker/internal/report"
)

func TestParseEnvelopeAcceptsJSONAndCodeFence(t *testing.T) {
	value := `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"succeeded","summary":"done","work_completed":["done"],"files_changed":[],"no_files_changed_reason":"fixture","validation":[{"command":"fixture","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]}`
	for _, raw := range []string{value, "```json\n" + value + "\n```"} {
		got, err := ParseEnvelope([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != report.StatusSucceeded || got.TaskID != "t" {
			t.Fatalf("unexpected envelope: %+v", got)
		}
	}
}

func TestParseEnvelopeNormalizesNativeCompletion(t *testing.T) {
	raw := `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"completed","summary":"done","completed_work":["done"],"changed_files":["file.txt"],"validation":[{"command":"go test ./...","status":"passed"}],"remaining_work":[],"blockers":[],"risks":[],"handoff_notes":"ready"}`
	got, err := ParseEnvelope([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != report.StatusSucceeded || len(got.WorkCompleted) != 1 || len(got.HandoffNotes) != 1 || !got.Validation[0].Passed {
		t.Fatalf("unexpected normalized envelope: %+v", got)
	}
}

func TestParseVersion(t *testing.T) {
	if got := ParseVersion([]byte("tool 1.2.3 (build)")); got != "1.2.3" {
		t.Fatalf("version = %q", got)
	}
}
