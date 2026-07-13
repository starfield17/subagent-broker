package protocol

import (
	"encoding/json"
	"strings"
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
	raw := `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"completed","summary":"done","completed_work":["done"],"changed_files":["file.txt"],"validation":[{"command":"go test ./...","status":"passed"}],"remaining_work":[],"blockers":[],"scope_expansion":[],"risks":[],"handoff_notes":"ready"}`
	got, err := ParseEnvelope([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != report.StatusSucceeded || len(got.WorkCompleted) != 1 || len(got.HandoffNotes) != 1 || !got.Validation[0].Passed {
		t.Fatalf("unexpected normalized envelope: %+v", got)
	}
}

func TestParseEnvelopeNormalizesExternalScopeExpansionShapes(t *testing.T) {
	cases := []struct {
		name     string
		scope    string
		include  bool
		wantNil  bool
		wantErr  bool
		wantPath string
	}{
		{name: "omitted", wantNil: true},
		{name: "null", scope: "null", include: true, wantNil: true},
		{name: "empty array", scope: "[]", include: true, wantNil: true},
		{name: "empty object", scope: "{}", include: true, wantNil: true},
		{name: "valid object", scope: `{"paths":["outside/**"],"reason":"new file required","consequence":"task cannot continue"}`, include: true, wantPath: "outside/**"},
		{name: "non-empty array", scope: `[{"paths":["outside/**"]}]`, include: true, wantErr: true},
		{name: "string", scope: `"invalid"`, include: true, wantErr: true},
		{name: "number", scope: `42`, include: true, wantErr: true},
		{name: "boolean", scope: `true`, include: true, wantErr: true},
		{name: "partial object", scope: `{"paths":["outside/**"],"reason":""}`, include: true, wantErr: true},
		{name: "whitespace path", scope: `{"paths":["  "],"reason":"why","consequence":"what"}`, include: true, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseEnvelope(testEnvelopeWithScope(tc.scope, tc.include))
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected parse error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantNil && got.ScopeExpansion != nil {
				t.Fatalf("scope expansion = %+v, want nil", got.ScopeExpansion)
			}
			if tc.wantPath == "" {
				return
			}
			if got.ScopeExpansion == nil || len(got.ScopeExpansion.Paths) != 1 || got.ScopeExpansion.Paths[0] != tc.wantPath {
				t.Fatalf("scope expansion = %+v", got.ScopeExpansion)
			}
		})
	}
}

func TestParseEnvelopeRealWorldEmptyScopeArrayIsCanonicalized(t *testing.T) {
	// This is the native completion shape that previously failed when the
	// external harness emitted an empty array for a no-expansion result.
	got, err := ParseEnvelope(testEnvelopeWithScope("[]", true))
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"scope_expansion":[]`) || strings.Contains(string(data), `"scope_expansion":[`) {
		t.Fatalf("normalized envelope emitted array: %s", data)
	}
}

func TestParseEnvelopeRejectsMalformedJSON(t *testing.T) {
	if _, err := ParseEnvelope([]byte(`{"schema_version":"v1alpha1"`)); err == nil {
		t.Fatal("expected malformed JSON error")
	}
}

func testEnvelopeWithScope(scope string, include bool) []byte {
	field := ""
	if include {
		field = `,"scope_expansion":` + scope
	}
	return []byte(`{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"succeeded","summary":"done","work_completed":["done"],"files_changed":[],"no_files_changed_reason":"fixture","validation":[{"command":"fixture","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]` + field + `}`)
}

func TestParseVersion(t *testing.T) {
	if got := ParseVersion([]byte("tool 1.2.3 (build)")); got != "1.2.3" {
		t.Fatalf("version = %q", got)
	}
}
