package message

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStatusMachineHelpers(t *testing.T) {
	cases := []struct {
		from, to Status
		ok       bool
	}{
		{Created, Validated, true},
		{Validated, Queued, true},
		{Queued, Delivered, true},
		{Delivered, Answered, true},
		{Answered, Queued, false},
		{Failed, Delivered, false},
		{Expired, Failed, false},
		{Queued, Queued, true},
	}
	for _, tc := range cases {
		err := ValidateTransition(tc.from, tc.to)
		if tc.ok && err != nil {
			t.Fatalf("%s -> %s: %v", tc.from, tc.to, err)
		}
		if !tc.ok && err == nil {
			t.Fatalf("%s -> %s should fail", tc.from, tc.to)
		}
	}
	if !IsTerminal(Expired) || IsPending(Failed) || !IsPending(Acknowledged) {
		t.Fatal("IsTerminal/IsPending mismatch")
	}
}

func TestQuestionPublicationIsValidated(t *testing.T) {
	dir := t.TempDir()
	invalid := QuestionEnvelope{SchemaVersion: "v1alpha1"}
	if err := PublishQuestion(dir, invalid); err == nil {
		t.Fatal("invalid question should fail")
	}
	if _, err := os.Stat(filepath.Join(dir, "question.md")); !os.IsNotExist(err) {
		t.Fatal("question.md must not exist after validation failure")
	}
	valid := QuestionEnvelope{SchemaVersion: "v1alpha1", Question: "May I change go.mod?", Reason: "new dependency required", CurrentScope: []string{"internal/a/**"}, RequestedScope: []string{"go.mod", "go.sum"}, WorkspaceState: "No out-of-scope edits made"}
	if err := PublishQuestion(dir, valid); err != nil {
		t.Fatal(err)
	}
}
