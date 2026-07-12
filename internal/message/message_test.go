package message

import (
	"os"
	"path/filepath"
	"testing"
)

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
