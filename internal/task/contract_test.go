package task

import (
	"strings"
	"testing"

	"github.com/vnai/subagent-broker/internal/domain"
)

func TestRenderContractIncludesSafetyRules(t *testing.T) {
	contract := domain.Task{
		TaskID: "task-auth", Title: "Auth", Objective: "Implement auth", CompletionCriteria: []string{"tests pass"},
		WriteScope: []string{"internal/auth/**"}, ForbiddenScope: []string{"go.mod"},
		ValidationCommands: []domain.ValidationCommand{{Command: "go test ./internal/auth/..."}}, ProjectRoot: "/repo",
	}
	text, err := RenderContract(contract, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"scope expansion request", "Do not use `git reset --hard`", "Do not create or invoke nested subagents"} {
		if !strings.Contains(text, required) {
			t.Fatalf("contract missing %q", required)
		}
	}
}
