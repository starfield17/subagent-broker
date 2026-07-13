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
	for _, required := range []string{
		"scope expansion request", "Do not use `git reset --hard`", "Do not create or invoke nested subagents",
		`"schema_version"`, `"task_id"`, `"worker_id"`, `"status"`, `"summary"`, `"work_completed"`,
		`"files_changed"`, `"no_files_changed_reason"`, `"validation"`, `"validation_not_run_reason"`,
		`"remaining_work"`, `"blocking_issues"`, `"scope_expansion": null`, `"scope_violations_self_reported"`,
		`"risks"`, `"handoff_notes"`, "scope_expansion` must be `null`", "Never emit an array for this field",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("contract missing %q", required)
		}
	}
}
