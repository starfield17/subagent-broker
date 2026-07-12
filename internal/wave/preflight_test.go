package wave

import (
	"testing"

	"github.com/vnai/subagent-broker/internal/domain"
)

func baseTask(id, scopePattern string) domain.Task {
	return domain.Task{
		TaskID: domain.TaskID(id), Title: id, Objective: "implement " + id,
		CompletionCriteria: []string{"tests pass"}, WriteScope: []string{scopePattern},
		ValidationCommands: []domain.ValidationCommand{{Command: "go test ./internal/...", Scope: "local"}},
		ProjectRoot:        "/repo",
	}
}

func TestPreflightRejectsOverlap(t *testing.T) {
	result := Preflight([]domain.Task{baseTask("auth", "internal/auth/**"), baseTask("token", "internal/auth/token.go")})
	if result.Allowed {
		t.Fatal("overlapping scopes must be rejected")
	}
}

func TestPreflightRejectsSameWaveDependency(t *testing.T) {
	a := baseTask("a", "internal/a/**")
	b := baseTask("b", "internal/b/**")
	b.DependsOn = []domain.TaskID{"a"}
	if Preflight([]domain.Task{a, b}).Allowed {
		t.Fatal("same-wave output dependency must be rejected")
	}
}

func TestPreflightAllowsIndependentTasks(t *testing.T) {
	result := Preflight([]domain.Task{baseTask("auth", "internal/auth/**"), baseTask("cache", "internal/cache/**")})
	if !result.Allowed {
		t.Fatalf("independent tasks should pass: %+v", result.Issues)
	}
}

func TestPreflightRejectsWriteReadDependency(t *testing.T) {
	writer := baseTask("api", "internal/api/**")
	reader := baseTask("tests", "tests/**")
	reader.KnownReadDependencies = []string{"internal/api/**"}
	result := Preflight([]domain.Task{writer, reader})
	if result.Allowed {
		t.Fatal("write-read dependency must be rejected")
	}
}

func TestPreflightRejectsRepositoryWideValidation(t *testing.T) {
	task := baseTask("auth", "internal/auth/**")
	task.ValidationCommands = []domain.ValidationCommand{{Command: "go test ./...", Scope: "repository"}}
	if Preflight([]domain.Task{task}).Allowed {
		t.Fatal("repository-wide validation is unsafe during a Wave")
	}
}

func TestPreflightRejectsIncompleteContract(t *testing.T) {
	task := baseTask("auth", "internal/auth/**")
	task.ProjectRoot = ""
	task.ValidationCommands = nil
	result := Preflight([]domain.Task{task})
	if result.Allowed || len(result.Issues) == 0 {
		t.Fatalf("incomplete contract must be rejected: %+v", result)
	}
}
