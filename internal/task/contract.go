package task

import (
	"fmt"
	"sort"
	"strings"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/scope"
)

func ValidateContract(task domain.Task) error {
	var problems []string
	if task.TaskID == "" {
		problems = append(problems, "task_id is required")
	}
	if strings.TrimSpace(task.Title) == "" {
		problems = append(problems, "title is required")
	}
	if strings.TrimSpace(task.Objective) == "" {
		problems = append(problems, "objective is required")
	}
	if len(task.CompletionCriteria) == 0 {
		problems = append(problems, "completion criteria are required")
	}
	if len(task.WriteScope) == 0 {
		problems = append(problems, "write scope is required")
	}
	for _, pattern := range append(append([]string(nil), task.WriteScope...), task.ForbiddenScope...) {
		if _, err := scope.Compile(pattern); err != nil {
			problems = append(problems, err.Error())
		}
	}
	if len(task.ValidationCommands) == 0 {
		problems = append(problems, "at least one local validation command is required")
	}
	if strings.TrimSpace(task.ProjectRoot) == "" {
		problems = append(problems, "project root is required")
	}
	if task.AllowNestedAgents {
		problems = append(problems, "nested agents are forbidden in V1")
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("invalid task contract: %s", strings.Join(problems, "; "))
	}
	return nil
}

func RenderContract(task domain.Task, runID domain.RunID) (string, error) {
	if err := ValidateContract(task); err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# Task Contract: %s\n\n", task.Title)
	fmt.Fprintf(&b, "- Run ID: `%s`\n- Task ID: `%s`\n- Project root: `%s`\n\n", runID, task.TaskID, task.ProjectRoot)
	b.WriteString("## Objective\n\n" + task.Objective + "\n\n")
	writeList(&b, "Completion criteria", task.CompletionCriteria)
	writeList(&b, "Allowed write scope", task.WriteScope)
	writeList(&b, "Forbidden scope", task.ForbiddenScope)
	writeList(&b, "Known read dependencies", task.KnownReadDependencies)
	b.WriteString("## Parallel responsibilities\n\n")
	if len(task.ParallelResponsibilities) == 0 {
		b.WriteString("- None declared.\n\n")
	} else {
		ids := make([]string, 0, len(task.ParallelResponsibilities))
		for id := range task.ParallelResponsibilities {
			ids = append(ids, string(id))
		}
		sort.Strings(ids)
		for _, id := range ids {
			fmt.Fprintf(&b, "- `%s`: %s\n", id, task.ParallelResponsibilities[domain.TaskID(id)])
		}
		b.WriteString("\n")
	}
	b.WriteString("## Public interface policy\n\n")
	if task.AllowPublicInterfaceChange {
		b.WriteString("Public-interface changes are permitted only when necessary for this contract.\n\n")
	} else {
		b.WriteString("Do not change public interfaces. Submit a scope/decision request if one is required.\n\n")
	}
	b.WriteString("## Required validation\n\n")
	for _, command := range task.ValidationCommands {
		fmt.Fprintf(&b, "- `%s`", command.Command)
		if command.Scope != "" {
			fmt.Fprintf(&b, " (%s)", command.Scope)
		}
		b.WriteString("\n")
	}
	b.WriteString("\n## Scope expansion\n\nIf an out-of-scope edit is required, stop that edit and submit a scope expansion request with paths, reason, consequences, current modifications, dependencies, and a recommended decision.\n\n")
	b.WriteString("## Prohibited operations\n\n- Do not use `git reset --hard`, `git clean`, `git stash`, branch switching, or broad restore/checkout operations.\n- Do not revert or clean changes owned by other tasks.\n- Do not run whole-repository formatters or generators.\n- Do not commit all workspace changes.\n- Do not delete unknown untracked files.\n- Do not create or invoke nested subagents or another orchestrator.\n\n")
	b.WriteString("## Final report\n\nSubmit a structured Result Envelope covering status, summary, completed work, changed files, validation, remaining work, blockers, scope expansion, risks, and handoff notes.\n")
	return b.String(), nil
}

func writeList(b *strings.Builder, title string, items []string) {
	fmt.Fprintf(b, "## %s\n\n", title)
	if len(items) == 0 {
		b.WriteString("- None.\n\n")
		return
	}
	for _, item := range items {
		fmt.Fprintf(b, "- %s\n", item)
	}
	b.WriteString("\n")
}
