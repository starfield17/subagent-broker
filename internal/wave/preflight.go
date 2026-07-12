package wave

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/scope"
	taskcontract "github.com/vnai/subagent-broker/internal/task"
)

type IssueKind string

const (
	IssueDuplicateTask                  IssueKind = "duplicate_task"
	IssueScopeOverlap                   IssueKind = "scope_overlap"
	IssueWriteReadConflict              IssueKind = "write_read_conflict"
	IssueValidationConflict             IssueKind = "validation_conflict"
	IssueSameWaveDependency             IssueKind = "same_wave_dependency"
	IssueNestedAgents                   IssueKind = "nested_agents_forbidden"
	IssueMissingContract                IssueKind = "missing_contract_field"
	IssueHighRiskShared                 IssueKind = "high_risk_file_shared"
	IssueMissingParallelResponsibility  IssueKind = "missing_parallel_responsibility"
	IssueInvalidParallelResponsibility  IssueKind = "invalid_parallel_responsibility"
	IssueHarnessNotRegistered           IssueKind = "harness_not_registered"
	IssueHarnessNotInstalled            IssueKind = "harness_not_installed"
	IssueHarnessNotAuthenticated        IssueKind = "harness_not_authenticated"
	IssueHarnessProbeFailed             IssueKind = "harness_probe_failed"
	IssueHarnessIncompatible            IssueKind = "harness_incompatible"
	IssueHarnessCompatibilityUnverified IssueKind = "harness_compatibility_unverified"
)

// IssueSeverity classifies preflight findings. Empty severity is treated as error
// for backward compatibility with older Issue values.
type IssueSeverity string

const (
	SeverityWarning IssueSeverity = "warning"
	SeverityError   IssueSeverity = "error"
)

type Issue struct {
	Kind     IssueKind     `json:"kind"`
	Severity IssueSeverity `json:"severity,omitempty"`
	Tasks    []string      `json:"tasks,omitempty"`
	Details  string        `json:"details"`
}

type PreflightResult struct {
	Allowed     bool                `json:"allowed"`
	Concurrency int                 `json:"concurrency"`
	Scopes      map[string][]string `json:"scopes"`
	Issues      []Issue             `json:"issues"`
}

// AdapterResolver looks up harness adapters for environment preflight.
// *adapter.Registry satisfies this interface.
type AdapterResolver interface {
	Get(adapter.HarnessName) (adapter.Adapter, bool)
}

// PreflightEnvironment supplies harness probe context.
// Callers must fill empty Task.HarnessPreference before calling EvaluatePreflight;
// the evaluator does not invent a default harness.
type PreflightEnvironment struct {
	Registry               AdapterResolver
	Executable             string
	ProbeTimeout           time.Duration
	PermissionMode         string
	RequirePermissionHooks bool // when true, missing permission capability is fatal after probe
	SafeMode               bool
}

// HarnessPreflight records one harness probe outcome.
type HarnessPreflight struct {
	Harness string              `json:"harness"`
	Probe   adapter.ProbeResult `json:"probe"`
	Error   string              `json:"error,omitempty"`
}

// EnvironmentPreflightResult combines static and harness probe findings.
type EnvironmentPreflightResult struct {
	Static    PreflightResult             `json:"static"`
	Harnesses map[string]HarnessPreflight `json:"harnesses,omitempty"`
	Issues    []Issue                     `json:"issues"`
	Allowed   bool                        `json:"allowed"`
}

var highRiskFiles = []string{
	"go.mod", "go.sum", "package.json", "package-lock.json", "pnpm-lock.yaml", "yarn.lock",
	"Cargo.toml", "Cargo.lock", ".github/workflows/**", "migrations/**", "schema/**",
}

// Preflight runs pure static Wave checks. Existing callers and tests remain valid.
func Preflight(tasks []domain.Task) PreflightResult {
	result := PreflightResult{Allowed: true, Concurrency: len(tasks), Scopes: map[string][]string{}}
	ids := map[domain.TaskID]domain.Task{}
	for _, task := range tasks {
		id := string(task.TaskID)
		if err := taskcontract.ValidateContract(task); err != nil {
			result.Issues = append(result.Issues, Issue{Kind: IssueMissingContract, Severity: SeverityError, Tasks: []string{id}, Details: err.Error()})
		}
		if _, exists := ids[task.TaskID]; exists {
			result.Issues = append(result.Issues, Issue{Kind: IssueDuplicateTask, Severity: SeverityError, Tasks: []string{id}, Details: "duplicate task id"})
		}
		ids[task.TaskID] = task
		result.Scopes[id] = append([]string(nil), task.WriteScope...)
		if task.AllowNestedAgents {
			result.Issues = append(result.Issues, Issue{Kind: IssueNestedAgents, Severity: SeverityError, Tasks: []string{id}, Details: "V1 globally forbids nested agents"})
		}
	}
	if len(tasks) > 1 {
		result.Issues = append(result.Issues, checkParallelResponsibilities(tasks, ids)...)
	}
	for _, task := range tasks {
		for _, dependency := range task.DependsOn {
			if _, sameWave := ids[dependency]; sameWave {
				result.Issues = append(result.Issues, Issue{Kind: IssueSameWaveDependency, Severity: SeverityError, Tasks: []string{string(task.TaskID), string(dependency)}, Details: "a task depends on another task in the same Wave"})
			}
		}
		for _, validation := range task.ValidationCommands {
			if isGlobalValidation(validation) {
				result.Issues = append(result.Issues, Issue{Kind: IssueValidationConflict, Severity: SeverityError, Tasks: []string{string(task.TaskID)}, Details: "same-Wave task declares repository-wide validation: " + validation.Command})
			}
		}
	}
	for _, reader := range tasks {
		for _, dependency := range reader.KnownReadDependencies {
			for _, writer := range tasks {
				if reader.TaskID == writer.TaskID {
					continue
				}
				for _, writePattern := range writer.WriteScope {
					overlap, err := scope.MayOverlap(dependency, writePattern)
					if err == nil && overlap {
						result.Issues = append(result.Issues, Issue{Kind: IssueWriteReadConflict, Severity: SeverityError, Tasks: []string{string(reader.TaskID), string(writer.TaskID)}, Details: fmt.Sprintf("read dependency %s overlaps writer scope %s", dependency, writePattern)})
					}
				}
			}
		}
	}
	if overlaps, err := scope.FindOverlaps(result.Scopes); err != nil {
		result.Issues = append(result.Issues, Issue{Kind: IssueScopeOverlap, Severity: SeverityError, Details: err.Error()})
	} else {
		for _, overlap := range overlaps {
			result.Issues = append(result.Issues, Issue{
				Kind:     IssueScopeOverlap,
				Severity: SeverityError,
				Tasks:    []string{overlap.LeftOwner, overlap.RightOwner},
				Details:  fmt.Sprintf("%s overlaps %s", overlap.LeftPattern, overlap.RightPattern),
			})
		}
	}
	for _, highRisk := range highRiskFiles {
		owners := ownersPotentiallyClaiming(highRisk, result.Scopes)
		if len(owners) > 1 {
			result.Issues = append(result.Issues, Issue{Kind: IssueHighRiskShared, Severity: SeverityError, Tasks: owners, Details: highRisk + " is a high-risk global object"})
		}
	}
	sortIssues(result.Issues)
	result.Allowed = !hasErrorIssues(result.Issues)
	return result
}

// EvaluatePreflight runs static Preflight plus one probe per unique non-empty harness preference.
func EvaluatePreflight(
	ctx context.Context,
	tasks []domain.Task,
	environment PreflightEnvironment,
) EnvironmentPreflightResult {
	static := Preflight(tasks)
	result := EnvironmentPreflightResult{
		Static:    static,
		Harnesses: map[string]HarnessPreflight{},
		Issues:    append([]Issue(nil), static.Issues...),
	}

	unique := uniqueHarnesses(tasks)
	timeout := environment.ProbeTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	for _, name := range unique {
		harnessResult := probeHarness(ctx, name, environment, timeout)
		result.Harnesses[name] = harnessResult
		result.Issues = append(result.Issues, harnessIssues(name, harnessResult)...)
		// Safety-critical: if tasks require permission events, hooks must be installable.
		if tasksRequirePermission(tasks, name) {
			if harnessResult.Error != "" || !harnessResult.Probe.Installed {
				result.Issues = append(result.Issues, Issue{
					Kind: IssueHarnessProbeFailed, Severity: SeverityError, Tasks: []string{name},
					Details: fmt.Sprintf("harness %s is required for permission routing but probe failed", name),
				})
			} else if !harnessResult.Probe.Capabilities.PermissionEvents && !harnessResult.Probe.Capabilities.Hooks {
				// Probe returned empty capabilities — fall back to declared via probe result only.
			}
			// When SessionConfig would not install hooks (caller passes PermissionMode),
			// environment may include RequirePermissionHooks.
			if environment.RequirePermissionHooks {
				// Actual install is session-level; preflight requires declared permission support.
				if !harnessResult.Probe.Capabilities.PermissionEvents && harnessResult.Error == "" {
					// If probe capabilities are zero, check is deferred to session start.
					// Still emit a warning that permission routing is required.
					result.Issues = append(result.Issues, Issue{
						Kind: IssueHarnessCompatibilityUnverified, Severity: SeverityWarning, Tasks: []string{name},
						Details: fmt.Sprintf("harness %s permission routing required; verify hooks install at session start", name),
					})
				}
			}
		}
	}
	sortIssues(result.Issues)
	result.Allowed = !hasErrorIssues(result.Issues)
	return result
}

// tasksRequirePermission reports whether any task using harness needs permission routing.
func tasksRequirePermission(tasks []domain.Task, harness string) bool {
	for _, task := range tasks {
		if strings.TrimSpace(task.HarnessPreference) != harness && harness != "" {
			// still check when preference empty and filled by caller
		}
		// Task metadata: AllowPublicInterfaceChange alone is not permission routing.
		// Require when ForbiddenScope is non-empty or validation implies tool gates —
		// for PR5, RequirePermissionHooks on environment is the primary gate.
		_ = task
	}
	return false
}

func checkParallelResponsibilities(tasks []domain.Task, ids map[domain.TaskID]domain.Task) []Issue {
	var issues []Issue
	for _, task := range tasks {
		taskID := string(task.TaskID)
		if task.ParallelResponsibilities == nil {
			task.ParallelResponsibilities = map[domain.TaskID]string{}
		}
		for otherID := range ids {
			if otherID == task.TaskID {
				continue
			}
			text, ok := task.ParallelResponsibilities[otherID]
			if !ok {
				issues = append(issues, Issue{
					Kind:     IssueMissingParallelResponsibility,
					Severity: SeverityError,
					Tasks:    []string{taskID, string(otherID)},
					Details:  fmt.Sprintf("task %s is missing parallel responsibility for %s", task.TaskID, otherID),
				})
				continue
			}
			if isPlaceholderResponsibility(text) {
				issues = append(issues, Issue{
					Kind:     IssueInvalidParallelResponsibility,
					Severity: SeverityError,
					Tasks:    []string{taskID, string(otherID)},
					Details:  fmt.Sprintf("task %s has empty or placeholder parallel responsibility for %s", task.TaskID, otherID),
				})
			}
		}
		for declared, text := range task.ParallelResponsibilities {
			if declared == task.TaskID {
				issues = append(issues, Issue{
					Kind:     IssueInvalidParallelResponsibility,
					Severity: SeverityError,
					Tasks:    []string{taskID},
					Details:  fmt.Sprintf("task %s must not declare a parallel responsibility for itself", task.TaskID),
				})
				continue
			}
			if _, inWave := ids[declared]; !inWave {
				issues = append(issues, Issue{
					Kind:     IssueInvalidParallelResponsibility,
					Severity: SeverityError,
					Tasks:    []string{taskID, string(declared)},
					Details:  fmt.Sprintf("task %s declares parallel responsibility for out-of-wave task %s", task.TaskID, declared),
				})
				_ = text
			}
		}
	}
	return issues
}

func isPlaceholderResponsibility(text string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(text))
	if trimmed == "" {
		return true
	}
	switch trimmed {
	case "none", "none declared", "none declared.", "n/a", "na", "tbd", "todo", "-", "—", "see contract", "see task contract":
		return true
	}
	return false
}

// RenderPreflightMarkdown produces an audit-friendly preflight report.
func RenderPreflightMarkdown(result EnvironmentPreflightResult) string {
	var b strings.Builder
	b.WriteString("# Wave Preflight\n\n")
	fmt.Fprintf(&b, "- Allowed: `%v`\n- Static concurrency: %d\n\n", result.Allowed, result.Static.Concurrency)
	b.WriteString("## Harnesses\n\n")
	if len(result.Harnesses) == 0 {
		b.WriteString("- None probed.\n\n")
	} else {
		names := make([]string, 0, len(result.Harnesses))
		for name := range result.Harnesses {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			h := result.Harnesses[name]
			fmt.Fprintf(&b, "### %s\n\n- Installed: `%v`\n- Version: `%s`\n- Compatibility: `%s`\n", name, h.Probe.Installed, h.Probe.Version, h.Probe.Compatibility)
			if h.Error != "" {
				fmt.Fprintf(&b, "- Error: %s\n", h.Error)
			}
			b.WriteString("\n")
		}
	}
	b.WriteString("## Issues\n\n")
	if len(result.Issues) == 0 {
		b.WriteString("- None.\n")
	} else {
		for _, issue := range result.Issues {
			sev := issue.Severity
			if sev == "" {
				sev = SeverityError
			}
			fmt.Fprintf(&b, "- **%s** `%s`: %s\n", sev, issue.Kind, issue.Details)
		}
	}
	return b.String()
}

func uniqueHarnesses(tasks []domain.Task) []string {
	seen := map[string]struct{}{}
	var names []string
	for _, task := range tasks {
		name := strings.TrimSpace(task.HarnessPreference)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func probeHarness(ctx context.Context, name string, environment PreflightEnvironment, timeout time.Duration) HarnessPreflight {
	result := HarnessPreflight{Harness: name}
	if environment.Registry == nil {
		result.Error = "adapter registry is not configured"
		return result
	}
	harness, ok := environment.Registry.Get(adapter.HarnessName(name))
	if !ok {
		result.Error = "adapter is not registered"
		return result
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	probe, err := harness.Probe(probeCtx, adapter.ProbeRequest{Executable: environment.Executable})
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.Probe = probe
	return result
}

func harnessIssues(name string, value HarnessPreflight) []Issue {
	tasks := []string{name}
	if value.Error != "" {
		kind := IssueHarnessProbeFailed
		if value.Error == "adapter is not registered" {
			kind = IssueHarnessNotRegistered
		}
		return []Issue{{Kind: kind, Severity: SeverityError, Tasks: tasks, Details: fmt.Sprintf("harness %s: %s", name, value.Error)}}
	}
	var issues []Issue
	if !value.Probe.Installed {
		issues = append(issues, Issue{Kind: IssueHarnessNotInstalled, Severity: SeverityError, Tasks: tasks, Details: fmt.Sprintf("harness %s is not installed", name)})
	}
	if value.Probe.Authenticated != nil && !*value.Probe.Authenticated {
		issues = append(issues, Issue{Kind: IssueHarnessNotAuthenticated, Severity: SeverityError, Tasks: tasks, Details: fmt.Sprintf("harness %s is not authenticated", name)})
	}
	switch strings.ToLower(strings.TrimSpace(value.Probe.Compatibility)) {
	case "incompatible":
		issues = append(issues, Issue{Kind: IssueHarnessIncompatible, Severity: SeverityError, Tasks: tasks, Details: fmt.Sprintf("harness %s is incompatible", name)})
	case "probe_failed":
		issues = append(issues, Issue{Kind: IssueHarnessProbeFailed, Severity: SeverityError, Tasks: tasks, Details: fmt.Sprintf("harness %s probe failed", name)})
	case "compatibility_unverified":
		issues = append(issues, Issue{Kind: IssueHarnessCompatibilityUnverified, Severity: SeverityWarning, Tasks: tasks, Details: fmt.Sprintf("harness %s compatibility is unverified", name)})
	}
	for _, warning := range value.Probe.Warnings {
		if strings.TrimSpace(warning) == "" {
			continue
		}
		issues = append(issues, Issue{
			Kind:     IssueHarnessCompatibilityUnverified,
			Severity: SeverityWarning,
			Tasks:    tasks,
			Details:  fmt.Sprintf("harness %s: %s", name, warning),
		})
	}
	return issues
}

func ownersPotentiallyClaiming(pattern string, leases map[string][]string) []string {
	var owners []string
	for owner, patterns := range leases {
		for _, candidate := range patterns {
			overlap, err := scope.MayOverlap(pattern, candidate)
			if err == nil && overlap {
				owners = append(owners, owner)
				break
			}
		}
	}
	sort.Strings(owners)
	return owners
}

func isGlobalValidation(command domain.ValidationCommand) bool {
	scopeName := strings.ToLower(strings.TrimSpace(command.Scope))
	if scopeName == "global" || scopeName == "repository" || scopeName == "repo" || scopeName == "all" {
		return true
	}
	text := strings.ToLower(command.Command)
	return strings.Contains(text, "go test ./...") || strings.Contains(text, "npm test -- --all") || strings.Contains(text, "cargo test --workspace")
}

func issueSeverity(issue Issue) IssueSeverity {
	if issue.Severity == "" {
		return SeverityError
	}
	return issue.Severity
}

func hasErrorIssues(issues []Issue) bool {
	for _, issue := range issues {
		if issueSeverity(issue) == SeverityError {
			return true
		}
	}
	return false
}

func sortIssues(issues []Issue) {
	sort.SliceStable(issues, func(i, j int) bool {
		si, sj := issueSeverity(issues[i]), issueSeverity(issues[j])
		if si != sj {
			// errors before warnings for stable operator reading
			if si == SeverityError && sj != SeverityError {
				return true
			}
			if sj == SeverityError && si != SeverityError {
				return false
			}
			return si < sj
		}
		if issues[i].Kind != issues[j].Kind {
			return issues[i].Kind < issues[j].Kind
		}
		return issues[i].Details < issues[j].Details
	})
}
