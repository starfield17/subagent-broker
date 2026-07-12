package wave

import (
	"fmt"
	"sort"
	"strings"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/scope"
	taskcontract "github.com/vnai/subagent-broker/internal/task"
)

type IssueKind string

const (
	IssueDuplicateTask      IssueKind = "duplicate_task"
	IssueScopeOverlap       IssueKind = "scope_overlap"
	IssueWriteReadConflict  IssueKind = "write_read_conflict"
	IssueValidationConflict IssueKind = "validation_conflict"
	IssueSameWaveDependency IssueKind = "same_wave_dependency"
	IssueNestedAgents       IssueKind = "nested_agents_forbidden"
	IssueMissingContract    IssueKind = "missing_contract_field"
	IssueHighRiskShared     IssueKind = "high_risk_file_shared"
)

type Issue struct {
	Kind    IssueKind `json:"kind"`
	Tasks   []string  `json:"tasks,omitempty"`
	Details string    `json:"details"`
}

type PreflightResult struct {
	Allowed     bool                `json:"allowed"`
	Concurrency int                 `json:"concurrency"`
	Scopes      map[string][]string `json:"scopes"`
	Issues      []Issue             `json:"issues"`
}

var highRiskFiles = []string{
	"go.mod", "go.sum", "package.json", "package-lock.json", "pnpm-lock.yaml", "yarn.lock",
	"Cargo.toml", "Cargo.lock", ".github/workflows/**", "migrations/**", "schema/**",
}

func Preflight(tasks []domain.Task) PreflightResult {
	result := PreflightResult{Allowed: true, Concurrency: len(tasks), Scopes: map[string][]string{}}
	ids := map[domain.TaskID]domain.Task{}
	for _, task := range tasks {
		id := string(task.TaskID)
		if err := taskcontract.ValidateContract(task); err != nil {
			result.Issues = append(result.Issues, Issue{Kind: IssueMissingContract, Tasks: []string{id}, Details: err.Error()})
		}
		if _, exists := ids[task.TaskID]; exists {
			result.Issues = append(result.Issues, Issue{Kind: IssueDuplicateTask, Tasks: []string{id}, Details: "duplicate task id"})
		}
		ids[task.TaskID] = task
		result.Scopes[id] = append([]string(nil), task.WriteScope...)
		if task.AllowNestedAgents {
			result.Issues = append(result.Issues, Issue{Kind: IssueNestedAgents, Tasks: []string{id}, Details: "V1 globally forbids nested agents"})
		}
	}
	for _, task := range tasks {
		for _, dependency := range task.DependsOn {
			if _, sameWave := ids[dependency]; sameWave {
				result.Issues = append(result.Issues, Issue{Kind: IssueSameWaveDependency, Tasks: []string{string(task.TaskID), string(dependency)}, Details: "a task depends on another task in the same Wave"})
			}
		}
		for _, validation := range task.ValidationCommands {
			if isGlobalValidation(validation) {
				result.Issues = append(result.Issues, Issue{Kind: IssueValidationConflict, Tasks: []string{string(task.TaskID)}, Details: "same-Wave task declares repository-wide validation: " + validation.Command})
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
						result.Issues = append(result.Issues, Issue{Kind: IssueWriteReadConflict, Tasks: []string{string(reader.TaskID), string(writer.TaskID)}, Details: fmt.Sprintf("read dependency %s overlaps writer scope %s", dependency, writePattern)})
					}
				}
			}
		}
	}
	if overlaps, err := scope.FindOverlaps(result.Scopes); err != nil {
		result.Issues = append(result.Issues, Issue{Kind: IssueScopeOverlap, Details: err.Error()})
	} else {
		for _, overlap := range overlaps {
			result.Issues = append(result.Issues, Issue{
				Kind:    IssueScopeOverlap,
				Tasks:   []string{overlap.LeftOwner, overlap.RightOwner},
				Details: fmt.Sprintf("%s overlaps %s", overlap.LeftPattern, overlap.RightPattern),
			})
		}
	}
	for _, highRisk := range highRiskFiles {
		owners := ownersPotentiallyClaiming(highRisk, result.Scopes)
		if len(owners) > 1 {
			result.Issues = append(result.Issues, Issue{Kind: IssueHighRiskShared, Tasks: owners, Details: highRisk + " is a high-risk global object"})
		}
	}
	sort.SliceStable(result.Issues, func(i, j int) bool {
		if result.Issues[i].Kind != result.Issues[j].Kind {
			return result.Issues[i].Kind < result.Issues[j].Kind
		}
		return result.Issues[i].Details < result.Issues[j].Details
	})
	result.Allowed = len(result.Issues) == 0
	return result
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
