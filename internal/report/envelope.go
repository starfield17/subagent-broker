package report

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/storage"
)

const SchemaVersion = "v1alpha1"

type Status string

const (
	StatusSucceeded Status = "succeeded"
	StatusPartial   Status = "partial"
	StatusBlocked   Status = "blocked"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

type Validation struct {
	Command string `json:"command"`
	Passed  bool   `json:"passed"`
	Details string `json:"details,omitempty"`
}

type ScopeExpansion struct {
	Paths               []string `json:"paths"`
	Reason              string   `json:"reason"`
	Consequence         string   `json:"consequence"`
	PartialWriteMade    bool     `json:"partial_write_made"`
	RelatedTasks        []string `json:"related_tasks,omitempty"`
	SuggestedResolution string   `json:"suggested_resolution,omitempty"`
}

type Envelope struct {
	SchemaVersion               string          `json:"schema_version"`
	TaskID                      string          `json:"task_id"`
	WorkerID                    string          `json:"worker_id"`
	Status                      Status          `json:"status"`
	Summary                     string          `json:"summary"`
	WorkCompleted               []string        `json:"work_completed"`
	FilesChanged                []string        `json:"files_changed"`
	NoFilesChangedReason        string          `json:"no_files_changed_reason,omitempty"`
	Validation                  []Validation    `json:"validation"`
	ValidationNotRunReason      string          `json:"validation_not_run_reason,omitempty"`
	RemainingWork               []string        `json:"remaining_work"`
	BlockingIssues              []string        `json:"blocking_issues"`
	StopReason                  string          `json:"stop_reason,omitempty"`
	FailureStage                string          `json:"failure_stage,omitempty"`
	ErrorSummary                string          `json:"error_summary,omitempty"`
	WorkspaceState              string          `json:"workspace_state,omitempty"`
	ScopeExpansion              *ScopeExpansion `json:"scope_expansion,omitempty"`
	ScopeViolationsSelfReported []string        `json:"scope_violations_self_reported"`
	Risks                       []string        `json:"risks"`
	HandoffNotes                []string        `json:"handoff_notes"`
}

func ValidateEnvelope(e Envelope) error {
	var problems []string
	if e.SchemaVersion != SchemaVersion {
		problems = append(problems, "unsupported or missing schema_version")
	}
	if strings.TrimSpace(e.TaskID) == "" || strings.TrimSpace(e.WorkerID) == "" {
		problems = append(problems, "task_id and worker_id are required")
	}
	if strings.TrimSpace(e.Summary) == "" {
		problems = append(problems, "summary is required")
	}
	if len(e.FilesChanged) == 0 && strings.TrimSpace(e.NoFilesChangedReason) == "" {
		problems = append(problems, "state changed files or explain why no files changed")
	}
	if len(e.Validation) == 0 && strings.TrimSpace(e.ValidationNotRunReason) == "" {
		problems = append(problems, "state validation results or explain why validation was not run")
	}
	switch e.Status {
	case StatusSucceeded:
		if len(e.WorkCompleted) == 0 {
			problems = append(problems, "succeeded requires work_completed")
		}
		if len(e.RemainingWork) > 0 || len(e.BlockingIssues) > 0 {
			problems = append(problems, "succeeded cannot contain remaining work or blocking issues")
		}
	case StatusPartial:
		if len(e.WorkCompleted) == 0 || len(e.RemainingWork) == 0 || strings.TrimSpace(e.StopReason) == "" || strings.TrimSpace(e.WorkspaceState) == "" {
			problems = append(problems, "partial requires completed work, remaining work, stop reason, and workspace state")
		}
	case StatusBlocked:
		if len(e.BlockingIssues) == 0 || strings.TrimSpace(e.StopReason) == "" || strings.TrimSpace(e.WorkspaceState) == "" {
			problems = append(problems, "blocked requires blocking issues, stop reason, and workspace state")
		}
	case StatusFailed:
		if strings.TrimSpace(e.FailureStage) == "" || strings.TrimSpace(e.ErrorSummary) == "" || strings.TrimSpace(e.WorkspaceState) == "" || len(e.HandoffNotes) == 0 {
			problems = append(problems, "failed requires failure stage, error summary, workspace state, and handoff notes")
		}
	case StatusCancelled:
		if strings.TrimSpace(e.StopReason) == "" || strings.TrimSpace(e.WorkspaceState) == "" {
			problems = append(problems, "cancelled requires stop reason and workspace state")
		}
	default:
		problems = append(problems, fmt.Sprintf("invalid status %q", e.Status))
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("invalid result envelope: %s", strings.Join(problems, "; "))
	}
	return nil
}

type Meta struct {
	SchemaVersion string    `json:"schema_version"`
	TaskID        string    `json:"task_id"`
	WorkerID      string    `json:"worker_id"`
	Status        Status    `json:"status"`
	PublishedAt   time.Time `json:"published_at"`
}

func Publish(taskDir string, e Envelope, now time.Time) error {
	if err := ValidateEnvelope(e); err != nil {
		return err
	}
	meta, err := json.MarshalIndent(Meta{SchemaVersion: SchemaVersion, TaskID: e.TaskID, WorkerID: e.WorkerID, Status: e.Status, PublishedAt: now.UTC()}, "", "  ")
	if err != nil {
		return err
	}
	// Metadata is written first; report.md is the formal publication marker.
	if err := storage.AtomicWriteFile(filepath.Join(taskDir, "report.meta.json"), append(meta, '\n'), 0o600); err != nil {
		return err
	}
	return storage.AtomicWriteFile(filepath.Join(taskDir, "report.md"), []byte(RenderMarkdown(e)), 0o600)
}

func RenderMarkdown(e Envelope) string {
	var b strings.Builder
	b.WriteString("# Task 报告\n\n## 状态\n\n" + string(e.Status) + "\n\n")
	b.WriteString("## 完成内容\n\n")
	writeItems(&b, e.WorkCompleted, e.Summary)
	b.WriteString("## 修改文件\n\n")
	writeItems(&b, e.FilesChanged, e.NoFilesChangedReason)
	b.WriteString("## 验证\n\n")
	if len(e.Validation) == 0 {
		b.WriteString(e.ValidationNotRunReason + "\n\n")
	} else {
		for _, v := range e.Validation {
			result := "失败"
			if v.Passed {
				result = "通过"
			}
			fmt.Fprintf(&b, "- `%s`: %s", v.Command, result)
			if v.Details != "" {
				fmt.Fprintf(&b, " — %s", v.Details)
			}
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	b.WriteString("## 未完成内容\n\n")
	writeItems(&b, e.RemainingWork, "无。")
	b.WriteString("## 风险与注意事项\n\n")
	writeItems(&b, e.Risks, "无已知风险。")
	b.WriteString("## 交接说明\n\n")
	writeItems(&b, e.HandoffNotes, "无额外交接说明。")
	return b.String()
}

func writeItems(b *strings.Builder, items []string, fallback string) {
	if len(items) == 0 {
		b.WriteString(fallback + "\n\n")
		return
	}
	for _, item := range items {
		fmt.Fprintf(b, "- %s\n", item)
	}
	b.WriteString("\n")
}
