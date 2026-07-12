package message

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/storage"
)

type Type string

const (
	Instruction            Type = "instruction"
	Question               Type = "question"
	Answer                 Type = "answer"
	ScopeExpansionRequest  Type = "scope_expansion_request"
	ScopeExpansionDecision Type = "scope_expansion_decision"
	PermissionRequest      Type = "permission_request"
	PermissionDecision     Type = "permission_decision"
	ProgressNote           Type = "progress_note"
	CompletionReport       Type = "completion_report"
	SystemNotice           Type = "system_notice"
)

type Status string

const (
	Created      Status = "created"
	Validated    Status = "validated"
	Queued       Status = "queued"
	Delivered    Status = "delivered"
	Acknowledged Status = "acknowledged"
	Answered     Status = "answered"
	Expired      Status = "expired"
	Failed       Status = "failed"
)

type Category string

const (
	Decision           Category = "decision"
	Scope              Category = "scope"
	Permission         Category = "permission"
	MissingInformation Category = "missing_information"
	Conflict           Category = "conflict"
	Environment        Category = "environment"
	ValidationFailure  Category = "validation_failure"
)

type Message struct {
	SchemaVersion string          `json:"schema_version"`
	MessageID     string          `json:"message_id"`
	RunID         string          `json:"run_id"`
	TaskID        string          `json:"task_id,omitempty"`
	WorkerID      string          `json:"worker_id,omitempty"`
	Type          Type            `json:"type"`
	Category      Category        `json:"category,omitempty"`
	Status        Status          `json:"status"`
	CreatedAt     time.Time       `json:"created_at"`
	Payload       json.RawMessage `json:"payload"`
}

type QuestionEnvelope struct {
	SchemaVersion  string   `json:"schema_version"`
	Question       string   `json:"question"`
	Reason         string   `json:"reason"`
	CurrentScope   []string `json:"current_scope"`
	RequestedScope []string `json:"requested_scope,omitempty"`
	RelatedTasks   []string `json:"related_tasks,omitempty"`
	WorkspaceState string   `json:"workspace_state"`
	Suggestion     string   `json:"suggestion,omitempty"`
}

func ValidateQuestion(q QuestionEnvelope) error {
	var problems []string
	if q.SchemaVersion == "" || strings.TrimSpace(q.Question) == "" || strings.TrimSpace(q.Reason) == "" || len(q.CurrentScope) == 0 || strings.TrimSpace(q.WorkspaceState) == "" {
		problems = append(problems, "schema version, question, reason, current scope, and workspace state are required")
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("invalid question: %s", strings.Join(problems, "; "))
	}
	return nil
}

func PublishQuestion(taskDir string, q QuestionEnvelope) error {
	if err := ValidateQuestion(q); err != nil {
		return err
	}
	meta, err := json.MarshalIndent(q, "", "  ")
	if err != nil {
		return err
	}
	if err := storage.AtomicWriteFile(filepath.Join(taskDir, "question.meta.json"), append(meta, '\n'), 0o600); err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("# 需要主 Agent 决策\n\n## 问题\n\n" + q.Question + "\n\n## 原因\n\n" + q.Reason + "\n\n## 当前任务范围\n\n")
	for _, item := range q.CurrentScope {
		fmt.Fprintf(&b, "- `%s`\n", item)
	}
	b.WriteString("\n## 请求扩大范围（如适用）\n\n")
	if len(q.RequestedScope) == 0 {
		b.WriteString("- 无。\n")
	} else {
		for _, item := range q.RequestedScope {
			fmt.Fprintf(&b, "- `%s`\n", item)
		}
	}
	b.WriteString("\n## 与其他任务的关系\n\n")
	if len(q.RelatedTasks) == 0 {
		b.WriteString("无已知关系。\n")
	} else {
		for _, item := range q.RelatedTasks {
			fmt.Fprintf(&b, "- `%s`\n", item)
		}
	}
	b.WriteString("\n## 当前工作区状态\n\n" + q.WorkspaceState + "\n\n## 建议\n\n")
	if q.Suggestion == "" {
		b.WriteString("无。\n")
	} else {
		b.WriteString(q.Suggestion + "\n")
	}
	return storage.AtomicWriteFile(filepath.Join(taskDir, "question.md"), []byte(b.String()), 0o600)
}
