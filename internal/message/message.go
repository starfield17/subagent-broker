package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
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

// DeliveryMode is the Broker-owned delivery semantic for a message.
// JSON values match the adapter delivery vocabulary.
type DeliveryMode string

const (
	DeliveryImmediate   DeliveryMode = "immediate"
	DeliveryNextTurn    DeliveryMode = "next_turn"
	DeliveryResume      DeliveryMode = "resume"
	DeliveryUnsupported DeliveryMode = "unsupported"
)

// IsValidDeliveryMode reports whether mode is a known Broker delivery semantic.
func IsValidDeliveryMode(mode DeliveryMode) bool {
	switch mode {
	case DeliveryImmediate, DeliveryNextTurn, DeliveryResume, DeliveryUnsupported:
		return true
	default:
		return false
	}
}

// SchemaVersion is the message journal / envelope schema.
const SchemaVersion = "v1alpha1"

var statusTransitions = map[Status]map[Status]bool{
	Created:      {Validated: true, Failed: true},
	Validated:    {Queued: true, Failed: true},
	Queued:       {Delivered: true, Answered: true, Expired: true, Failed: true},
	Delivered:    {Acknowledged: true, Answered: true, Expired: true, Failed: true},
	Acknowledged: {Answered: true, Expired: true, Failed: true},
	Answered:     {},
	Expired:      {},
	Failed:       {},
}

// ValidateTransition checks whether a message may move from -> to.
// Same-status transitions are idempotent no-ops.
func ValidateTransition(from, to Status) error {
	if from == to {
		return nil
	}
	if IsTerminal(from) {
		return fmt.Errorf("cannot transition from terminal status %s to %s", from, to)
	}
	if statusTransitions[from][to] {
		return nil
	}
	return fmt.Errorf("invalid message status transition %s -> %s", from, to)
}

// IsTerminal reports whether status is a terminal message state.
func IsTerminal(status Status) bool {
	return status == Answered || status == Expired || status == Failed
}

// IsPending reports whether status is non-terminal.
// Do not use this alone for instruction outbox membership — see IsDeliveryPending.
func IsPending(status Status) bool {
	return !IsTerminal(status)
}

// IsDeliveryPending reports whether an instruction still needs to be sent.
// Delivered instructions are NOT delivery-pending (must not be re-sent on flush).
func IsDeliveryPending(msg Message) bool {
	return msg.Type == Instruction && msg.Status == Queued
}

// IsDecisionType reports whether the message is a Main Agent decision request.
func IsDecisionType(t Type) bool {
	switch t {
	case Question, ScopeExpansionRequest, PermissionRequest:
		return true
	default:
		return false
	}
}

// IsDecisionPending reports whether a decision message still blocks the Task.
func IsDecisionPending(msg Message) bool {
	return IsDecisionType(msg.Type) && !IsTerminal(msg.Status)
}

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
	SchemaVersion    string          `json:"schema_version"`
	MessageID        string          `json:"message_id"`
	RunID            string          `json:"run_id"`
	TaskID           string          `json:"task_id,omitempty"`
	WorkerID         string          `json:"worker_id,omitempty"`
	AttemptNumber    int             `json:"attempt_number,omitempty"`
	Type             Type            `json:"type"`
	Category         Category        `json:"category,omitempty"`
	Status           Status          `json:"status"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	InReplyTo        string          `json:"in_reply_to,omitempty"`
	DeliveryMode     DeliveryMode    `json:"delivery_mode,omitempty"`
	DeliveryAttempts int             `json:"delivery_attempts,omitempty"`
	Error            string          `json:"error,omitempty"`
	Payload          json.RawMessage `json:"payload"`
	Resolution       json.RawMessage `json:"resolution,omitempty"`
}

type InstructionPayload struct {
	Text string `json:"text"`
}

type AnswerPayload struct {
	Text string `json:"text"`
}

type ScopeRequestPayload struct {
	RequestedScope                []string `json:"requested_scope"`
	Reason                        string   `json:"reason"`
	Consequence                   string   `json:"consequence"`
	PartialModifications          string   `json:"partial_modifications"`
	RelatedTasks                  []string `json:"related_tasks,omitempty"`
	Recommendation                string   `json:"recommendation,omitempty"`
	RequiresPublicInterfaceChange bool     `json:"requires_public_interface_change,omitempty"`
}

// PermissionOption is one native permission choice (e.g. ACP option).
// OptionID is the opaque protocol id that must be returned on selection.
type PermissionOption struct {
	OptionID string `json:"option_id"`
	Kind     string `json:"kind,omitempty"`
	Name     string `json:"name,omitempty"`
}

type PermissionRequestPayload struct {
	ToolName string          `json:"tool_name"`
	Input    json.RawMessage `json:"input"`
	// Native routing metadata (protocol-native permission events).
	// Empty for Claude hook-backed permission requests.
	Harness            string `json:"harness,omitempty"`
	NativeSessionID    string `json:"native_session_id,omitempty"`
	NativePermissionID string `json:"native_permission_id,omitempty"`
	// NativeTurnID is optional; validated when both stored and active values exist.
	NativeTurnID string `json:"native_turn_id,omitempty"`
	// NativeOptions carries protocol options (ACP) needed to form a valid response.
	// Omitted for Claude hooks and Codex; old journals without this field still decode.
	NativeOptions []PermissionOption `json:"native_options,omitempty"`
}

// SelectPermissionOptionID chooses a native option by protocol kind for allow/deny.
// Prefer once over always; never infers from display names when kind is present.
func SelectPermissionOptionID(options []PermissionOption, allowed bool) (string, error) {
	if len(options) == 0 {
		return "", fmt.Errorf("no native permission options available")
	}
	prefer := []string{"reject_once", "reject_always"}
	if allowed {
		prefer = []string{"allow_once", "allow_always"}
	}
	byKind := map[string]string{}
	for _, opt := range options {
		kind := strings.TrimSpace(strings.ToLower(opt.Kind))
		id := strings.TrimSpace(opt.OptionID)
		if kind == "" || id == "" {
			continue
		}
		if _, exists := byKind[kind]; !exists {
			byKind[kind] = id
		}
	}
	for _, kind := range prefer {
		if id := byKind[kind]; id != "" {
			return id, nil
		}
	}
	if allowed {
		return "", fmt.Errorf("no compatible allow option among native permission options")
	}
	return "", fmt.Errorf("no compatible reject option among native permission options")
}

type DecisionPayload struct {
	Allowed                    bool   `json:"allowed"`
	Reason                     string `json:"reason,omitempty"`
	AllowPublicInterfaceChange bool   `json:"allow_public_interface_change,omitempty"`
}

type ResolutionKind string

const (
	ResolutionKindAnswer   ResolutionKind = "answer"
	ResolutionKindDecision ResolutionKind = "decision"
)

type Resolution struct {
	Kind     ResolutionKind   `json:"kind"`
	Answer   *AnswerPayload   `json:"answer,omitempty"`
	Decision *DecisionPayload `json:"decision,omitempty"`
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
	return PublishQuestionID(taskDir, "", q)
}

// QuestionMeta is the durable projection metadata for a question artifact.
type QuestionMeta struct {
	QuestionEnvelope
	MessageID string `json:"message_id,omitempty"`
	TaskID    string `json:"task_id,omitempty"`
}

func PublishQuestionID(taskDir, messageID string, q QuestionEnvelope) error {
	return PublishQuestionProjection(taskDir, messageID, "", q, true)
}

// PublishQuestionProjection writes the archive (always when messageID set) and
// optionally the top-level current-question projection.
func PublishQuestionProjection(taskDir, messageID, taskID string, q QuestionEnvelope, updateTopLevel bool) error {
	if err := ValidateQuestion(q); err != nil {
		return err
	}
	metaValue := QuestionMeta{QuestionEnvelope: q, MessageID: messageID, TaskID: taskID}
	meta, err := json.MarshalIndent(metaValue, "", "  ")
	if err != nil {
		return err
	}
	markdown := renderQuestionMarkdown(q)
	if messageID != "" {
		archive := filepath.Join(taskDir, "questions", messageID)
		if err := storage.AtomicWriteFile(filepath.Join(archive, "question.meta.json"), append(meta, '\n'), 0o600); err != nil {
			return err
		}
		if err := storage.AtomicWriteFile(filepath.Join(archive, "question.md"), markdown, 0o600); err != nil {
			return err
		}
	}
	if !updateTopLevel {
		return nil
	}
	if err := storage.AtomicWriteFile(filepath.Join(taskDir, "question.meta.json"), append(meta, '\n'), 0o600); err != nil {
		return err
	}
	return storage.AtomicWriteFile(filepath.Join(taskDir, "question.md"), markdown, 0o600)
}

// ClearTopLevelQuestion removes only the current-question projection files.
// Missing files are ignored; any other remove error is returned (fail-closed callers).
func ClearTopLevelQuestion(taskDir string) error {
	for _, name := range []string{"question.md", "question.meta.json"} {
		path := filepath.Join(taskDir, name)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove question projection %s: %w", name, err)
		}
	}
	return nil
}

func renderQuestionMarkdown(q QuestionEnvelope) []byte {
	var b strings.Builder
	b.WriteString("# Main Agent Decision Required\n\n## Question\n\n" + q.Question + "\n\n## Reason\n\n" + q.Reason + "\n\n## Current Task Scope\n\n")
	for _, item := range q.CurrentScope {
		fmt.Fprintf(&b, "- `%s`\n", item)
	}
	b.WriteString("\n## Scope Expansion Request (if applicable)\n\n")
	if len(q.RequestedScope) == 0 {
		b.WriteString("- None.\n")
	} else {
		for _, item := range q.RequestedScope {
			fmt.Fprintf(&b, "- `%s`\n", item)
		}
	}
	b.WriteString("\n## Relationship to Other Tasks\n\n")
	if len(q.RelatedTasks) == 0 {
		b.WriteString("No known relationship.\n")
	} else {
		for _, item := range q.RelatedTasks {
			fmt.Fprintf(&b, "- `%s`\n", item)
		}
	}
	b.WriteString("\n## Current Workspace State\n\n" + q.WorkspaceState + "\n\n## Recommendation\n\n")
	if q.Suggestion == "" {
		b.WriteString("None.\n")
	} else {
		b.WriteString(q.Suggestion + "\n")
	}
	return []byte(b.String())
}
