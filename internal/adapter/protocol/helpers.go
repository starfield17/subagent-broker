// Package protocol contains protocol-independent helpers used by native
// adapters. It intentionally does not hide transport or harness semantics.
package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/vnai/subagent-broker/internal/report"
)

var versionPattern = regexp.MustCompile(`\bv?[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?\b`)

func CommandOutput(ctx context.Context, executable string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, executable, args...).CombinedOutput()
}

func ParseVersion(output []byte) string {
	match := versionPattern.Find(output)
	return strings.TrimPrefix(string(match), "v")
}

func ParseEnvelope(raw []byte) (report.Envelope, error) {
	trimmed, err := envelopeJSON(raw)
	if err != nil {
		return report.Envelope{}, err
	}
	var envelope report.Envelope
	canonicalErr := json.Unmarshal(trimmed, &envelope)
	if canonicalErr == nil {
		if validationErr := report.ValidateEnvelope(envelope); validationErr == nil {
			return envelope, nil
		} else {
			canonicalErr = validationErr
		}
	}

	// Codex can return its own task-completion summary even when the prompt
	// requests the Broker envelope. Normalize that stable native shape at the
	// protocol boundary so the supervisor still receives one canonical report.
	var native nativeCompletion
	if err := json.Unmarshal(trimmed, &native); err == nil {
		if normalized, normalizeErr := native.normalize(); normalizeErr == nil {
			if validateErr := report.ValidateEnvelope(normalized); validateErr == nil {
				return normalized, nil
			} else {
				canonicalErr = validateErr
			}
		}
	}
	return report.Envelope{}, fmt.Errorf("decode Result Envelope: %w", canonicalErr)
}

func envelopeJSON(raw []byte) ([]byte, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return nil, fmt.Errorf("empty Result Envelope")
	}
	if strings.HasPrefix(trimmed, "```") {
		trimmed = strings.TrimPrefix(trimmed, "```")
		if newline := strings.IndexByte(trimmed, '\n'); newline >= 0 {
			trimmed = trimmed[newline+1:]
		}
		trimmed = strings.TrimSuffix(strings.TrimSpace(trimmed), "```")
	}
	if strings.HasPrefix(trimmed, `"`) {
		var text string
		if err := json.Unmarshal([]byte(trimmed), &text); err != nil {
			return nil, fmt.Errorf("decode Result Envelope text: %w", err)
		}
		trimmed = strings.TrimSpace(text)
	}
	if json.Valid([]byte(trimmed)) {
		return []byte(trimmed), nil
	}

	// Native CLIs sometimes add a short prose preamble despite the worker
	// contract. Recover only a complete JSON object; schema validation below
	// still rejects arbitrary or incomplete text.
	start := strings.IndexByte(trimmed, '{')
	end := strings.LastIndexByte(trimmed, '}')
	if start >= 0 && end > start {
		candidate := strings.TrimSpace(trimmed[start : end+1])
		if json.Valid([]byte(candidate)) {
			return []byte(candidate), nil
		}
	}
	return nil, fmt.Errorf("decode Result Envelope: invalid JSON")
}

type nativeCompletion struct {
	SchemaVersion          string                 `json:"schema_version"`
	TaskID                 string                 `json:"task_id"`
	WorkerID               string                 `json:"worker_id"`
	Status                 string                 `json:"status"`
	Summary                string                 `json:"summary"`
	CompletedWork          []string               `json:"completed_work"`
	ChangedFiles           []string               `json:"changed_files"`
	NoFilesChangedReason   string                 `json:"no_files_changed_reason"`
	Validation             json.RawMessage        `json:"validation"`
	ValidationNotRunReason string                 `json:"validation_not_run_reason"`
	RemainingWork          []string               `json:"remaining_work"`
	Blockers               []string               `json:"blockers"`
	BlockingIssues         []string               `json:"blocking_issues"`
	StopReason             string                 `json:"stop_reason"`
	FailureStage           string                 `json:"failure_stage"`
	ErrorSummary           string                 `json:"error_summary"`
	WorkspaceState         string                 `json:"workspace_state"`
	ScopeExpansion         *report.ScopeExpansion `json:"scope_expansion"`
	ScopeViolations        []string               `json:"scope_violations_self_reported"`
	Risks                  []string               `json:"risks"`
	HandoffNotes           json.RawMessage        `json:"handoff_notes"`
}

type nativeValidation struct {
	Command string `json:"command"`
	Status  string `json:"status"`
	Passed  *bool  `json:"passed"`
	Details string `json:"details"`
}

func (n nativeCompletion) normalize() (report.Envelope, error) {
	status := strings.ToLower(strings.TrimSpace(n.Status))
	var normalized report.Status
	switch status {
	case "completed", "succeeded", "success":
		normalized = report.StatusSucceeded
	case "partial":
		normalized = report.StatusPartial
	case "blocked":
		normalized = report.StatusBlocked
	case "failed", "failure":
		normalized = report.StatusFailed
	case "cancelled", "canceled":
		normalized = report.StatusCancelled
	default:
		return report.Envelope{}, fmt.Errorf("unsupported native completion status %q", n.Status)
	}

	blocking := append([]string(nil), n.Blockers...)
	blocking = append(blocking, n.BlockingIssues...)
	if normalized == report.StatusSucceeded && (len(n.RemainingWork) > 0 || len(blocking) > 0) {
		normalized = report.StatusPartial
	}
	nativeValidations := nativeValidationList(n.Validation)
	validation := make([]report.Validation, 0, len(nativeValidations))
	for _, item := range nativeValidations {
		passed := strings.EqualFold(strings.TrimSpace(item.Status), "passed")
		if item.Passed != nil {
			passed = *item.Passed
		}
		validation = append(validation, report.Validation{Command: item.Command, Passed: passed, Details: item.Details})
	}
	handoff := nativeStringList(n.HandoffNotes)
	if normalized == report.StatusFailed && len(handoff) == 0 {
		handoff = []string{"Native harness reported failure."}
	}
	result := report.Envelope{
		SchemaVersion:               report.SchemaVersion,
		TaskID:                      n.TaskID,
		WorkerID:                    n.WorkerID,
		Status:                      normalized,
		Summary:                     n.Summary,
		WorkCompleted:               append([]string(nil), n.CompletedWork...),
		FilesChanged:                append([]string(nil), n.ChangedFiles...),
		NoFilesChangedReason:        n.NoFilesChangedReason,
		Validation:                  validation,
		ValidationNotRunReason:      n.ValidationNotRunReason,
		RemainingWork:               append([]string(nil), n.RemainingWork...),
		BlockingIssues:              blocking,
		StopReason:                  n.StopReason,
		FailureStage:                n.FailureStage,
		ErrorSummary:                n.ErrorSummary,
		WorkspaceState:              n.WorkspaceState,
		ScopeExpansion:              n.ScopeExpansion,
		ScopeViolationsSelfReported: append([]string(nil), n.ScopeViolations...),
		Risks:                       append([]string(nil), n.Risks...),
		HandoffNotes:                handoff,
	}
	if len(result.FilesChanged) == 0 && result.NoFilesChangedReason == "" {
		result.NoFilesChangedReason = "Native harness reported no changed files."
	}
	if len(result.Validation) == 0 && result.ValidationNotRunReason == "" {
		result.ValidationNotRunReason = "Native harness reported no validation results."
	}
	switch result.Status {
	case report.StatusPartial:
		if result.StopReason == "" {
			result.StopReason = "Native harness reported incomplete work."
		}
		if result.WorkspaceState == "" {
			result.WorkspaceState = "Workspace state reported by native harness."
		}
	case report.StatusBlocked:
		if result.StopReason == "" {
			result.StopReason = "Native harness reported blocking issues."
		}
		if result.WorkspaceState == "" {
			result.WorkspaceState = "Workspace state reported by native harness."
		}
	case report.StatusFailed:
		if result.FailureStage == "" {
			result.FailureStage = "native_harness"
		}
		if result.ErrorSummary == "" {
			result.ErrorSummary = result.Summary
		}
		if result.WorkspaceState == "" {
			result.WorkspaceState = "Workspace state reported by native harness."
		}
	case report.StatusCancelled:
		if result.StopReason == "" {
			result.StopReason = "Native harness cancelled the task."
		}
		if result.WorkspaceState == "" {
			result.WorkspaceState = "Workspace state reported by native harness."
		}
	}
	return result, nil
}

func nativeValidationList(raw json.RawMessage) []nativeValidation {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var values []nativeValidation
	if json.Unmarshal(raw, &values) == nil {
		return values
	}
	var value nativeValidation
	if json.Unmarshal(raw, &value) == nil {
		return []nativeValidation{value}
	}
	return nil
}

func nativeStringList(raw json.RawMessage) []string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var values []string
	if json.Unmarshal(raw, &values) == nil {
		return values
	}
	var value string
	if json.Unmarshal(raw, &value) == nil && strings.TrimSpace(value) != "" {
		return []string{value}
	}
	return nil
}

func Bool(value bool) *bool { return &value }
