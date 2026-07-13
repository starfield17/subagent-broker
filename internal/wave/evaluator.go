package wave

import (
	"fmt"
	"sort"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/verify"
)

// ReportAssessment is a pure evaluation view of one Task report.
// Disk reread / identity checks are performed by the caller and supplied here.
type ReportAssessment struct {
	TaskID         domain.TaskID `json:"task_id"`
	RuntimeStatus  state.Task    `json:"runtime_status,omitempty"`
	EnvelopeStatus report.Status `json:"envelope_status,omitempty"`
	Present        bool          `json:"present"`
	MetaValid      bool          `json:"meta_valid"`
	MarkdownValid  bool          `json:"markdown_valid"`
	IdentityValid  bool          `json:"identity_valid"`
	Error          string        `json:"error,omitempty"`
}

// PendingDecision is an unresolved Main Agent message that blocks the Wave.
type PendingDecision struct {
	MessageID string       `json:"message_id"`
	TaskID    string       `json:"task_id"`
	Type      message.Type `json:"type"`
}

// HighRiskChange is a changed path classified by the caller as high-risk.
type HighRiskChange struct {
	Path     string        `json:"path"`
	Severity IssueSeverity `json:"severity"`
	Reason   string        `json:"reason"`
}

// BarrierInputs is the pure input model for EvaluateBarrier.
// Callers collect workspace/report/message data; the evaluator never touches disk.
type BarrierInputs struct {
	WaveID           domain.WaveID
	Cancelled        bool
	Reports          []ReportAssessment
	Pending          []PendingDecision
	ChangedFiles     []string
	ScopeAudit       verify.ScopeAudit
	HighRiskChanges  []HighRiskChange
	Checks           []CheckResult
	ExistingWarnings []string
	ExistingErrors   []string
}

// EvaluateBarrier decides Barrier result from pure inputs.
//
// Priority:
//  1. cancelled
//  2. failed
//  3. blocked
//  4. passed_with_warnings
//  5. passed
func EvaluateBarrier(input BarrierInputs, now time.Time) Verification {
	verification := Verification{
		SchemaVersion:   "v1alpha1",
		WaveID:          input.WaveID,
		StartedAt:       now.UTC(),
		EndedAt:         now.UTC(),
		ChangedFiles:    uniqueSorted(input.ChangedFiles),
		ScopeAudit:      input.ScopeAudit,
		Checks:          append([]CheckResult(nil), input.Checks...),
		HighRiskChanges: append([]HighRiskChange(nil), input.HighRiskChanges...),
		Reports:         append([]ReportAssessment(nil), input.Reports...),
	}

	var errors []string
	var warnings []string
	blocked := false

	errors = append(errors, input.ExistingErrors...)
	warnings = append(warnings, input.ExistingWarnings...)

	for _, item := range input.Reports {
		if item.Error != "" {
			errors = append(errors, item.Error)
		}
		if !item.Present {
			errors = append(errors, fmt.Sprintf("task %s report is missing", item.TaskID))
		} else {
			if !item.MetaValid {
				errors = append(errors, fmt.Sprintf("task %s report.meta.json is invalid", item.TaskID))
			}
			if !item.MarkdownValid {
				errors = append(errors, fmt.Sprintf("task %s report.md is invalid", item.TaskID))
			}
			if !item.IdentityValid {
				errors = append(errors, fmt.Sprintf("task %s report identity mismatch", item.TaskID))
			}
		}

		switch item.RuntimeStatus {
		case state.TaskFailed, state.TaskVerificationFailed:
			errors = append(errors, fmt.Sprintf("task %s is %s", item.TaskID, item.RuntimeStatus))
		case state.TaskCancelled:
			if !input.Cancelled {
				errors = append(errors, fmt.Sprintf("task %s is cancelled", item.TaskID))
			}
		case state.TaskBlocked:
			blocked = true
			warnings = append(warnings, fmt.Sprintf("task %s is blocked", item.TaskID))
		case state.TaskVerifiedPartial:
			warnings = append(warnings, fmt.Sprintf("task %s is verified_partial", item.TaskID))
		}

		switch item.EnvelopeStatus {
		case report.StatusFailed, report.StatusCancelled:
			errors = append(errors, fmt.Sprintf("task %s envelope status is %s", item.TaskID, item.EnvelopeStatus))
		case report.StatusBlocked:
			blocked = true
			warnings = append(warnings, fmt.Sprintf("task %s envelope status is blocked", item.TaskID))
		case report.StatusPartial:
			warnings = append(warnings, fmt.Sprintf("task %s envelope status is partial", item.TaskID))
		}
	}

	for _, pending := range input.Pending {
		blocked = true
		verification.PendingMessages = append(verification.PendingMessages, pending.MessageID)
		warnings = append(warnings, fmt.Sprintf("pending %s message %s for task %s", pending.Type, pending.MessageID, pending.TaskID))
	}

	for _, path := range input.ScopeAudit.Unauthorized {
		errors = append(errors, "unauthorized file: "+path)
	}
	for _, item := range input.ScopeAudit.OwnerUncertain {
		warnings = append(warnings, "owner uncertain: "+item.Path)
	}
	if len(input.ScopeAudit.Ephemeral) > 0 {
		warnings = append(warnings, fmt.Sprintf("ephemeral workspace artifacts observed: %d path(s)", len(input.ScopeAudit.Ephemeral)))
	}

	for _, change := range input.HighRiskChanges {
		severity := change.Severity
		if severity == "" {
			severity = SeverityError
		}
		detail := change.Path
		if change.Reason != "" {
			detail = change.Path + ": " + change.Reason
		}
		if severity == SeverityError {
			errors = append(errors, "high-risk change: "+detail)
		} else {
			warnings = append(warnings, "high-risk change: "+detail)
		}
	}

	for _, check := range input.Checks {
		if !check.Passed {
			errors = append(errors, "integration check failed: "+check.Command)
		}
	}

	verification.Errors = uniqueSorted(errors)
	verification.Warnings = uniqueSorted(warnings)
	verification.PendingMessages = uniqueSorted(verification.PendingMessages)
	sort.SliceStable(verification.HighRiskChanges, func(i, j int) bool {
		if verification.HighRiskChanges[i].Path != verification.HighRiskChanges[j].Path {
			return verification.HighRiskChanges[i].Path < verification.HighRiskChanges[j].Path
		}
		return verification.HighRiskChanges[i].Reason < verification.HighRiskChanges[j].Reason
	})
	sort.SliceStable(verification.Reports, func(i, j int) bool {
		return verification.Reports[i].TaskID < verification.Reports[j].TaskID
	})

	switch {
	case input.Cancelled:
		verification.Result = domain.BarrierCancelled
	case len(verification.Errors) > 0:
		verification.Result = domain.BarrierFailed
	case blocked || len(input.Pending) > 0:
		verification.Result = domain.BarrierBlocked
	case len(verification.Warnings) > 0:
		verification.Result = domain.BarrierPassedWithWarnings
	default:
		verification.Result = domain.BarrierPassed
	}
	return verification
}

func uniqueSorted(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
