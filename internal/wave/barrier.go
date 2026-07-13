package wave

import (
	"fmt"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/verify"
)

type CheckResult struct {
	Command string `json:"command"`
	Passed  bool   `json:"passed"`
	Details string `json:"details,omitempty"`
}

type Verification struct {
	SchemaVersion   string               `json:"schema_version"`
	WaveID          domain.WaveID        `json:"wave_id"`
	StartedAt       time.Time            `json:"started_at"`
	EndedAt         time.Time            `json:"ended_at"`
	Result          domain.BarrierResult `json:"result"`
	InputHash       string               `json:"input_hash,omitempty"`
	ChangedFiles    []string             `json:"changed_files"`
	ScopeAudit      verify.ScopeAudit    `json:"scope_audit"`
	Checks          []CheckResult        `json:"checks,omitempty"`
	Warnings        []string             `json:"warnings,omitempty"`
	Errors          []string             `json:"errors,omitempty"`
	PendingMessages []string             `json:"pending_messages,omitempty"`
	HighRiskChanges []HighRiskChange     `json:"high_risk_changes,omitempty"`
	Reports         []ReportAssessment   `json:"reports,omitempty"`
	Accepted        bool                 `json:"accepted,omitempty"`
	AcceptReason    string               `json:"acceptance_reason,omitempty"`
	AcceptedAt      *time.Time           `json:"accepted_at,omitempty"`
	AcceptedBy      string               `json:"accepted_by,omitempty"`
	Rejected        bool                 `json:"rejected,omitempty"`
	RejectReason    string               `json:"rejection_reason,omitempty"`
	RejectedAt      *time.Time           `json:"rejected_at,omitempty"`
	RejectedBy      string               `json:"rejected_by,omitempty"`
}

func RenderBarrier(value Verification) string {
	var b strings.Builder
	b.WriteString("# Wave Barrier\n\n")
	fmt.Fprintf(&b, "- Wave: `%s`\n- Result: `%s`\n- Started: %s\n- Ended: %s\n\n", value.WaveID, value.Result, value.StartedAt.UTC().Format(time.RFC3339), value.EndedAt.UTC().Format(time.RFC3339))
	b.WriteString("## Changed Files\n\n")
	writeBarrierList(&b, value.ChangedFiles, "None.")
	b.WriteString("\n## Ephemeral Changes\n\n")
	if len(value.ScopeAudit.Ephemeral) == 0 {
		b.WriteString("- None.\n")
	} else {
		for _, item := range value.ScopeAudit.Ephemeral {
			fmt.Fprintf(&b, "- `%s` (pattern `%s`)\n", item.Path, item.Pattern)
		}
	}
	b.WriteString("\n## Unauthorized Files\n\n")
	writeBarrierList(&b, value.ScopeAudit.Unauthorized, "None.")
	b.WriteString("\n## Integration Checks\n\n")
	if len(value.Checks) == 0 {
		b.WriteString("- None.\n")
	} else {
		for _, check := range value.Checks {
			status := "failed"
			if check.Passed {
				status = "passed"
			}
			fmt.Fprintf(&b, "- `%s`: %s\n", check.Command, status)
		}
	}
	b.WriteString("\n## Warnings\n\n")
	writeBarrierList(&b, value.Warnings, "None.")
	b.WriteString("\n## Errors\n\n")
	writeBarrierList(&b, value.Errors, "None.")
	if value.Accepted {
		fmt.Fprintf(&b, "\n## Warning Acceptance\n\n- Actor: `%s`\n- Reason: %s\n", value.AcceptedBy, value.AcceptReason)
	}
	if value.Rejected {
		fmt.Fprintf(&b, "\n## Warning Rejection\n\n- Actor: `%s`\n- Reason: %s\n", value.RejectedBy, value.RejectReason)
	}
	return b.String()
}

func writeBarrierList(b *strings.Builder, values []string, fallback string) {
	if len(values) == 0 {
		fmt.Fprintf(b, "- %s\n", fallback)
		return
	}
	for _, value := range values {
		fmt.Fprintf(b, "- `%s`\n", value)
	}
}
