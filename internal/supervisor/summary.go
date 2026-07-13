package supervisor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/stall"
	"github.com/vnai/subagent-broker/internal/storage"
	"github.com/vnai/subagent-broker/internal/verify"
	"github.com/vnai/subagent-broker/internal/wave"
)

// RunSummary is the durable final aggregation for Main Agent consumption.
type RunSummary struct {
	SchemaVersion   string                `json:"schema_version"`
	RunID           string                `json:"run_id"`
	RunStatus       domain.RunStatus      `json:"run_status"`
	GeneratedAt     time.Time             `json:"generated_at"`
	Waves           []WaveSummaryEntry    `json:"waves"`
	Tasks           []TaskSummaryEntry    `json:"tasks"`
	ChangedFiles    []string              `json:"changed_files,omitempty"`
	Ephemeral       []string              `json:"ephemeral,omitempty"`
	Unauthorized    []string              `json:"unauthorized,omitempty"`
	OwnerUncertain  []string              `json:"owner_uncertain,omitempty"`
	HighRiskChanges []wave.HighRiskChange `json:"high_risk_changes,omitempty"`
	FailureEvidence []string              `json:"failure_evidence,omitempty"`
	Messages        MessageSummaryEntry   `json:"messages"`
	LastError       string                `json:"last_error,omitempty"`
}

type WaveSummaryEntry struct {
	WaveID          string               `json:"wave_id"`
	Status          domain.WaveStatus    `json:"status"`
	FailureReason   string               `json:"failure_reason,omitempty"`
	BarrierResult   domain.BarrierResult `json:"barrier_result,omitempty"`
	BarrierAccepted bool                 `json:"barrier_accepted,omitempty"`
	AcceptReason    string               `json:"accept_reason,omitempty"`
	PreflightOK     *bool                `json:"preflight_ok,omitempty"`
	Warnings        []string             `json:"warnings,omitempty"`
	Errors          []string             `json:"errors,omitempty"`
}

type TaskSummaryEntry struct {
	TaskID              string            `json:"task_id"`
	Status              string            `json:"status"`
	BlockKind           string            `json:"block_kind,omitempty"`
	ReportPath          string            `json:"report_path,omitempty"`
	FailureEvidencePath string            `json:"failure_evidence_path,omitempty"`
	Attempts            int               `json:"attempts"`
	LastError           string            `json:"last_error,omitempty"`
	WriteScope          []string          `json:"write_scope,omitempty"`
	Stall               *stall.Assessment `json:"stall_assessment,omitempty"`
}

type MessageSummaryEntry struct {
	Pending  int `json:"pending"`
	Answered int `json:"answered"`
	Expired  int `json:"expired"`
	Failed   int `json:"failed"`
}

func (s *Service) buildRunSummary(baseline verify.WorkspaceSnapshot) (RunSummary, error) {
	snap := s.Snapshot()
	summary := RunSummary{
		SchemaVersion: SchemaVersion,
		RunID:         string(snap.Run.RunID),
		RunStatus:     snap.Run.Status,
		GeneratedAt:   time.Now().UTC(),
		LastError:     snap.LastError,
	}

	after, err := verify.CaptureWorkspace(s.projectRoot(), s.config.BrokerHome)
	if err != nil {
		return summary, err
	}
	if baseline.Files != nil {
		summary.ChangedFiles = verify.ChangedFiles(baseline, after)
	} else if s.runBaseline.Files != nil {
		summary.ChangedFiles = verify.ChangedFiles(s.runBaseline, after)
	}
	leases := map[string][]string{}
	for _, runtime := range snap.Tasks {
		leases[string(runtime.Task.TaskID)] = append([]string(nil), runtime.Task.WriteScope...)
	}
	policy, err := s.frozenAuditPolicy()
	if err != nil {
		return summary, fmt.Errorf("resolve audit policy: %w", err)
	}
	audit, err := verify.AuditScopes(summary.ChangedFiles, leases, policy)
	if err != nil {
		return summary, fmt.Errorf("audit run changes: %w", err)
	}
	summary.Unauthorized = append([]string(nil), audit.Unauthorized...)
	for _, item := range audit.Ephemeral {
		summary.Ephemeral = append(summary.Ephemeral, item.Path)
	}
	for _, item := range audit.OwnerUncertain {
		summary.OwnerUncertain = append(summary.OwnerUncertain, item.Path)
	}
	summary.HighRiskChanges = classifyHighRiskChanges(summary.ChangedFiles, audit)

	for _, w := range snap.Waves {
		entry := WaveSummaryEntry{
			WaveID: string(w.WaveID), Status: w.Status,
			FailureReason: w.FailureReason,
			BarrierResult: w.BarrierResult, BarrierAccepted: w.BarrierAccepted, AcceptReason: w.BarrierReason,
		}
		paths := s.wavePaths(w.WaveID)
		if data, err := os.ReadFile(paths.Preflight); err == nil {
			var pref wave.EnvironmentPreflightResult
			if json.Unmarshal(data, &pref) == nil {
				ok := pref.Allowed
				entry.PreflightOK = &ok
			}
		}
		if data, err := os.ReadFile(paths.Verification); err == nil {
			var ver wave.Verification
			if json.Unmarshal(data, &ver) == nil {
				entry.Warnings = append([]string(nil), ver.Warnings...)
				entry.Errors = append([]string(nil), ver.Errors...)
				if ver.Accepted {
					entry.BarrierAccepted = true
					entry.AcceptReason = ver.AcceptReason
				}
			}
		}
		summary.Waves = append(summary.Waves, entry)
	}

	for _, runtime := range snap.Tasks {
		failureEvidencePath := runtime.FailureEvidencePath
		summary.Tasks = append(summary.Tasks, TaskSummaryEntry{
			TaskID: string(runtime.Task.TaskID), Status: string(runtime.Task.Status),
			BlockKind: string(runtime.BlockKind), ReportPath: runtime.ReportPath, FailureEvidencePath: failureEvidencePath,
			Attempts: len(runtime.Attempts), LastError: runtime.LastError,
			WriteScope: append([]string(nil), runtime.Task.WriteScope...),
			Stall:      runtime.Stall,
		})
		if failureEvidencePath != "" {
			summary.FailureEvidence = append(summary.FailureEvidence, failureEvidencePath)
		}
	}

	if s.router != nil {
		all := s.router.Snapshot(true)
		for _, item := range all {
			switch item.Status {
			case message.Answered:
				summary.Messages.Answered++
			case message.Expired:
				summary.Messages.Expired++
			case message.Failed:
				summary.Messages.Failed++
			default:
				if message.IsPending(item.Status) {
					summary.Messages.Pending++
				}
			}
		}
	}
	return summary, nil
}

func (s *Service) writeRunSummary(summary RunSummary) error {
	if err := storage.AtomicWriteJSON(filepath.Join(s.runDir, "summary.json"), summary, 0o600); err != nil {
		return err
	}
	md := renderAggregatedSummary(summary)
	if err := storage.AtomicWriteFile(s.paths.RunSummary, []byte(md), 0o600); err != nil {
		return err
	}
	return storage.AtomicWriteFile(filepath.Join(s.runDir, "summary.md"), []byte(md), 0o600)
}

func renderAggregatedSummary(summary RunSummary) string {
	var b strings.Builder
	b.WriteString("# Run Summary\n\n")
	fmt.Fprintf(&b, "## Overall Status\n\n`%s`\n\n", summary.RunStatus)
	if summary.LastError != "" {
		fmt.Fprintf(&b, "Last error: %s\n\n", summary.LastError)
	}
	b.WriteString("## Waves\n\n")
	for _, w := range summary.Waves {
		fmt.Fprintf(&b, "- `%s`: %s", w.WaveID, w.Status)
		if w.FailureReason != "" {
			fmt.Fprintf(&b, " failure=%q", w.FailureReason)
		}
		if w.BarrierResult != "" {
			fmt.Fprintf(&b, " barrier=`%s`", w.BarrierResult)
		}
		if w.BarrierAccepted {
			fmt.Fprintf(&b, " accepted=%q", w.AcceptReason)
		}
		b.WriteString("\n")
	}
	b.WriteString("\n## Tasks\n\n")
	for _, t := range summary.Tasks {
		fmt.Fprintf(&b, "### %s\n\n- Status: `%s`\n- Attempts: %d\n", t.TaskID, t.Status, t.Attempts)
		if t.ReportPath != "" {
			fmt.Fprintf(&b, "- Report: `%s`\n", t.ReportPath)
		}
		if t.FailureEvidencePath != "" {
			fmt.Fprintf(&b, "- Failure evidence: `%s`\n", t.FailureEvidencePath)
		}
		if len(t.WriteScope) > 0 {
			fmt.Fprintf(&b, "- Scope: %s\n", strings.Join(t.WriteScope, ", "))
		}
		if t.LastError != "" {
			fmt.Fprintf(&b, "- Error: %s\n", t.LastError)
		}
		if t.Stall != nil {
			fmt.Fprintf(&b, "- Progress assessment: `%s`\n- Stall confidence: `%s`\n- Stall reason: %s\n", t.Stall.State, t.Stall.Confidence, t.Stall.Reason)
			for _, evidence := range t.Stall.Evidence {
				fmt.Fprintf(&b, "  - Evidence: %s\n", evidence)
			}
		}
		b.WriteString("\n")
	}
	b.WriteString("## Changed Files\n\n")
	if len(summary.ChangedFiles) == 0 {
		b.WriteString("- None.\n")
	} else {
		for _, f := range summary.ChangedFiles {
			fmt.Fprintf(&b, "- `%s`\n", f)
		}
	}
	b.WriteString("\n## Unauthorized\n\n")
	if len(summary.Unauthorized) == 0 {
		b.WriteString("- None.\n")
	} else {
		for _, f := range summary.Unauthorized {
			fmt.Fprintf(&b, "- `%s`\n", f)
		}
	}
	b.WriteString("\n## Ephemeral Changes\n\n")
	if len(summary.Ephemeral) == 0 {
		b.WriteString("- None.\n")
	} else {
		for _, f := range summary.Ephemeral {
			fmt.Fprintf(&b, "- `%s`\n", f)
		}
	}
	b.WriteString("\n## Owner Uncertain\n\n")
	if len(summary.OwnerUncertain) == 0 {
		b.WriteString("- None.\n")
	} else {
		for _, f := range summary.OwnerUncertain {
			fmt.Fprintf(&b, "- `%s`\n", f)
		}
	}
	b.WriteString("\n## Failure Evidence\n\n")
	if len(summary.FailureEvidence) == 0 {
		b.WriteString("- None.\n")
	} else {
		for _, path := range summary.FailureEvidence {
			fmt.Fprintf(&b, "- `%s`\n", path)
		}
	}
	b.WriteString("\n## High-Risk Changes\n\n")
	if len(summary.HighRiskChanges) == 0 {
		b.WriteString("- None.\n")
	} else {
		for _, h := range summary.HighRiskChanges {
			fmt.Fprintf(&b, "- `%s` (%s): %s\n", h.Path, h.Severity, h.Reason)
		}
	}
	fmt.Fprintf(&b, "\n## Messages\n\n- Pending: %d\n- Answered: %d\n- Expired: %d\n- Failed: %d\n",
		summary.Messages.Pending, summary.Messages.Answered, summary.Messages.Expired, summary.Messages.Failed)
	return b.String()
}
