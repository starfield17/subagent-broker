package supervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/verify"
	"github.com/vnai/subagent-broker/internal/wave"
)

// collectBarrierInputs rebuilds Barrier inputs from durable disk facts and Router.
func (s *Service) collectBarrierInputs(ctx context.Context, planned domain.WavePlan, baseline verify.WorkspaceSnapshot) (wave.BarrierInputs, error) {
	cancelled := false
	reports := make([]wave.ReportAssessment, 0, len(planned.Tasks))
	for _, item := range planned.Tasks {
		runtime, ok := s.taskState(item.TaskID)
		if !ok {
			reports = append(reports, wave.ReportAssessment{
				TaskID: item.TaskID, Present: false, Error: "missing task state: " + string(item.TaskID),
			})
			continue
		}
		if runtime.Task.Status == state.TaskCancelled {
			cancelled = true
		}
		reports = append(reports, s.assessTaskReport(runtime))
	}

	pending := s.collectPendingDecisions(planned)
	checks := make([]wave.CheckResult, 0, len(planned.IntegrationChecks))
	for index, check := range planned.IntegrationChecks {
		logPath := filepath.Join(s.wavePaths(planned.WaveID).Root, fmt.Sprintf("check-%03d.log", index+1))
		checks = append(checks, s.runCheck(ctx, check, logPath))
	}

	after, err := verify.CaptureWorkspace(s.projectRoot(), s.config.BrokerHome)
	if err != nil {
		return wave.BarrierInputs{}, err
	}
	changed := verify.ChangedFiles(baseline, after)
	leases := map[string][]string{}
	for _, item := range planned.Tasks {
		if runtime, ok := s.taskState(item.TaskID); ok {
			leases[string(item.TaskID)] = append([]string(nil), runtime.Task.WriteScope...)
		}
	}
	scopeAudit, err := verify.AuditScopes(changed, leases)
	if err != nil {
		return wave.BarrierInputs{}, err
	}
	highRisk := classifyHighRiskChanges(changed, scopeAudit)

	return wave.BarrierInputs{
		WaveID:          planned.WaveID,
		Cancelled:       cancelled,
		Reports:         reports,
		Pending:         pending,
		ChangedFiles:    changed,
		ScopeAudit:      scopeAudit,
		HighRiskChanges: highRisk,
		Checks:          checks,
	}, nil
}

func (s *Service) assessTaskReport(runtime TaskState) wave.ReportAssessment {
	assessment := wave.ReportAssessment{
		TaskID:        runtime.Task.TaskID,
		RuntimeStatus: runtime.Task.Status,
	}
	taskDir := s.taskDir(runtime.Task)
	metaPath := filepath.Join(taskDir, "report.meta.json")
	mdPath := filepath.Join(taskDir, "report.md")

	metaData, metaErr := os.ReadFile(metaPath)
	mdData, mdErr := os.ReadFile(mdPath)
	if metaErr != nil && mdErr != nil {
		// Tasks that never reached report publication still fail Barrier unless cancelled with no work.
		if runtime.Task.Status == state.TaskCancelled || runtime.Task.Status == state.TaskFailed {
			assessment.Present = false
			assessment.Error = fmt.Sprintf("task %s is %s without durable report", runtime.Task.TaskID, runtime.Task.Status)
			return assessment
		}
		assessment.Present = false
		assessment.Error = fmt.Sprintf("task %s report is missing", runtime.Task.TaskID)
		return assessment
	}
	if metaErr != nil {
		assessment.Present = false
		assessment.Error = fmt.Sprintf("task %s report.meta.json missing: %v", runtime.Task.TaskID, metaErr)
		return assessment
	}
	if mdErr != nil {
		assessment.Present = true
		assessment.MetaValid = true
		assessment.MarkdownValid = false
		assessment.Error = fmt.Sprintf("task %s report.md missing: %v", runtime.Task.TaskID, mdErr)
		return assessment
	}

	var meta report.Meta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		assessment.Present = true
		assessment.MetaValid = false
		assessment.Error = fmt.Sprintf("task %s report.meta.json invalid: %v", runtime.Task.TaskID, err)
		return assessment
	}
	assessment.Present = true
	assessment.MetaValid = meta.SchemaVersion == report.SchemaVersion && meta.TaskID != ""
	assessment.EnvelopeStatus = meta.Status

	// Identity: meta task_id must match; worker_id should match current or historical attempt.
	if meta.TaskID != string(runtime.Task.TaskID) {
		assessment.IdentityValid = false
		assessment.Error = fmt.Sprintf("task %s report identity mismatch: meta task_id=%q", runtime.Task.TaskID, meta.TaskID)
	} else {
		assessment.IdentityValid = workerMatches(runtime, meta.WorkerID)
		if !assessment.IdentityValid {
			assessment.Error = fmt.Sprintf("task %s report worker_id %q does not match attempts", runtime.Task.TaskID, meta.WorkerID)
		}
	}

	// Markdown must match meta-rendered content when we can load envelope from meta-adjacent storage.
	// We only have status in Meta; re-render check uses Status line presence + non-empty body.
	assessment.MarkdownValid = len(strings.TrimSpace(string(mdData))) > 0 &&
		strings.Contains(string(mdData), string(meta.Status))
	if !assessment.MarkdownValid && assessment.Error == "" {
		assessment.Error = fmt.Sprintf("task %s report.md does not match meta status", runtime.Task.TaskID)
	}

	// Incomplete runtime status still fails when report claims success but task not verified.
	switch runtime.Task.Status {
	case state.TaskVerifiedSuccess, state.TaskVerifiedPartial, state.TaskBlocked, state.TaskCancelled,
		state.TaskFailed, state.TaskVerificationFailed, state.TaskReportedComplete:
	default:
		if assessment.Error == "" {
			assessment.Error = fmt.Sprintf("task %s is not in a barrier-ready status: %s", runtime.Task.TaskID, runtime.Task.Status)
		}
	}
	return assessment
}

func workerMatches(runtime TaskState, workerID string) bool {
	if workerID == "" {
		return false
	}
	if runtime.Worker != nil && string(runtime.Worker.WorkerID) == workerID {
		return true
	}
	for _, attempt := range runtime.Attempts {
		if string(attempt.Worker.WorkerID) == workerID {
			return true
		}
	}
	return runtime.Worker == nil && len(runtime.Attempts) == 0
}

func (s *Service) collectPendingDecisions(planned domain.WavePlan) []wave.PendingDecision {
	if s.router == nil {
		return nil
	}
	taskSet := map[string]bool{}
	for _, item := range planned.Tasks {
		taskSet[string(item.TaskID)] = true
	}
	var pending []wave.PendingDecision
	for _, item := range planned.Tasks {
		for _, msg := range s.router.PendingDecisions(string(item.TaskID)) {
			pending = append(pending, wave.PendingDecision{
				MessageID: msg.MessageID, TaskID: msg.TaskID, Type: msg.Type,
			})
		}
		// Queued instructions on a verified/partial task are consistency errors handled via ExistingErrors by caller.
		_ = taskSet
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].TaskID == pending[j].TaskID {
			return pending[i].MessageID < pending[j].MessageID
		}
		return pending[i].TaskID < pending[j].TaskID
	})
	return pending
}

func classifyHighRiskChanges(changed []string, audit verify.ScopeAudit) []wave.HighRiskChange {
	unauthorized := map[string]bool{}
	for _, path := range audit.Unauthorized {
		unauthorized[path] = true
	}
	ownerUncertain := map[string]bool{}
	for _, item := range audit.OwnerUncertain {
		ownerUncertain[item.Path] = true
	}
	var out []wave.HighRiskChange
	for _, match := range verify.ClassifyHighRisk(changed) {
		severity := wave.SeverityWarning
		reason := match.Reason
		if unauthorized[match.Path] {
			severity = wave.SeverityError
			reason = match.Reason + " (unauthorized)"
		} else if ownerUncertain[match.Path] {
			severity = wave.SeverityError
			reason = match.Reason + " (owner uncertain)"
		}
		out = append(out, wave.HighRiskChange{Path: match.Path, Severity: severity, Reason: reason})
	}
	// Unauthorized non-high-risk already fail via ScopeAudit.Unauthorized in evaluator.
	return out
}

func hashBarrierInputs(input wave.BarrierInputs) string {
	raw, _ := json.Marshal(input)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// collectQueuedInstructionErrors flags consistency issues for completed tasks with leftover outbox.
func (s *Service) collectQueuedInstructionErrors(planned domain.WavePlan) []string {
	if s.router == nil {
		return nil
	}
	var errors []string
	for _, item := range planned.Tasks {
		runtime, ok := s.taskState(item.TaskID)
		if !ok {
			continue
		}
		pending := s.router.PendingInstructions(string(item.TaskID))
		if len(pending) == 0 {
			continue
		}
		if recoveryTaskTerminal(runtime) || runtime.Task.Status == state.TaskVerifiedSuccess || runtime.Task.Status == state.TaskVerifiedPartial {
			for _, msg := range pending {
				errors = append(errors, fmt.Sprintf("task %s is terminal/complete but has queued instruction %s", item.TaskID, msg.MessageID))
			}
		} else if runtime.Task.Status == state.TaskReportedComplete || runtime.Task.Status == state.TaskVerifying {
			for _, msg := range pending {
				errors = append(errors, fmt.Sprintf("task %s reported complete with undelivered instruction %s", item.TaskID, msg.MessageID))
			}
		}
	}
	return errors
}
