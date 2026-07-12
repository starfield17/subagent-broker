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

	_, metaErr := os.Stat(metaPath)
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

	assessment.Present = true

	// Prefer full disk integrity verification (meta↔envelope binding + hashes).
	verifiedMeta, envelope, verifyErr := report.VerifyDiskArtifacts(taskDir)
	if verifyErr != nil {
		assessment.MetaValid = false
		assessment.MarkdownValid = false
		assessment.IdentityValid = false
		assessment.Error = fmt.Sprintf("task %s report integrity failed: %v", runtime.Task.TaskID, verifyErr)
		return assessment
	}
	meta := verifiedMeta

	// Canonical envelope is the source of status, not an unbound meta field.
	assessment.EnvelopeStatus = envelope.Status
	if assessment.EnvelopeStatus == "" {
		assessment.EnvelopeStatus = meta.Status
	}
	assessment.MetaValid = meta.SchemaVersion == report.SchemaVersion && meta.TaskID != ""

	// Legacy reports without attempt/hash binding are not Phase 1–3 passable.
	if meta.Unverified || meta.AttemptNumber <= 0 || meta.EnvelopeHash == "" || meta.MarkdownHash == "" {
		assessment.MetaValid = false
		assessment.MarkdownValid = false
		assessment.IdentityValid = false
		assessment.Error = fmt.Sprintf("task %s report is legacy/unverified (missing attempt or content hashes)", runtime.Task.TaskID)
		return assessment
	}

	// Identity: disk meta must match the frozen ReportIdentity from publication time,
	// and that attempt must still exist in history with the same worker id.
	// Do NOT bind to a dynamic current/latest ActiveAttempt.
	if meta.TaskID != string(runtime.Task.TaskID) || envelope.TaskID != string(runtime.Task.TaskID) {
		assessment.IdentityValid = false
		assessment.Error = fmt.Sprintf("task %s report identity mismatch: meta task_id=%q envelope task_id=%q", runtime.Task.TaskID, meta.TaskID, envelope.TaskID)
	} else if runtime.ReportIdentity == nil {
		assessment.IdentityValid = false
		assessment.Error = fmt.Sprintf("task %s has no frozen ReportIdentity; report cannot be verified", runtime.Task.TaskID)
	} else if runtime.ReportIdentity.Stale {
		assessment.IdentityValid = false
		assessment.Error = fmt.Sprintf("task %s ReportIdentity is stale after a newer attempt", runtime.Task.TaskID)
	} else if meta.WorkerID != runtime.ReportIdentity.WorkerID || meta.AttemptNumber != runtime.ReportIdentity.AttemptNumber ||
		envelope.WorkerID != runtime.ReportIdentity.WorkerID {
		assessment.IdentityValid = false
		assessment.Error = fmt.Sprintf(
			"task %s report meta worker=%q attempt=%d does not match frozen ReportIdentity worker=%s attempt=%d",
			runtime.Task.TaskID, meta.WorkerID, meta.AttemptNumber,
			runtime.ReportIdentity.WorkerID, runtime.ReportIdentity.AttemptNumber,
		)
	} else if meta.EnvelopeHash != runtime.ReportIdentity.EnvelopeHash || meta.MarkdownHash != runtime.ReportIdentity.MarkdownHash {
		assessment.IdentityValid = false
		assessment.Error = fmt.Sprintf("task %s report hashes do not match frozen ReportIdentity", runtime.Task.TaskID)
	} else if !attemptExists(runtime, meta.WorkerID, meta.AttemptNumber) {
		assessment.IdentityValid = false
		assessment.Error = fmt.Sprintf(
			"task %s report attempt %d worker %q not found in Attempts history",
			runtime.Task.TaskID, meta.AttemptNumber, meta.WorkerID,
		)
	} else {
		assessment.IdentityValid = true
	}

	// Markdown integrity already verified via hash; also require non-empty body + envelope status line.
	statusForMD := string(envelope.Status)
	if statusForMD == "" {
		statusForMD = string(meta.Status)
	}
	assessment.MarkdownValid = len(strings.TrimSpace(string(mdData))) > 0 &&
		strings.Contains(string(mdData), statusForMD) &&
		meta.MarkdownHash == report.HashBytes(mdData)
	if !assessment.MarkdownValid && assessment.Error == "" {
		assessment.Error = fmt.Sprintf("task %s report.md integrity or status mismatch", runtime.Task.TaskID)
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

// attemptExists confirms the frozen report's attempt is still in durable history.
func attemptExists(runtime TaskState, workerID string, attemptNumber int) bool {
	if workerID == "" || attemptNumber <= 0 {
		return false
	}
	for _, a := range runtime.Attempts {
		if a.Number == attemptNumber && string(a.Worker.WorkerID) == workerID {
			return true
		}
	}
	// Legacy single-worker projection without Attempts.
	if len(runtime.Attempts) == 0 && runtime.Worker != nil {
		number := runtime.Worker.Attempt
		if number <= 0 {
			number = 1
		}
		return string(runtime.Worker.WorkerID) == workerID && number == attemptNumber
	}
	return false
}

func (s *Service) collectPendingDecisions(planned domain.WavePlan) []wave.PendingDecision {
	if s.router == nil {
		return nil
	}
	var pending []wave.PendingDecision
	for _, item := range planned.Tasks {
		for _, msg := range s.router.PendingDecisions(string(item.TaskID)) {
			pending = append(pending, wave.PendingDecision{
				MessageID: msg.MessageID, TaskID: msg.TaskID, Type: msg.Type,
			})
		}
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

// revalidateBarrierCurrentFacts re-collects current Barrier inputs and compares
// their hash to the stored verification InputHash. Old barrier-input.json is never
// treated as current fact.
func (s *Service) revalidateBarrierCurrentFacts(ctx context.Context, waveID domain.WaveID, expectedHash string) error {
	if strings.TrimSpace(expectedHash) == "" {
		return fmt.Errorf("stale_verification: wave %s verification lacks input_hash; re-run barrier", waveID)
	}
	planned, ok := s.plannedWave(waveID)
	if !ok {
		return fmt.Errorf("stale_verification: wave %s plan not found", waveID)
	}
	paths := s.wavePaths(waveID)
	baselineData, err := os.ReadFile(paths.Baseline)
	if err != nil {
		return fmt.Errorf("stale_verification: wave %s baseline missing; re-run barrier: %w", waveID, err)
	}
	var baseline verify.WorkspaceSnapshot
	if err := json.Unmarshal(baselineData, &baseline); err != nil {
		return fmt.Errorf("stale_verification: wave %s baseline invalid: %w", waveID, err)
	}
	currentInputs, err := s.collectBarrierInputs(ctx, planned, baseline)
	if err != nil {
		return fmt.Errorf("stale_verification: collect current barrier inputs: %w", err)
	}
	currentInputs.ExistingErrors = append(currentInputs.ExistingErrors, s.collectQueuedInstructionErrors(planned)...)
	currentHash := hashBarrierInputs(currentInputs)
	if currentHash != expectedHash {
		return fmt.Errorf("stale_verification: barrier inputs changed since evaluation (conflict); re-run barrier")
	}
	return nil
}

func (s *Service) plannedWave(waveID domain.WaveID) (domain.WavePlan, bool) {
	for _, planned := range s.plan.Waves {
		if planned.WaveID == waveID {
			return planned, true
		}
	}
	return domain.WavePlan{}, false
}
