package supervisor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/storage"
	"github.com/vnai/subagent-broker/internal/verify"
)

const FailureEvidenceSchemaVersion = "v1alpha1"

// FailureEvidence is an observation record, not a Result Envelope or a
// verification result. It deliberately describes what was visible while a
// Task/Wave ran without claiming that every path was written by that Task.
type FailureEvidence struct {
	SchemaVersion string    `json:"schema_version"`
	RunID         string    `json:"run_id"`
	WaveID        string    `json:"wave_id"`
	TaskID        string    `json:"task_id"`
	WorkerID      string    `json:"worker_id"`
	AttemptNumber int       `json:"attempt_number"`
	CapturedAt    time.Time `json:"captured_at"`

	FailureStage   string             `json:"failure_stage"`
	ErrorSummary   string             `json:"error_summary"`
	ResultObserved bool               `json:"result_observed"`
	ReportPath     string             `json:"report_path,omitempty"`
	Validation     []ValidationResult `json:"validation,omitempty"`

	Process   FailureProcessEvidence   `json:"process"`
	Activity  FailureActivityEvidence  `json:"activity"`
	Workspace FailureWorkspaceEvidence `json:"workspace"`
}

type FailureProcessEvidence struct {
	ExitObserved          bool     `json:"exit_observed"`
	ExitCode              *int     `json:"exit_code,omitempty"`
	ExitSignal            string   `json:"exit_signal,omitempty"`
	TreeExitConfirmed     bool     `json:"tree_exit_confirmed"`
	ResultingProcessState string   `json:"resulting_process_state,omitempty"`
	RemainingPIDs         []int    `json:"remaining_pids,omitempty"`
	OrphanRisk            bool     `json:"orphan_risk"`
	TerminationErrors     []string `json:"termination_errors,omitempty"`
	TerminationRequested  bool     `json:"termination_requested,omitempty"`
	TerminationInitiator  string   `json:"termination_initiator,omitempty"`
	TerminationPhase      string   `json:"termination_phase,omitempty"`
}

type FailureActivityEvidence struct {
	LastEventAt         time.Time `json:"last_event_at,omitempty"`
	LastProtocolEventAt time.Time `json:"last_protocol_event_at,omitempty"`
	LastStdoutAt        time.Time `json:"last_stdout_at,omitempty"`
	LastStderrAt        time.Time `json:"last_stderr_at,omitempty"`
	LastProgressAt      time.Time `json:"last_progress_at,omitempty"`
	LastToolStartAt     time.Time `json:"last_tool_start_at,omitempty"`
	LastToolFinishAt    time.Time `json:"last_tool_finish_at,omitempty"`
	TurnStartedAt       time.Time `json:"turn_started_at,omitempty"`
	TurnEndedAt         time.Time `json:"turn_ended_at,omitempty"`
	NativeSessionID     string    `json:"native_session_id,omitempty"`
	NativeTurnID        string    `json:"native_turn_id,omitempty"`
}

type FailureWorkspaceEvidence struct {
	BaselineSource string `json:"baseline_source"`
	BaselinePath   string `json:"baseline_path,omitempty"`
	BaselineError  string `json:"baseline_error,omitempty"`
	DiffAvailable  bool   `json:"diff_available"`
	CaptureError   string `json:"capture_error,omitempty"`
	AuditError     string `json:"audit_error,omitempty"`

	ChangedFiles  []string                    `json:"changed_files,omitempty"`
	ObservedPaths []string                    `json:"observed_paths,omitempty"`
	Before        map[string]verify.FileState `json:"before,omitempty"`
	After         map[string]verify.FileState `json:"after,omitempty"`
	ScopeAudit    verify.ScopeAudit           `json:"scope_audit"`
}

type failureEvidencePublication struct {
	TaskID        string
	WorkerID      string
	AttemptNumber int
	FailureStage  string
	JSONPath      string
	JSONHash      string
}

func failureProcessEvidenceFromResolution(resolution WorkerExitResolution) FailureProcessEvidence {
	return FailureProcessEvidence{
		ExitObserved:          resolution.ExitObserved,
		ExitCode:              cloneIntPointer(resolution.ExitCode),
		ExitSignal:            resolution.ExitSignal,
		TreeExitConfirmed:     resolution.TreeExitConfirmed,
		ResultingProcessState: string(resolution.ProcessState),
		RemainingPIDs:         append([]int(nil), resolution.RemainingPIDs...),
		OrphanRisk:            resolution.OrphanRisk,
		TerminationErrors:     append([]string(nil), resolution.Errors...),
		TerminationRequested:  resolution.TerminationRequested,
		TerminationInitiator:  resolution.TerminationInitiator,
		TerminationPhase:      resolution.TerminationPhase,
	}
}

func failureProcessEvidenceFromTerminationResult(result process.TerminationResult, processState string) FailureProcessEvidence {
	return FailureProcessEvidence{
		TreeExitConfirmed:     result.TreeExited || result.PIDReused,
		ResultingProcessState: processState,
		RemainingPIDs:         append([]int(nil), result.RemainingPIDs...),
		OrphanRisk:            result.OrphanRisk,
		TerminationErrors:     append([]string(nil), result.Errors...),
		TerminationRequested:  result.TerminationRequested,
		TerminationInitiator:  result.TerminationInitiator,
		TerminationPhase:      result.TerminationPhase,
	}
}

func cloneIntPointer(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func failureProcessEvidenceFor(runtime *TaskState) FailureProcessEvidence {
	if runtime == nil {
		return FailureProcessEvidence{}
	}
	if runtime.LastProcessEvidence != nil {
		value := *runtime.LastProcessEvidence
		value.ExitCode = cloneIntPointer(value.ExitCode)
		value.RemainingPIDs = append([]int(nil), value.RemainingPIDs...)
		value.TerminationErrors = append([]string(nil), value.TerminationErrors...)
		return value
	}
	value := FailureProcessEvidence{ResultingProcessState: string(runtime.Dimensions.Process)}
	worker := runtime.Worker
	if worker == nil && len(runtime.Attempts) > 0 {
		last := runtime.Attempts[len(runtime.Attempts)-1].Worker
		worker = &last
	}
	if worker != nil && worker.ExitCode != nil {
		value.ExitObserved = true
		value.ExitCode = cloneIntPointer(worker.ExitCode)
	}
	return value
}

func failureActivityEvidenceFor(runtime *TaskState) FailureActivityEvidence {
	if runtime == nil {
		return FailureActivityEvidence{}
	}
	value := FailureActivityEvidence{
		LastProtocolEventAt: runtime.LastProtocolEventAt,
		LastStdoutAt:        runtime.LastStdout,
		LastStderrAt:        runtime.LastStderr,
		LastProgressAt:      runtime.LastProgress,
		LastToolStartAt:     runtime.LastToolStart,
		LastToolFinishAt:    runtime.LastToolFinish,
		TurnStartedAt:       runtime.TurnStartedAt,
		TurnEndedAt:         runtime.TurnEndedAt,
	}
	worker := runtime.Worker
	if worker == nil && len(runtime.Attempts) > 0 {
		last := runtime.Attempts[len(runtime.Attempts)-1].Worker
		worker = &last
	}
	if worker != nil {
		value.LastEventAt = worker.LastEventAt
		value.NativeSessionID = worker.NativeSessionID
		value.NativeTurnID = worker.NativeTurnID
	}
	return value
}

// recordFailureEvidence collects durable facts and publishes the canonical
// JSON before its Markdown projection. Collection failures are represented in
// Workspace rather than returned as a false empty-success record.
func (s *Service) recordFailureEvidence(runtime *TaskState, stage string, cause error, resultObserved bool) (failureEvidencePublication, error) {
	if runtime == nil {
		return failureEvidencePublication{}, fmt.Errorf("failure evidence requires Task state")
	}
	now := time.Now().UTC()
	snapshot := s.Snapshot()
	evidence := FailureEvidence{
		SchemaVersion:  FailureEvidenceSchemaVersion,
		RunID:          string(snapshot.Run.RunID),
		WaveID:         string(runtime.Task.WaveID),
		TaskID:         string(runtime.Task.TaskID),
		WorkerID:       evidenceWorkerID(runtime),
		AttemptNumber:  reportAttemptNumber(runtime),
		CapturedAt:     now,
		FailureStage:   stage,
		ErrorSummary:   errorSummary(cause),
		ResultObserved: resultObserved,
		ReportPath:     runtime.ReportPath,
		Validation:     append([]ValidationResult(nil), runtime.Validation...),
		Process:        failureProcessEvidenceFor(runtime),
		Activity:       failureActivityEvidenceFor(runtime),
		Workspace:      s.collectFailureWorkspaceEvidence(runtime.Task.WaveID),
	}

	taskPaths := failureEvidenceTaskPaths(s, runtime.Task)
	evidenceJSON, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return failureEvidencePublication{}, fmt.Errorf("marshal failure evidence: %w", err)
	}
	evidenceJSON = append(evidenceJSON, '\n')
	hash := sha256.Sum256(evidenceJSON)
	jsonHash := hex.EncodeToString(hash[:])
	if err := storage.AtomicWriteFile(taskPaths.FailureEvidence, evidenceJSON, 0o600); err != nil {
		return failureEvidencePublication{}, fmt.Errorf("publish failure evidence JSON: %w", err)
	}
	// The JSON is canonical. Retain its path in TaskState even if the
	// human-readable projection fails, so the durability failure still points
	// at the artifact that was successfully published.
	runtime.FailureEvidencePath = taskPaths.FailureEvidence
	markdown := renderFailureEvidence(evidence, jsonHash)
	if err := storage.AtomicWriteFile(taskPaths.FailureEvidenceMarkdown, []byte(markdown), 0o600); err != nil {
		return failureEvidencePublication{}, fmt.Errorf("publish failure evidence Markdown: %w", err)
	}

	return failureEvidencePublication{
		TaskID: string(runtime.Task.TaskID), WorkerID: evidence.WorkerID, AttemptNumber: evidence.AttemptNumber,
		FailureStage: stage, JSONPath: taskPaths.FailureEvidence, JSONHash: jsonHash,
	}, nil
}

func (s *Service) appendFailureEvidencePublication(publication failureEvidencePublication) error {
	if err := s.appendEvent(event.Input{
		TaskID: string(publication.TaskID), WorkerID: publication.WorkerID, Source: "supervisor",
		Type: event.FailureEvidencePublished, Severity: "error",
		Payload: map[string]any{
			"task_id": string(publication.TaskID), "worker_id": publication.WorkerID,
			"attempt": publication.AttemptNumber, "failure_stage": publication.FailureStage,
			"json_path": publication.JSONPath, "json_hash": publication.JSONHash,
		},
	}); err != nil {
		return fmt.Errorf("record failure evidence publication: %w", err)
	}
	return nil
}

func errorSummary(err error) string {
	if err == nil {
		return "unknown failure"
	}
	return err.Error()
}

func evidenceWorkerID(runtime *TaskState) string {
	if runtime != nil && runtime.Worker != nil && runtime.Worker.WorkerID != "" {
		return string(runtime.Worker.WorkerID)
	}
	if runtime != nil && len(runtime.Attempts) > 0 && runtime.Attempts[len(runtime.Attempts)-1].Worker.WorkerID != "" {
		return string(runtime.Attempts[len(runtime.Attempts)-1].Worker.WorkerID)
	}
	return "unknown"
}

func failureEvidenceTaskPaths(s *Service, item domain.Task) storage.TaskPaths {
	root := s.taskDir(item)
	snapshot := s.Snapshot()
	paths := storage.TaskPaths{
		Root:                    root,
		FailureEvidence:         filepath.Join(root, "failure-evidence.json"),
		FailureEvidenceMarkdown: filepath.Join(root, "failure-evidence.md"),
	}
	if s.config.BrokerHome == "" || snapshot.Run.ProjectID == "" || snapshot.Run.RunID == "" {
		return paths
	}
	if layout, err := storage.NewLayout(s.config.BrokerHome); err == nil {
		if durable, err := layout.TaskPaths(string(snapshot.Run.ProjectID), string(snapshot.Run.RunID), string(item.TaskID)); err == nil && filepath.Clean(durable.Root) == filepath.Clean(root) {
			return durable
		}
	}
	return paths
}

func (s *Service) collectFailureWorkspaceEvidence(waveID domain.WaveID) FailureWorkspaceEvidence {
	evidence := FailureWorkspaceEvidence{
		BaselineSource: "unavailable",
		Before:         map[string]verify.FileState{},
		After:          map[string]verify.FileState{},
	}
	baseline, source, baselinePath, baselineLoaded, baselineErr := s.loadFailureBaseline(waveID)
	evidence.BaselineSource = source
	evidence.BaselinePath = baselinePath
	if baselineErr != nil {
		evidence.BaselineError = baselineErr.Error()
	}
	root := s.projectRoot()
	if strings.TrimSpace(root) == "" {
		evidence.CaptureError = "project root is unavailable"
		return evidence
	}
	after, err := verify.CaptureWorkspace(root, s.config.BrokerHome)
	if err != nil {
		evidence.CaptureError = err.Error()
		return evidence
	}
	if !baselineLoaded {
		// The current workspace is never used as its own baseline. Keep a
		// visible observation list so an unavailable baseline cannot look like
		// a successful empty diff.
		evidence.ObservedPaths = sortedSnapshotPaths(after)
		return evidence
	}
	evidence.DiffAvailable = true
	evidence.ChangedFiles = verify.ChangedFiles(baseline, after)
	evidence.ObservedPaths = append([]string(nil), evidence.ChangedFiles...)
	for _, path := range evidence.ChangedFiles {
		if value, ok := baseline.Files[path]; ok {
			evidence.Before[path] = value
		}
		if value, ok := after.Files[path]; ok {
			evidence.After[path] = value
		}
	}
	leases := s.failureEvidenceLeases(waveID)
	policy, policyErr := s.frozenAuditPolicy()
	if policyErr != nil {
		evidence.AuditError = policyErr.Error()
		return evidence
	}
	audit, auditErr := verify.AuditScopes(evidence.ChangedFiles, leases, policy)
	if auditErr != nil {
		evidence.AuditError = auditErr.Error()
		return evidence
	}
	evidence.ScopeAudit = audit
	return evidence
}

func (s *Service) loadFailureBaseline(waveID domain.WaveID) (verify.WorkspaceSnapshot, string, string, bool, error) {
	wavePath := ""
	if s.paths.Waves != "" && waveID != "" {
		wavePath = filepath.Join(s.paths.Waves, string(waveID), "baseline.json")
	} else if waveID != "" {
		wavePath = s.wavePaths(waveID).Baseline
	}
	var waveErr error
	if wavePath != "" {
		data, err := os.ReadFile(wavePath)
		if err == nil {
			var baseline verify.WorkspaceSnapshot
			if err := json.Unmarshal(data, &baseline); err == nil {
				return baseline, "wave", wavePath, true, nil
			} else {
				waveErr = fmt.Errorf("decode Wave baseline: %w", err)
			}
		} else {
			waveErr = fmt.Errorf("read Wave baseline: %w", err)
		}
	}
	if s.runBaseline.Files != nil {
		runPath := s.paths.Baseline
		if runPath == "" {
			runPath = filepath.Join(s.runDir, "baseline.json")
		}
		return s.runBaseline, "run_fallback", runPath, true, waveErr
	}
	return verify.WorkspaceSnapshot{}, "unavailable", wavePath, false, waveErr
}

func (s *Service) failureEvidenceLeases(waveID domain.WaveID) map[string][]string {
	leases := map[string][]string{}
	for _, runtime := range s.Snapshot().Tasks {
		if waveID != "" && runtime.Task.WaveID != waveID {
			continue
		}
		leases[string(runtime.Task.TaskID)] = append([]string(nil), runtime.Task.WriteScope...)
	}
	return leases
}

func sortedSnapshotPaths(snapshot verify.WorkspaceSnapshot) []string {
	paths := make([]string, 0, len(snapshot.Files))
	for path := range snapshot.Files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func renderFailureEvidence(value FailureEvidence, jsonHash string) string {
	var b strings.Builder
	b.WriteString("# Failure Evidence\n\n")
	fmt.Fprintf(&b, "- Schema: `%s`\n- Run: `%s`\n- Wave: `%s`\n- Task: `%s`\n- Worker: `%s`\n- Attempt: %d\n- Captured: %s\n- Failure stage: `%s`\n- JSON SHA-256: `%s`\n\n", value.SchemaVersion, value.RunID, value.WaveID, value.TaskID, value.WorkerID, value.AttemptNumber, value.CapturedAt.UTC().Format(time.RFC3339Nano), value.FailureStage, jsonHash)
	fmt.Fprintf(&b, "Error: %s\n\nResult observed: `%t`\nReport path: `%s`\n\n", value.ErrorSummary, value.ResultObserved, value.ReportPath)
	b.WriteString("## Validation\n\n")
	if len(value.Validation) == 0 {
		b.WriteString("- None recorded.\n")
	} else {
		for _, item := range value.Validation {
			status := "failed"
			if item.Passed {
				status = "passed"
			}
			fmt.Fprintf(&b, "- `%s`: %s", item.Command, status)
			if item.Details != "" {
				fmt.Fprintf(&b, " — %s", item.Details)
			}
			b.WriteString("\n")
		}
	}
	b.WriteString("## Process Evidence\n\n")
	fmt.Fprintf(&b, "- Exit observed: `%t`\n- Tree exit confirmed: `%t`\n- Resulting process state: `%s`\n- Orphan risk: `%t`\n- Termination requested: `%t`\n- Termination initiator: `%s`\n- Termination phase: `%s`\n", value.Process.ExitObserved, value.Process.TreeExitConfirmed, value.Process.ResultingProcessState, value.Process.OrphanRisk, value.Process.TerminationRequested, value.Process.TerminationInitiator, value.Process.TerminationPhase)
	if value.Process.ExitCode != nil {
		fmt.Fprintf(&b, "- Exit code: `%d`\n", *value.Process.ExitCode)
	} else {
		b.WriteString("- Exit code: unknown\n")
	}
	if len(value.Process.RemainingPIDs) > 0 {
		fmt.Fprintf(&b, "- Remaining PIDs: `%v`\n", value.Process.RemainingPIDs)
	}
	if len(value.Process.TerminationErrors) > 0 {
		fmt.Fprintf(&b, "- Termination errors: %s\n", strings.Join(value.Process.TerminationErrors, "; "))
	}
	b.WriteString("\n## Activity Evidence\n\n")
	fmt.Fprintf(&b, "- Last protocol event: %s\n- Last stdout: %s\n- Last stderr: %s\n- Last progress: %s\n- Last tool start: %s\n- Last tool finish: %s\n- Turn start: %s\n- Turn end: %s\n- Native session: `%s`\n- Native turn: `%s`\n", evidenceTime(value.Activity.LastProtocolEventAt), evidenceTime(value.Activity.LastStdoutAt), evidenceTime(value.Activity.LastStderrAt), evidenceTime(value.Activity.LastProgressAt), evidenceTime(value.Activity.LastToolStartAt), evidenceTime(value.Activity.LastToolFinishAt), evidenceTime(value.Activity.TurnStartedAt), evidenceTime(value.Activity.TurnEndedAt), value.Activity.NativeSessionID, value.Activity.NativeTurnID)
	b.WriteString("\n## Workspace Evidence\n\n")
	fmt.Fprintf(&b, "- Baseline source: `%s`\n- Baseline path: `%s`\n- Diff available: `%t`\n", value.Workspace.BaselineSource, value.Workspace.BaselinePath, value.Workspace.DiffAvailable)
	if value.Workspace.BaselineError != "" {
		fmt.Fprintf(&b, "- Baseline error: %s\n", value.Workspace.BaselineError)
	}
	if value.Workspace.CaptureError != "" {
		fmt.Fprintf(&b, "- Capture error: %s\n", value.Workspace.CaptureError)
	}
	if value.Workspace.AuditError != "" {
		fmt.Fprintf(&b, "- Audit error: %s\n", value.Workspace.AuditError)
	}
	if value.Workspace.DiffAvailable {
		b.WriteString("\n### Observed Changed Paths\n\n")
		writeFailureEvidencePaths(&b, value.Workspace.ChangedFiles)
	} else {
		b.WriteString("\n### Observed Paths (baseline unavailable)\n\n")
		writeFailureEvidencePaths(&b, value.Workspace.ObservedPaths)
	}
	b.WriteString("\n### Scope Audit\n\n")
	for _, item := range value.Workspace.ScopeAudit.Authorized {
		fmt.Fprintf(&b, "- Authorized candidate `%s`: owners `%s`\n", item.Path, strings.Join(item.Owners, ", "))
	}
	for _, item := range value.Workspace.ScopeAudit.Ephemeral {
		fmt.Fprintf(&b, "- Ephemeral `%s`: pattern `%s`\n", item.Path, item.Pattern)
	}
	for _, path := range value.Workspace.ScopeAudit.Unauthorized {
		fmt.Fprintf(&b, "- Unauthorized `%s`\n", path)
	}
	for _, item := range value.Workspace.ScopeAudit.OwnerUncertain {
		fmt.Fprintf(&b, "- Owner uncertain `%s`: candidates `%s`\n", item.Path, strings.Join(item.Owners, ", "))
	}
	if len(value.Workspace.ScopeAudit.Authorized) == 0 && len(value.Workspace.ScopeAudit.Ephemeral) == 0 && len(value.Workspace.ScopeAudit.Unauthorized) == 0 && len(value.Workspace.ScopeAudit.OwnerUncertain) == 0 {
		b.WriteString("- None.\n")
	}
	b.WriteString("\n### Before/After File State\n\n")
	paths := append([]string(nil), value.Workspace.ChangedFiles...)
	for _, path := range paths {
		before, beforeOK := value.Workspace.Before[path]
		after, afterOK := value.Workspace.After[path]
		fmt.Fprintf(&b, "- `%s`: before=%s after=%s\n", path, formatFileState(before, beforeOK), formatFileState(after, afterOK))
	}
	b.WriteString("\nThis artifact records observed facts. It is not a Result Envelope, formal verification, ownership proof, or automatic adoption of residual files.\n")
	return b.String()
}

func evidenceTime(value time.Time) string {
	if value.IsZero() {
		return "unknown"
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func writeFailureEvidencePaths(b *strings.Builder, paths []string) {
	if len(paths) == 0 {
		b.WriteString("- None recorded; see `diff_available` and capture/baseline errors above.\n")
		return
	}
	for _, path := range paths {
		fmt.Fprintf(b, "- `%s`\n", path)
	}
}

func formatFileState(value verify.FileState, present bool) string {
	if !present {
		return "<absent>"
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "<unavailable>"
	}
	return "`" + string(raw) + "`"
}
