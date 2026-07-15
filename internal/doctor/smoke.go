package doctor

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/task"
	"github.com/vnai/subagent-broker/internal/verify"
)

func runSmoke(parent context.Context, runID string, harness adapter.Adapter, descriptor adapter.Descriptor, item HarnessResult, cfg Config, evidenceDir string) HarnessResult {
	started := cfg.Now().UTC()
	item.StartedAt = started
	item.Stages = map[string]StageResult{"probe": {Status: item.ProbeStatus}}
	item.ProtocolSmokeStatus = "failed"
	item.IdentityStatus = IdentityUnavailable
	item.WorkspaceStatus = "failed"
	item.CleanupStatus = "failed"
	item.OverallStatus = "failed"

	harnessDir := filepath.Join(evidenceDir, safeSegment(string(descriptor.Name)))
	item.Artifacts.HarnessDir = harnessDir
	item.Artifacts.EvidenceJSON = filepath.Join(harnessDir, "evidence.json")
	item.Artifacts.EvidenceMarkdown = filepath.Join(harnessDir, "evidence.md")
	item.Artifacts.NormalizedEvents = filepath.Join(harnessDir, "normalized-events.jsonl")
	item.Artifacts.Stderr = filepath.Join(harnessDir, "stderr.log")
	if err := os.MkdirAll(harnessDir, 0o700); err != nil {
		item.Errors = append(item.Errors, sanitizeText(fmt.Sprintf("create Harness evidence directory: %v", err)))
		return finishSmoke(item, cfg.Now())
	}

	if !smokeProbeUsable(item.Probe) {
		item.Errors = append(item.Errors, "live smoke requires installed Harness with explicitly usable authentication")
		item.Stages["probe"] = StageResult{Status: "failed", Error: item.Errors[len(item.Errors)-1]}
		return finishSmoke(item, cfg.Now())
	}

	workspace, err := os.MkdirTemp("", "subagent-broker-doctor-")
	if err != nil {
		item.Errors = append(item.Errors, sanitizeText(fmt.Sprintf("create isolated smoke workspace: %v", err)))
		return finishSmoke(item, cfg.Now())
	}
	item.Artifacts.Workspace = workspace
	item.Workspace.Root = workspace
	if err := os.WriteFile(filepath.Join(workspace, "README.md"), []byte("Doctor smoke workspace.\n"), 0o600); err != nil {
		item.Errors = append(item.Errors, sanitizeText(fmt.Sprintf("write smoke marker: %v", err)))
		item.Stages["workspace_setup"] = StageResult{Status: "failed", Error: item.Errors[len(item.Errors)-1]}
		return finishSmokeWithWorkspace(item, cfg, workspace, verify.WorkspaceSnapshot{}, nil, false)
	}
	before, err := verify.CaptureWorkspace(workspace)
	if err != nil {
		item.Errors = append(item.Errors, sanitizeText(fmt.Sprintf("capture smoke baseline: %v", err)))
		item.Stages["workspace_setup"] = StageResult{Status: "failed", Error: item.Errors[len(item.Errors)-1]}
		return finishSmokeWithWorkspace(item, cfg, workspace, verify.WorkspaceSnapshot{}, nil, false)
	}
	item.Workspace.Before = before
	item.Stages["workspace_setup"] = StageResult{Status: "passed"}

	taskID, workerID, err := smokeIDs(runID, descriptor.Name, cfg.Now())
	if err != nil {
		item.Errors = append(item.Errors, sanitizeText(err.Error()))
		return finishSmokeWithWorkspace(item, cfg, workspace, before, nil, false)
	}
	item.TaskWorker.ExpectedTaskID = taskID
	item.TaskWorker.ExpectedWorkerID = workerID
	contract, err := smokeContract(taskID, workerID, workspace, runID, cfg.Now())
	if err != nil {
		item.Errors = append(item.Errors, sanitizeText(fmt.Sprintf("render smoke contract: %v", err)))
		return finishSmokeWithWorkspace(item, cfg, workspace, before, nil, false)
	}

	smokeCtx, cancel := context.WithTimeout(parent, cfg.Timeout)
	defer cancel()
	session, err := harness.StartSession(smokeCtx, adapter.StartRequest{
		RunID: runID, TaskID: taskID, WorkerID: workerID, ProjectRoot: workspace,
		Contract: contract, Model: cfg.Model, Scenario: cfg.Scenario,
		Options: map[string]string{"doctor_smoke": "true"},
	})
	if err != nil {
		item.Errors = append(item.Errors, sanitizeText(fmt.Sprintf("start session: %v", err)))
		item.Stages["session_start"] = StageResult{Status: "failed", Error: item.Errors[len(item.Errors)-1]}
		return finishSmokeWithWorkspace(item, cfg, workspace, before, nil, false)
	}
	item.NativeSessionID = session.NativeSessionID
	item.NativeTurnID = session.NativeTurnID
	item.ProcessIdentity = process.Identity{
		PID: session.PID, StartToken: session.ProcessStartToken, ProcessGroupToken: session.ProcessGroupToken,
	}
	item.Stages["session_start"] = StageResult{Status: "passed"}

	driver := smokeSessionDriver{harness: harness, session: session, taskID: taskID, workerID: workerID}
	driver.run(smokeCtx, &item)

	// Query identity after the result boundary/collection, while the adapter's
	// session state is still available. A missing fact remains unavailable.
	driver.refreshIdentity(smokeCtx, cfg.Model, descriptor.Name, &item)

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cleanupTimeout(cfg.Timeout))
	driver.cleanup(cleanupCtx, cfg, &item)
	cleanupCancel()
	item.Events = append([]string(nil), driver.normalized...)
	item.normalizedEventsLog = strings.Join(driver.normalizedLog, "")
	item.stderrLog = redactLog(driver.stderr.String())
	item.CapabilityEvidence = CapabilityEvidenceForSmoke(descriptor.Capabilities, item.Probe.Capabilities, item.Stages, item.Events)
	if len(item.CapabilityEvidence.Contradicted) > 0 {
		item.Errors = append(item.Errors, "live smoke contradicted declared capability: "+strings.Join(item.CapabilityEvidence.Contradicted, ", "))
	}

	after, captureErr := verify.CaptureWorkspace(workspace)
	if captureErr != nil {
		item.Errors = append(item.Errors, sanitizeText(fmt.Sprintf("capture final smoke workspace: %v", captureErr)))
		item.Stages["workspace_final"] = StageResult{Status: "failed", Error: item.Errors[len(item.Errors)-1]}
	} else {
		item.Workspace.After = after
		item.Workspace.ChangedPaths = verify.ChangedFiles(before, after)
		if len(item.Workspace.ChangedPaths) == 0 {
			item.WorkspaceStatus = "passed"
			item.Stages["workspace_final"] = StageResult{Status: "passed"}
		} else {
			item.WorkspaceStatus = "failed"
			item.Stages["workspace_final"] = StageResult{Status: "failed", Error: "isolated smoke workspace changed"}
			item.Errors = append(item.Errors, "isolated smoke workspace changed")
		}
	}

	workspaceClean := captureErr == nil && len(item.Workspace.ChangedPaths) == 0
	if item.CleanupStatus == "passed" && workspaceClean && !cfg.KeepWorkspace {
		if err := os.RemoveAll(workspace); err != nil {
			item.Cleanup.Errors = append(item.Cleanup.Errors, sanitizeText(fmt.Sprintf("remove smoke workspace: %v", err)))
			item.CleanupStatus = "failed"
			item.Workspace.Retained = true
		} else {
			item.Workspace.Removed = true
		}
	} else {
		item.Workspace.Retained = true
	}
	if cfg.KeepWorkspace {
		item.Workspace.Retained = true
	}

	item.ProtocolSmokeStatus = protocolStatus(item)
	if item.IdentityStatus == "" {
		item.IdentityStatus = IdentityUnavailable
	}
	item.OverallStatus = "passed"
	for _, status := range []string{item.ProbeStatus, item.ProtocolSmokeStatus, item.WorkspaceStatus, item.CleanupStatus} {
		if status != "passed" {
			item.OverallStatus = "failed"
		}
	}
	if item.IdentityStatus == IdentityMismatch {
		item.OverallStatus = "failed"
	}
	return finishSmoke(item, cfg.Now())
}

type smokeSessionDriver struct {
	harness  adapter.Adapter
	session  adapter.Session
	taskID   string
	workerID string

	exitObserved  bool
	exit          adapter.ExitStatus
	resultSeen    bool
	turnFailed    bool
	timedOut      bool
	normalized    []string
	normalizedLog []string
	stderr        bytes.Buffer
	result        report.Envelope
	resultErr     error
	identity      adapter.RuntimeIdentity
	identityErr   error
}

// run is the only reader of Session.Events, Session.Stderr, and Session.Exited
// for the entire smoke lifecycle. Cleanup continues draining those same
// channels synchronously after result collection.
func (d *smokeSessionDriver) run(ctx context.Context, item *HarnessResult) {
	events, stderr, exited := d.session.Events, d.session.Stderr, d.session.Exited
	for events != nil || stderr != nil || exited != nil {
		select {
		case <-ctx.Done():
			d.timedOut = true
			item.Errors = append(item.Errors, "live smoke timed out")
			item.Stages["timeout"] = StageResult{Status: "failed", Error: "live smoke timed out"}
			events, stderr, exited = nil, nil, nil
		case native, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			input, err := d.harness.NormalizeEvent(native)
			if err != nil {
				// Preserve an explicit native terminal boundary even when its
				// payload cannot be normalized. Collection/validation then records
				// the failure with ResultObserved=true rather than downgrading it
				// to a pre-result missing-boundary failure.
				d.normalized = appendUniqueString(d.normalized, native.Kind)
				d.normalizedLog = append(d.normalizedLog, normalizedLogLine(native.Kind, native.Timestamp))
				item.Errors = append(item.Errors, sanitizeText(fmt.Sprintf("normalize event %s: %v", native.Kind, err)))
				item.Stages["event_stream"] = StageResult{Status: "failed", Error: item.Errors[len(item.Errors)-1]}
				if native.Kind == event.ResultSubmitted && currentTurnEvent(native, d.session.NativeTurnID) {
					d.resultSeen = true
					item.Stages["result_boundary"] = StageResult{Status: "passed"}
					goto collected
				}
				if native.Kind == event.TurnFailed && currentTurnEvent(native, d.session.NativeTurnID) {
					d.turnFailed = true
					item.Stages["result_boundary"] = StageResult{Status: "failed", Error: "Harness emitted TurnFailed"}
					goto collected
				}
				continue
			}
			kind := input.Type
			if kind == "" {
				kind = native.Kind
			}
			d.normalized = appendUniqueString(d.normalized, kind)
			d.normalizedLog = append(d.normalizedLog, normalizedLogLine(kind, native.Timestamp))
			if native.Kind == event.ResultSubmitted && currentTurnEvent(native, d.session.NativeTurnID) {
				d.resultSeen = true
				item.Stages["event_stream"] = StageResult{Status: "passed"}
				item.Stages["result_boundary"] = StageResult{Status: "passed"}
				goto collected
			}
			if native.Kind == event.TurnFailed && currentTurnEvent(native, d.session.NativeTurnID) {
				d.turnFailed = true
				item.Stages["result_boundary"] = StageResult{Status: "failed", Error: "Harness emitted TurnFailed"}
				goto collected
			}
		case chunk, ok := <-stderr:
			if !ok {
				stderr = nil
				continue
			}
			if d.stderr.Len() < 1024*1024 {
				d.stderr.Write(chunk.Data)
			}
		case status, ok := <-exited:
			d.exitObserved = true
			if ok {
				d.exit = status
			} else {
				d.exit = adapter.ExitStatus{Code: -1, Error: "session exited without status"}
			}
			exited = nil
			// Exited can race with already-published terminal events. Keep the
			// single owner draining Events until the stream closes or the
			// authoritative current-turn boundary is observed.
		}
	}

collected:
	if !d.resultSeen {
		if d.timedOut {
			return
		}
		if d.turnFailed {
			item.Errors = append(item.Errors, "Harness emitted TurnFailed")
			return
		}
		item.Errors = append(item.Errors, "process exited without current-turn ResultSubmitted")
		return
	}
	result, err := d.harness.CollectFinalResult(ctx, d.session.NativeSessionID)
	d.result = result
	d.resultErr = err
	item.Result.Observed = true
	if err != nil {
		item.Errors = append(item.Errors, sanitizeText(fmt.Sprintf("collect final result: %v", err)))
		item.Stages["result_collection"] = StageResult{Status: "failed", Error: item.Errors[len(item.Errors)-1]}
		return
	}
	item.Stages["result_collection"] = StageResult{Status: "passed"}
	item.Result.Status = string(result.Status)
	item.Result.TaskID = result.TaskID
	item.Result.WorkerID = result.WorkerID
	item.Result.FilesChanged = append([]string(nil), result.FilesChanged...)
	raw, marshalErr := json.Marshal(result)
	if marshalErr == nil {
		digest := sha256.Sum256(raw)
		item.Result.SHA256 = hex.EncodeToString(digest[:])
	}
	item.TaskWorker.ObservedTaskID = result.TaskID
	item.TaskWorker.ObservedWorkerID = result.WorkerID
	item.TaskWorker.TaskIDMatch = result.TaskID == d.taskID
	item.TaskWorker.WorkerIDMatch = result.WorkerID == d.workerID
	if validateErr := report.ValidateEnvelope(result); validateErr != nil {
		item.Errors = append(item.Errors, sanitizeText(fmt.Sprintf("invalid final envelope: %v", validateErr)))
		item.Stages["result_validation"] = StageResult{Status: "failed", Error: item.Errors[len(item.Errors)-1]}
		return
	}
	if !item.TaskWorker.TaskIDMatch || !item.TaskWorker.WorkerIDMatch {
		item.Errors = append(item.Errors, "result Task/Worker identity mismatch")
		item.Stages["result_validation"] = StageResult{Status: "failed", Error: "result Task/Worker identity mismatch"}
		return
	}
	if result.Status != report.StatusSucceeded {
		item.Errors = append(item.Errors, fmt.Sprintf("smoke expected succeeded result, got %s", result.Status))
		item.Stages["result_validation"] = StageResult{Status: "failed", Error: item.Errors[len(item.Errors)-1]}
		return
	}
	if len(result.FilesChanged) != 0 {
		item.Errors = append(item.Errors, "result reported changed files")
		item.Stages["result_validation"] = StageResult{Status: "failed", Error: item.Errors[len(item.Errors)-1]}
		return
	}
	item.Stages["result_validation"] = StageResult{Status: "passed"}
}

func (d *smokeSessionDriver) refreshIdentity(ctx context.Context, requested string, harness adapter.HarnessName, item *HarnessResult) {
	d.identity = adapter.RuntimeIdentity{RequestedModel: requested, ProviderSource: adapter.EvidenceUnavailable, ModelSource: adapter.EvidenceUnavailable}
	provider, ok := d.harness.(adapter.RuntimeIdentityProvider)
	if !ok {
		d.identityErr = fmt.Errorf("adapter does not expose runtime identity")
	} else {
		identity, err := provider.RuntimeIdentity(ctx, d.session.NativeSessionID)
		if err != nil {
			d.identityErr = err
		} else {
			identity.RequestedModel = requested
			if identity.ProviderSource == "" || identity.ObservedProvider == "" {
				identity.ProviderSource = adapter.EvidenceUnavailable
			}
			if identity.ModelSource == "" || identity.ObservedModel == "" {
				identity.ModelSource = adapter.EvidenceUnavailable
			}
			d.identity = identity
		}
	}
	item.RuntimeIdentity = assessIdentity(harness, requested, d.identity, d.identityErr)
	item.IdentityStatus = aggregateIdentityStatus(item.RuntimeIdentity)
	item.Stages["identity"] = StageResult{Status: "passed"}
	if item.IdentityStatus == IdentityMismatch {
		item.Stages["identity"] = StageResult{Status: "failed", Error: "runtime provider/model identity mismatch"}
		item.Errors = append(item.Errors, "runtime provider/model identity mismatch")
	}
}

func (d *smokeSessionDriver) cleanup(ctx context.Context, cfg Config, item *HarnessResult) {
	identity := process.Identity{PID: d.session.PID, StartToken: d.session.ProcessStartToken, ProcessGroupToken: d.session.ProcessGroupToken}
	if d.session.PID > 0 {
		if inspected, err := process.Inspect(ctx, d.session.PID); err == nil {
			identity = inspected
		}
	}
	item.ProcessIdentity = identity
	// A Harness may close its process immediately after publishing the result.
	// Observe an already-buffered natural exit before deciding whether Doctor
	// needs to request termination, so natural cleanup is not misreported as a
	// Doctor-initiated signal.
	if !d.timedOut {
		select {
		case status, ok := <-d.session.Exited:
			d.exitObserved = true
			if ok {
				d.exit = status
			}
		default:
		}
	}
	item.Cleanup.IdentityComplete = identity.Complete()
	if !d.exitObserved {
		item.Cleanup.AdapterTerminateAttempted = true
		item.Cleanup.TerminationRequested = true
		item.Cleanup.TerminationPhase = "adapter_terminate"
		if err := d.harness.TerminateSession(ctx, d.session.NativeSessionID); err != nil {
			item.Cleanup.AdapterTerminateError = sanitizeText(err.Error())
			item.Cleanup.Errors = append(item.Cleanup.Errors, item.Cleanup.AdapterTerminateError)
		} else {
			item.Cleanup.TerminationRequested = true
		}
	}

	manager := cfg.TreeManager
	if manager == nil {
		manager = process.PlatformManager{}
	}
	controller := process.Controller{Manager: manager}
	policy := cfg.TreePolicy
	if policy.PollInterval <= 0 {
		policy = process.TerminationPolicy{InterruptGrace: 100 * time.Millisecond, TermGrace: 250 * time.Millisecond, KillGrace: 500 * time.Millisecond, PollInterval: 20 * time.Millisecond}
	}
	if identity.Complete() {
		var termination process.TerminationResult
		var err error
		if item.Cleanup.AdapterTerminateAttempted || !d.exitObserved || d.timedOut {
			termination, err = controller.TerminateTree(ctx, identity, policy)
		} else {
			var gone, reused bool
			var remaining []int
			gone, reused, remaining, err = controller.WaitTreeGone(ctx, identity, cleanupTimeout(cfg.Timeout), policy.PollInterval)
			termination.TreeExited, termination.PIDReused, termination.RemainingPIDs = gone, reused, remaining
		}
		item.Cleanup.TreeExitConfirmed = termination.TreeExited || termination.PIDReused
		item.Cleanup.PIDReused = termination.PIDReused
		item.Cleanup.RemainingPIDs = append([]int(nil), termination.RemainingPIDs...)
		item.Cleanup.OrphanRisk = termination.OrphanRisk || len(termination.RemainingPIDs) > 0 || (!item.Cleanup.TreeExitConfirmed && err != nil)
		if item.Cleanup.OrphanRisk && len(item.Cleanup.RemainingPIDs) == 0 {
			// Termination may have exhausted the bounded context while the
			// controller's final inspection was unable to report its last view.
			// Preserve a fresh process-tree view in the durable Doctor evidence.
			if members, memberErr := manager.GroupMembers(context.Background(), identity); memberErr == nil {
				for _, member := range members {
					item.Cleanup.RemainingPIDs = appendUniqueInt(item.Cleanup.RemainingPIDs, member.PID)
				}
			} else {
				item.Cleanup.Errors = append(item.Cleanup.Errors, sanitizeText(memberErr.Error()))
			}
		}
		item.Cleanup.TerminationRequested = item.Cleanup.TerminationRequested || termination.TerminationRequested
		if termination.TerminationPhase != "" {
			item.Cleanup.TerminationPhase = termination.TerminationPhase
		}
		item.Cleanup.Errors = append(item.Cleanup.Errors, sanitizeStrings(termination.Errors)...)
		if err != nil {
			item.Cleanup.Errors = append(item.Cleanup.Errors, sanitizeText(err.Error()))
		}
	} else if item.Cleanup.AdapterTerminateAttempted {
		item.Cleanup.OrphanRisk = true
		item.Cleanup.Errors = append(item.Cleanup.Errors, "process identity incomplete after Doctor termination request")
	}

	// A natural exit without a complete OS identity is not asserted to have a
	// confirmed tree, but Doctor did not initiate termination or claim one.
	if d.exitObserved && !item.Cleanup.AdapterTerminateAttempted && !item.Cleanup.OrphanRisk && (!item.Cleanup.IdentityComplete || item.Cleanup.TreeExitConfirmed) {
		item.CleanupStatus = "passed"
	} else if item.Cleanup.TreeExitConfirmed && !item.Cleanup.OrphanRisk && len(item.Cleanup.RemainingPIDs) == 0 {
		item.CleanupStatus = "passed"
	} else {
		item.CleanupStatus = "failed"
	}
	item.Stages["termination"] = StageResult{Status: "passed"}
	if item.Cleanup.AdapterTerminateAttempted {
		item.Stages["termination"] = StageResult{Status: "passed"}
	}
	if item.CleanupStatus != "passed" {
		item.Stages["cleanup"] = StageResult{Status: "failed", Error: "process cleanup was not confirmed"}
		item.Errors = append(item.Errors, "process cleanup was not confirmed")
	} else {
		item.Stages["cleanup"] = StageResult{Status: "passed"}
	}
	// Drain all public channels from this same owner after termination. The
	// bounded context guarantees a stuck stream cannot keep Doctor alive.
	drainSessionChannels(ctx, d)
}

func drainSessionChannels(ctx context.Context, d *smokeSessionDriver) {
	events, stderr, exited := d.session.Events, d.session.Stderr, d.session.Exited
	for events != nil || stderr != nil || exited != nil {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-events:
			if !ok {
				events = nil
			}
		case chunk, ok := <-stderr:
			if !ok {
				stderr = nil
			} else if d.stderr.Len() < 1024*1024 {
				d.stderr.Write(chunk.Data)
			}
		case status, ok := <-exited:
			d.exitObserved = true
			if ok {
				d.exit = status
			}
			exited = nil
		}
	}
}

func protocolStatus(item HarnessResult) string {
	for name, stage := range item.Stages {
		if name == "probe" || name == "identity" || name == "termination" || name == "cleanup" || name == "workspace_final" || name == "workspace_setup" {
			continue
		}
		if stage.Status == "failed" {
			return "failed"
		}
	}
	if !item.Result.Observed || !item.TaskWorker.TaskIDMatch || !item.TaskWorker.WorkerIDMatch {
		return "failed"
	}
	if item.IdentityStatus == IdentityMismatch {
		return "failed"
	}
	if len(item.CapabilityEvidence.Contradicted) > 0 {
		return "failed"
	}
	return "passed"
}

func smokeIDs(runID string, harness adapter.HarnessName, now time.Time) (string, string, error) {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", "", err
	}
	return fmt.Sprintf("doctor-task-%s-%s-%x", safeSegment(string(harness)), compactID(runID), random[:4]), fmt.Sprintf("doctor-worker-%s-%x", compactID(runID), random[4:]), nil
}

func smokeContract(taskID, workerID, workspace, runID string, now time.Time) (string, error) {
	item := domain.Task{
		TaskID: domain.TaskID(taskID), Title: "Live Harness Doctor smoke", Objective: "Prove the Harness can start one isolated turn and return the canonical Result Envelope.",
		CompletionCriteria: []string{"Return one canonical succeeded Result Envelope and state that no files changed."},
		WriteScope:         []string{"doctor-placeholder.txt"}, ForbiddenScope: []string{"README.md"},
		KnownReadDependencies: []string{"README.md"}, ValidationCommands: []domain.ValidationCommand{{Command: "test -f README.md", Scope: "local"}},
		AllowNestedAgents: false, AllowPublicInterfaceChange: false, ProjectRoot: workspace,
	}
	contract, err := task.RenderContract(item, domain.RunID(runID))
	if err != nil {
		return "", err
	}
	return contract + fmt.Sprintf("\n## Doctor smoke rules\n\n- Exact Task ID: `%s`\n- Exact Worker ID: `%s`\n- Make no tool calls.\n- Make no file changes, including the permitted placeholder path.\n- Return exactly one canonical Result Envelope.\n- Set `status` to `succeeded`.\n- State that no files changed.\n- Do not start nested agents.\n", taskID, workerID), nil
}

func assessIdentity(harness adapter.HarnessName, requested string, identity adapter.RuntimeIdentity, identityErr error) IdentityAssessment {
	assessment := IdentityAssessment{
		RequestedModel: requested, ObservedProvider: identity.ObservedProvider, ObservedModel: identity.ObservedModel,
		ProviderSource: identity.ProviderSource, ModelSource: identity.ModelSource,
		ProviderStatus: IdentityUnavailable, ModelStatus: IdentityUnavailable,
	}
	if assessment.ProviderSource == "" {
		assessment.ProviderSource = adapter.EvidenceUnavailable
	}
	if assessment.ModelSource == "" {
		assessment.ModelSource = adapter.EvidenceUnavailable
	}
	if identityErr != nil {
		assessment.Warnings = append(assessment.Warnings, sanitizeText(identityErr.Error()))
	}
	expectedProvider := map[adapter.HarnessName]string{
		adapter.HarnessClaudeCode: "anthropic", adapter.HarnessCodex: "openai", adapter.HarnessGrokBuild: "xai",
	}[harness]
	if strings.TrimSpace(identity.ObservedProvider) == "" {
		assessment.ProviderStatus = IdentityUnavailable
		assessment.Warnings = append(assessment.Warnings, "native provider identity unavailable")
	} else if expectedProvider != "" {
		if strings.EqualFold(strings.TrimSpace(identity.ObservedProvider), expectedProvider) {
			assessment.ProviderStatus = IdentityVerified
		} else {
			assessment.ProviderStatus = IdentityMismatch
			assessment.Warnings = append(assessment.Warnings, fmt.Sprintf("expected provider %q, observed %q", expectedProvider, identity.ObservedProvider))
		}
	} else {
		// OpenCode is provider-brokered: a native provider ID is useful evidence,
		// but there is no fixed Descriptor provider to compare against.
		assessment.ProviderStatus = IdentityVerified
	}
	if strings.TrimSpace(identity.ObservedModel) == "" {
		if requested != "" {
			assessment.ModelStatus = IdentityRequestedOnly
			assessment.Warnings = append(assessment.Warnings, "native model identity unavailable")
		} else {
			assessment.ModelStatus = IdentityUnavailable
		}
	} else if requested == "" {
		assessment.ModelStatus = IdentityVerified
	} else if equal, comparable := canonicalModelEqual(harness, requested, identity.ObservedModel, identity.ObservedProvider); !comparable {
		assessment.ModelStatus = IdentityUnavailable
		assessment.Warnings = append(assessment.Warnings, "requested and observed model identities are not safely comparable")
	} else if equal {
		assessment.ModelStatus = IdentityVerified
	} else {
		assessment.ModelStatus = IdentityMismatch
		assessment.Warnings = append(assessment.Warnings, fmt.Sprintf("requested model %q differs from observed model %q", requested, identity.ObservedModel))
	}
	return assessment
}

func canonicalModelEqual(harness adapter.HarnessName, requested, observed, provider string) (bool, bool) {
	requested, observed, provider = strings.TrimSpace(requested), strings.TrimSpace(observed), strings.TrimSpace(provider)
	if requested == "" || observed == "" {
		return false, false
	}
	if harness == adapter.HarnessOpenCode {
		pieces := strings.SplitN(requested, "/", 2)
		if len(pieces) == 2 {
			if provider == "" {
				return false, false
			}
			return pieces[0] == provider && pieces[1] == observed, true
		}
	}
	return requested == observed, true
}

func aggregateIdentityStatus(identity IdentityAssessment) string {
	if identity.ProviderStatus == IdentityMismatch || identity.ModelStatus == IdentityMismatch {
		return IdentityMismatch
	}
	if identity.ProviderStatus == IdentityUnavailable && identity.ModelStatus == IdentityRequestedOnly {
		return IdentityRequestedOnly
	}
	if identity.ProviderStatus == IdentityUnavailable || identity.ModelStatus == IdentityUnavailable {
		return IdentityUnavailable
	}
	if identity.ProviderStatus == IdentityRequestedOnly || identity.ModelStatus == IdentityRequestedOnly {
		return IdentityRequestedOnly
	}
	if identity.ProviderStatus == IdentityVerified && identity.ModelStatus == IdentityVerified {
		return IdentityVerified
	}
	return IdentityUnavailable
}

func finishSmoke(item HarnessResult, ended time.Time) HarnessResult {
	item.EndedAt = ended.UTC()
	item.Duration = item.EndedAt.Sub(item.StartedAt)
	item.Warnings = sanitizeStrings(item.Warnings)
	item.Errors = sanitizeStrings(item.Errors)
	return item
}

func finishSmokeWithWorkspace(item HarnessResult, cfg Config, workspace string, before verify.WorkspaceSnapshot, after *verify.WorkspaceSnapshot, cleanup bool) HarnessResult {
	if after == nil {
		captured, err := verify.CaptureWorkspace(workspace)
		if err != nil {
			item.Errors = append(item.Errors, sanitizeText(fmt.Sprintf("capture final smoke workspace: %v", err)))
			item.Stages["workspace_final"] = StageResult{Status: "failed", Error: item.Errors[len(item.Errors)-1]}
		} else {
			after = &captured
		}
	}
	if after != nil {
		item.Workspace.After = *after
		item.Workspace.ChangedPaths = verify.ChangedFiles(before, *after)
		if len(item.Workspace.ChangedPaths) == 0 {
			item.WorkspaceStatus = "passed"
			item.Stages["workspace_final"] = StageResult{Status: "passed"}
		} else {
			item.WorkspaceStatus = "failed"
			item.Stages["workspace_final"] = StageResult{Status: "failed", Error: "isolated smoke workspace changed"}
		}
	}
	item.Workspace.Retained = true
	item.Artifacts.Workspace = workspace
	return finishSmoke(item, cfg.Now())
}

func cleanupTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 || timeout > 10*time.Second {
		return 10 * time.Second
	}
	return timeout
}

func normalizedLogLine(kind string, timestamp time.Time) string {
	data, _ := json.Marshal(map[string]any{"kind": kind, "timestamp": timestamp.UTC()})
	return string(data) + "\n"
}

// currentTurnEvent adds a second current-generation guard for protocols that
// expose a native turn ID. Adapters remain responsible for generation-aware
// filtering, but an explicitly mismatched ID must never satisfy Doctor's
// terminal boundary. Empty or unknown IDs mean this protocol does not provide
// a reliable comparison and are accepted on the Adapter's authority.
func currentTurnEvent(native adapter.NativeEvent, expected string) bool {
	expected = strings.TrimSpace(expected)
	if expected == "" || expected == "turn-unknown" || len(native.Payload) == 0 {
		return true
	}
	var value struct {
		TurnID  string `json:"turnId"`
		TurnID2 string `json:"turn_id"`
		Turn    struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	if err := json.Unmarshal(native.Payload, &value); err != nil {
		return true
	}
	observed := strings.TrimSpace(value.TurnID)
	if observed == "" {
		observed = strings.TrimSpace(value.TurnID2)
	}
	if observed == "" {
		observed = strings.TrimSpace(value.Turn.ID)
	}
	return observed == "" || observed == expected
}

func appendUniqueString(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func appendUniqueInt(values []int, value int) []int {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func compactID(value string) string {
	value = strings.ReplaceAll(value, "doctor-", "")
	value = strings.NewReplacer("/", "-", " ", "-", ":", "-").Replace(value)
	return value
}

func safeSegment(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	return b.String()
}
