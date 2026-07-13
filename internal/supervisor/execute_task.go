package supervisor

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/project"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/task"
	workerpkg "github.com/vnai/subagent-broker/internal/worker"
)

func (s *Service) executeTask(parent context.Context, runtime *TaskState) error {
	if err := task.ValidateContract(runtime.Task); err != nil {
		return err
	}
	if recoveryTaskTerminal(*runtime) {
		return nil
	}
	if runtime.Dimensions.Process == state.ProcessOrphaned {
		return fmt.Errorf("task %s has an orphaned worker and cannot start another", runtime.Task.TaskID)
	}
	if runtime.Dimensions.Process == state.ProcessUnknown {
		return fmt.Errorf("task %s has unknown process state; resume and fresh retry are forbidden until process tree is confirmed", runtime.Task.TaskID)
	}

	// Load latest task state for attempt history.
	if latest, ok := s.taskState(runtime.Task.TaskID); ok {
		*runtime = latest
	}
	migrateAttempts(runtime)

	mode := workerpkg.AttemptFresh
	if len(runtime.Attempts) > 0 {
		if runtime.Worker != nil && runtime.Worker.NativeSessionID != "" && runtime.Dimensions.Process == state.ProcessExited {
			mode = workerpkg.AttemptRecoveryResume
		} else if len(runtime.Attempts) > 0 && runtime.ActiveAttempt == 0 {
			// History exists but not a resume path: refuse silent retry.
			return fmt.Errorf("task %s has prior attempts and is not recovery-resumable; explicit_retry is not enabled", runtime.Task.TaskID)
		}
	}

	// Single harness selection rule: persisted Worker → Task preference → Run default.
	// Resume never routes a native session ID through a different adapter.
	harness, harnessName, err := s.resolveHarnessForExecution(runtime, mode)
	if err != nil {
		return err
	}

	workerID := fmt.Sprintf("worker-%d", time.Now().UTC().UnixNano())
	capSet := s.computeSessionCapabilities(harness, runtime.Task)
	seed := domain.WorkerSession{
		WorkerID: domain.WorkerID(workerID), TaskID: runtime.Task.TaskID, Harness: harnessName,
		AdapterVersion: harness.Descriptor().AdapterVersion, StartedAt: time.Now().UTC(),
		LastEventAt: time.Now().UTC(), LastProgressAt: time.Now().UTC(),
		Capabilities:           adapter.CapabilityMap(capSet.Effective),
		DeclaredCapabilities:   adapter.CapabilityMap(capSet.Declared),
		ProbeCapabilities:      adapter.CapabilityMap(capSet.Probe),
		ConfiguredCapabilities: adapter.CapabilityMap(capSet.Configured),
		CapabilityDowngrades:   append([]string(nil), capSet.Downgrades...),
		PermissionMode:         s.config.PermissionMode,
		HooksInstalled:         capSet.Configured.Hooks,
		StatusDimensions: state.Dimensions{
			Process: state.ProcessStarting, Protocol: state.ProtocolInitializing,
			Progress: state.ProgressActive, Task: state.TaskRunning,
		},
	}
	if mode == workerpkg.AttemptRecoveryResume && runtime.Worker != nil {
		seed.NativeSessionID = runtime.Worker.NativeSessionID
	}

	attempt, err := workerpkg.Begin(runtime.Task, runtime.Attempts, mode, seed, time.Now().UTC())
	if err != nil {
		return err
	}
	workerID = string(attempt.Worker.WorkerID)
	resumeSessionID := ""
	if mode == workerpkg.AttemptRecoveryResume {
		resumeSessionID = attempt.Worker.NativeSessionID
	}
	// Resume-mode outbox will flush after session is active.

	// Persist new attempt without dropping history.
	// Starting any new attempt invalidates a previously frozen report identity
	// (explicit retry / recovery resume must not silently reuse an old report).
	if err := s.commitMutate(parent, event.Input{
		TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor",
		Type: event.WorkerAttemptStarted, Severity: "info",
		Payload: map[string]any{
			"from": "", "to": string(workerpkg.AttemptRunning),
			"reason": string(mode), "attempt": attempt.Number, "mode": string(mode),
			"worker": attempt.Worker,
		},
	}, func(candidate *Snapshot) error {
		index, err := findTaskIndex(candidate, runtime.Task.TaskID)
		if err != nil {
			return err
		}
		migrateAttempts(&candidate.Tasks[index])
		candidate.Tasks[index].Attempts = append(candidate.Tasks[index].Attempts, attempt)
		candidate.Tasks[index].ActiveAttempt = attempt.Number
		w := attempt.Worker
		candidate.Tasks[index].Worker = &w
		candidate.Tasks[index].Dimensions = w.StatusDimensions
		candidate.Tasks[index].LastProgress = w.LastProgressAt
		if candidate.Tasks[index].ReportIdentity != nil {
			// Freeze invalidation: prior report no longer represents current execution facts.
			stale := *candidate.Tasks[index].ReportIdentity
			stale.Stale = true
			candidate.Tasks[index].ReportIdentity = &stale
		}
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	}); err != nil {
		return err
	}
	s.refreshTaskRuntime(runtime)

	if err := s.setRunStatus(domain.RunRunning, ""); err != nil {
		return err
	}
	if runtime.Task.Status != state.TaskRunning {
		if err := s.transitionTask(runtime, state.TaskRunning); err != nil {
			return err
		}
	}
	if err := s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor", Type: event.SessionStarting, Severity: "info"}); err != nil {
		return err
	}

	// Single context creation — never overwrite cancel.
	var workerCtx context.Context
	var cancel context.CancelFunc
	if s.config.HardTimeout > 0 {
		workerCtx, cancel = context.WithTimeout(parent, s.config.HardTimeout)
	} else {
		workerCtx, cancel = context.WithCancel(parent)
	}
	defer cancel()

	options := map[string]string{"permission_mode": s.config.PermissionMode, "max_turns": fmt.Sprintf("%d", s.config.MaxTurns), "scenario": s.config.Scenario}
	if s.config.SafeMode {
		options["safe_mode"] = "true"
	}
	model := s.config.Model
	if runtime.Task.ModelPreference != "" {
		model = runtime.Task.ModelPreference
	}
	runID := s.Snapshot().Run.RunID
	prompt := buildWorkerPrompt(runtime.Task, runID, workerID)
	if runtime.PendingInstruction != "" {
		prompt += "\n\nMain Agent follow-up instruction:\n" + runtime.PendingInstruction
		runtime.PendingInstruction = ""
	}
	executable, _ := os.Executable()
	interaction := adapter.InteractionConfig{Enabled: !s.config.SafeMode && harnessName == string(adapter.HarnessClaudeCode), BrokerExecutable: executable, RunDir: s.runDir}
	// Per-attempt Worker credential (env only; never control token or argv).
	// Claude known-session design: preselect native session ID, issue credential
	// already Bound, start with the same session_id, verify exact match.
	if interaction.Enabled && s.auth == nil {
		return fmt.Errorf("interaction enabled but auth is not initialized")
	}
	expectedNativeSessionID := resumeSessionID
	if interaction.Enabled && expectedNativeSessionID == "" {
		// Fresh session: generate expected Claude native session ID before credential.
		generated, genErr := project.UUIDv7(time.Now().UTC(), rand.Reader)
		if genErr != nil {
			return fmt.Errorf("generate expected native session id: %w", genErr)
		}
		expectedNativeSessionID = generated
		if options == nil {
			options = map[string]string{}
		}
		options["session_id"] = expectedNativeSessionID
	}
	if interaction.Enabled {
		// Issue already bound to the expected native session (not provisional).
		wtok, tokErr := s.auth.IssueWorkerCredential(string(runID), string(runtime.Task.TaskID), workerID, attempt.Number, expectedNativeSessionID)
		if tokErr != nil {
			return fmt.Errorf("issue worker credential: %w", tokErr)
		}
		interaction.WorkerToken = wtok
		interaction.WorkerSocket = WorkerSocketPath(s.runDir)
		interaction.NativeSessionID = expectedNativeSessionID
	}
	request := adapter.StartRequest{RunID: string(runID), TaskID: string(runtime.Task.TaskID), WorkerID: workerID, ProjectRoot: runtime.Task.ProjectRoot, Contract: prompt, Model: model, Scenario: s.config.Scenario, Options: options, Interaction: interaction}

	var session adapter.Session
	if resumeSessionID != "" {
		session, err = harness.ResumeSession(workerCtx, adapter.ResumeRequest{
			NativeSessionID: resumeSessionID, RunID: request.RunID, TaskID: request.TaskID, WorkerID: request.WorkerID,
			ProjectRoot: request.ProjectRoot, Contract: request.Contract, Model: request.Model, Options: options, Interaction: interaction,
		})
	} else {
		session, err = harness.StartSession(workerCtx, request)
	}
	if err != nil {
		if s.auth != nil {
			s.auth.RevokeWorkerAttempt(string(runtime.Task.TaskID), workerID, attempt.Number)
		}
		_ = s.finishAttempt(runtime, workerpkg.AttemptFailedStart, err.Error())
		if s.isCancelled() {
			return s.cancelTask(runtime, "cancelled before the Worker session started")
		}
		return s.failTask(runtime, "start_session", err)
	}

	// Verify returned native session matches the expected pre-bound identity.
	if interaction.Enabled && s.auth != nil {
		if session.NativeSessionID != expectedNativeSessionID {
			s.auth.RevokeWorkerAttempt(string(runtime.Task.TaskID), workerID, attempt.Number)
			_ = harness.TerminateSession(context.Background(), session.NativeSessionID)
			return s.failTask(runtime, "session_identity_mismatch", fmt.Errorf("returned native session does not match expected identity"))
		}
	}

	// Only OS Inspect may populate ProcessGroupToken. Never synthesize a PGID:
	// a fake group token would make identity.Complete() true and could mis-report
	// tree exit while real children remain.
	identity := process.Identity{PID: session.PID, StartToken: session.ProcessStartToken}
	if session.PID > 0 {
		if inspected, inspectErr := process.Inspect(context.Background(), session.PID); inspectErr == nil {
			identity = inspected
		}
		// Inspect failure keeps identity incomplete (no ProcessGroupToken).
	}

	s.mu.Lock()
	s.active[string(runtime.Task.TaskID)] = activeWorker{
		adapter: harness, sessionID: session.NativeSessionID, cancel: cancel,
		identity: identity, taskID: string(runtime.Task.TaskID), workerID: workerID, attempt: attempt.Number,
	}
	s.mu.Unlock()
	defer func() {
		if s.auth != nil {
			s.auth.RevokeWorkerAttempt(string(runtime.Task.TaskID), workerID, attempt.Number)
		}
		s.mu.Lock()
		delete(s.active, string(runtime.Task.TaskID))
		s.mu.Unlock()
	}()

	// Update worker identity on the active attempt.
	if err := s.commitMutate(parent, event.Input{
		TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor",
		Type: event.ProcessSpawned, Severity: "info",
		Payload: map[string]any{
			"pid": session.PID, "from": string(state.ProcessStarting), "to": string(state.ProcessAlive),
			"reason": "spawned", "start_token": identity.StartToken, "process_group": identity.ProcessGroupToken,
		},
	}, func(candidate *Snapshot) error {
		index, err := findTaskIndex(candidate, runtime.Task.TaskID)
		if err != nil {
			return err
		}
		if candidate.Tasks[index].Worker == nil {
			return fmt.Errorf("missing worker")
		}
		candidate.Tasks[index].Worker.NativeSessionID = session.NativeSessionID
		candidate.Tasks[index].Worker.NativeTurnID = session.NativeTurnID
		candidate.Tasks[index].Worker.PID = session.PID
		candidate.Tasks[index].Worker.ProcessStartToken = identity.StartToken
		candidate.Tasks[index].Worker.ProcessGroupIdentity = identity.ProcessGroupToken
		candidate.Tasks[index].Worker.StatusDimensions.Process = state.ProcessAlive
		candidate.Tasks[index].Dimensions.Process = state.ProcessAlive
		updateAttemptWorker(&candidate.Tasks[index], *candidate.Tasks[index].Worker)
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	}); err != nil {
		return err
	}
	s.refreshTaskRuntime(runtime)
	if err := s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor", Type: event.SessionStarted, Severity: "info"}); err != nil {
		return err
	}
	// After resume/start, flush resume-queued instructions into the active session.
	if mode == workerpkg.AttemptRecoveryResume {
		if flushErr := s.FlushInstructionOutbox(workerCtx, string(runtime.Task.TaskID), "session_resume"); flushErr != nil {
			// Non-fatal for session continuation, but surface when fail-closed.
			if !s.AcceptingWork() {
				return flushErr
			}
		}
	}

	sessionResult, drainErr := s.runWorkerSession(workerCtx, runtime, harness, session, workerID, identity)
	if drainErr != nil && !s.AcceptingWork() {
		return drainErr
	}
	s.refreshTaskRuntime(runtime)

	if sessionResult.Resolution.OrphanRisk || sessionResult.Resolution.ProcessState == state.ProcessOrphaned || sessionResult.Resolution.ProcessState == state.ProcessUnknown {
		// Cannot forge success; surface as failure with honest process state already committed.
		if s.isCancelled() || s.isTaskCancelled(string(runtime.Task.TaskID)) {
			return s.cancelTask(runtime, "cancelled by main agent with unconfirmed process tree")
		}
		if !sessionResult.ResultSeen {
			return s.failTask(runtime, "process_unconfirmed", fmt.Errorf("worker process tree exit unconfirmed: %s", strings.Join(sessionResult.Resolution.Errors, "; ")))
		}
		// Result was collected but process state is orphaned/unknown — still try to apply result,
		// but do not pretend ProcessExited.
	}

	if s.isCancelled() || s.isTaskCancelled(string(runtime.Task.TaskID)) {
		return s.cancelTask(runtime, "cancelled by main agent")
	}
	if sessionResult.TimedOut {
		return s.failTask(runtime, "hard_timeout", fmt.Errorf("Worker exceeded the hard timeout"))
	}
	if sessionResult.Exit.Code != 0 && sessionResult.ResultSeen == false && sessionResult.Resolution.ExitObserved {
		return s.failTask(runtime, "process", fmt.Errorf("worker exited with code %d: %s", sessionResult.Exit.Code, sessionResult.Exit.Error))
	}
	if !sessionResult.ResultSeen && !sessionResult.Resolution.ExitObserved && sessionResult.Resolution.OrphanRisk {
		return s.failTask(runtime, "process_unconfirmed", fmt.Errorf("worker exit unobserved and tree unconfirmed"))
	}
	// Exit code zero is not proof of a valid current-generation result.
	if !sessionResult.ResultSeen && !s.isCancelled() && !s.isTaskCancelled(string(runtime.Task.TaskID)) && !sessionResult.TimedOut {
		return s.failTask(runtime, "result_missing", fmt.Errorf("worker session ended without an authoritative ResultSubmitted for the current turn"))
	}

	result, err := harness.CollectFinalResult(context.Background(), session.NativeSessionID)
	if err != nil {
		return s.failTask(runtime, "result", err)
	}
	if result.TaskID != string(runtime.Task.TaskID) || result.WorkerID != workerID {
		return s.failTask(runtime, "result", fmt.Errorf("result identity mismatch: task=%q worker=%q", result.TaskID, result.WorkerID))
	}
	if latest, ok := s.taskState(runtime.Task.TaskID); ok {
		runtime.Task.WriteScope = append([]string(nil), latest.Task.WriteScope...)
		runtime.Task.AllowPublicInterfaceChange = latest.Task.AllowPublicInterfaceChange
	}
	taskDir := s.taskDir(runtime.Task)
	attemptNumber := reportAttemptNumber(runtime)
	if err := report.Publish(taskDir, result, attemptNumber, time.Now().UTC()); err != nil {
		return s.failTask(runtime, "result_validation", err)
	}
	runtime.ReportPath = filepath.Join(taskDir, "report.md")
	// Freeze producing-attempt identity at publication time (not Barrier-time latest).
	meta, _, verifyErr := report.VerifyDiskArtifacts(taskDir)
	if verifyErr != nil {
		return s.failTask(runtime, "result_validation", verifyErr)
	}
	reportID := ReportIdentity{
		TaskID: meta.TaskID, WorkerID: meta.WorkerID, AttemptNumber: meta.AttemptNumber,
		EnvelopeHash: meta.EnvelopeHash, MarkdownHash: meta.MarkdownHash, PublishedAt: meta.PublishedAt,
	}
	runtime.ReportIdentity = &reportID
	attemptOutcome := workerpkg.AttemptExited
	if sessionResult.Resolution.ProcessState == state.ProcessOrphaned {
		attemptOutcome = workerpkg.AttemptOrphaned
	}
	if finErr := s.finishAttempt(runtime, attemptOutcome, "result collected"); finErr != nil {
		return finErr
	}
	// Re-apply frozen identity after finishAttempt (refresh may have overwritten runtime).
	s.refreshTaskRuntime(runtime)
	runtime.ReportPath = filepath.Join(taskDir, "report.md")
	runtime.ReportIdentity = &reportID
	if err := s.saveRuntime(*runtime); err != nil {
		return err
	}
	if err := s.appendEvent(event.Input{
		TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor", Type: event.ReportPublished, Severity: "info",
		Payload: map[string]any{
			"attempt_number": attemptNumber, "worker_id": workerID,
			"envelope_hash": reportID.EnvelopeHash, "markdown_hash": reportID.MarkdownHash,
		},
	}); err != nil {
		return err
	}
	return s.applyResultEnvelope(runtime, result, workerID)
}

// WorkerExitResolution is the structured outcome of drain/cleanup termination.
// Inability to confirm exit is never reported as ProcessExited.
type WorkerExitResolution struct {
	ExitObserved         bool          `json:"exit_observed"`
	TreeExitConfirmed    bool          `json:"tree_exit_confirmed"`
	ExitCode             *int          `json:"exit_code,omitempty"`
	ExitSignal           string        `json:"exit_signal,omitempty"`
	RemainingPIDs        []int         `json:"remaining_pids,omitempty"`
	OrphanRisk           bool          `json:"orphan_risk"`
	ProcessState         state.Process `json:"process_state"`
	Errors               []string      `json:"errors,omitempty"`
	TerminationRequested bool          `json:"termination_requested,omitempty"`
	TerminationInitiator string        `json:"termination_initiator,omitempty"`
	TerminationPhase     string        `json:"termination_phase,omitempty"`
}

// workerSessionResult is the full outcome of runWorkerSession.
type workerSessionResult struct {
	ResultSeen bool
	TimedOut   bool
	Exit       adapter.ExitStatus
	Resolution WorkerExitResolution
}

// TurnBoundaryResult is the outcome of attempting to start a queued next turn
// after a successful ResultSubmitted. StartedNextTurn is explicit — callers
// must not infer it from message status alone.
type TurnBoundaryResult struct {
	StartedNextTurn bool
	MessageID       string
}

// runWorkerSession drains Events/Stderr and observes session.Exited in one select.
// Process exit is never deferred until after streams close — Exited starts a
// bounded post-exit drain so hang-open streams cannot stall the driver forever.
// It never forges ProcessExited when exit and tree confirmation are both missing.
func (s *Service) runWorkerSession(
	workerCtx context.Context,
	runtime *TaskState,
	harness adapter.Adapter,
	session adapter.Session,
	workerID string,
	identity process.Identity,
) (workerSessionResult, error) {
	var out workerSessionResult
	events := session.Events
	stderr := session.Stderr
	exited := session.Exited
	contextDone := workerCtx.Done()
	progressTicker := time.NewTicker(250 * time.Millisecond)
	defer progressTicker.Stop()

	closing := false
	exitObserved := false
	out.Exit = adapter.ExitStatus{Code: -1}
	drainTimeout := s.config.CancelGrace
	if drainTimeout <= 0 {
		drainTimeout = 1500 * time.Millisecond
	}
	var drainTimer *time.Timer
	var drainC <-chan time.Time
	var lastTerm process.TerminationResult
	var lastTermErr error
	terminationRequested := false
	terminationInitiator := ""
	terminationPhase := ""
	controller := process.Controller{Manager: process.PlatformManager{}}
	policy := defaultCancelPolicy(s.config.CancelGrace)
	mergeTermination := func(requested bool, initiator, phase string) {
		if requested {
			terminationRequested = true
		}
		if terminationInitiator == "" && initiator != "" {
			terminationInitiator = initiator
		}
		if terminationPhaseRank(phase) > terminationPhaseRank(terminationPhase) {
			terminationPhase = phase
		}
	}
	mergeControllerTermination := func(result process.TerminationResult, initiator string) {
		mergeTermination(result.TerminationRequested, initiator, result.TerminationPhase)
	}

	stopDrainTimer := func() {
		if drainTimer != nil {
			drainTimer.Stop()
			drainTimer = nil
			drainC = nil
		}
	}
	defer stopDrainTimer()

	startBoundedDrain := func() {
		if drainC != nil {
			return
		}
		stopDrainTimer()
		drainTimer = time.NewTimer(drainTimeout)
		drainC = drainTimer.C
	}

	terminateReliable := func(initiator string) {
		if termErr := harness.TerminateSession(context.Background(), session.NativeSessionID); termErr != nil {
			lastTerm.Errors = append(lastTerm.Errors, "adapter terminate: "+termErr.Error())
			lastTermErr = termErr
		} else {
			mergeTermination(true, initiator, "adapter_terminate")
		}
		if identity.Complete() {
			result, err := controller.TerminateTree(context.Background(), identity, policy)
			lastTerm = result
			mergeControllerTermination(result, initiator)
			if err != nil {
				lastTermErr = err
				lastTerm.Errors = append(lastTerm.Errors, err.Error())
			}
		} else {
			lastTerm.TreeExited = false
			lastTerm.OrphanRisk = true
			lastTerm.Errors = append(lastTerm.Errors, "incomplete process identity during drain")
		}
	}

	// Main loop: Events, Stderr, and Exited are peers. Never wait for stream
	// close before observing process exit. Nil channels are never selected.
	for events != nil || stderr != nil || exited != nil {
		select {
		case <-contextDone:
			contextDone = nil
			if !out.ResultSeen {
				terminateReliable(terminationInitiatorForSession(s, runtime, errors.Is(workerCtx.Err(), context.DeadlineExceeded)))
			}
			startBoundedDrain()
		case now := <-progressTicker.C:
			if !closing {
				s.updateProgress(runtime, now.UTC())
			}
		case <-drainC:
			// Bounded drain elapsed: stop waiting on hang-open streams.
			if !exitObserved {
				terminateReliable(terminationInitiatorForSession(s, runtime, out.TimedOut))
			} else if identity.Complete() && !lastTerm.TreeExited {
				// Parent exited but tree may still have children.
				result, err := controller.TerminateTree(context.Background(), identity, policy)
				lastTerm = result
				mergeControllerTermination(result, "worker_exit")
				if err != nil {
					lastTermErr = err
					lastTerm.Errors = append(lastTerm.Errors, err.Error())
				}
			}
			events = nil
			stderr = nil
			exited = nil
		case exitStatus, ok := <-exited:
			if ok {
				out.Exit = exitStatus
				exitObserved = true
			}
			exited = nil
			closing = true
			// Prefer any already-buffered events before the post-exit drain timer
			// (select races can otherwise observe Exited first and drop result.submitted).
			for events != nil {
				select {
				case native, ok := <-events:
					if !ok {
						events = nil
						break
					}
					s.handleNative(runtime, harness, native, workerID)
					// Post-exit: never start a next turn; only observe final result.
					if native.Kind == event.ResultSubmitted {
						out.ResultSeen = true
					}
				default:
					goto exitDrainDone
				}
			}
		exitDrainDone:
			// Process exited: allow a short drain of trailing events/stderr only.
			startBoundedDrain()
		case native, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			s.handleNative(runtime, harness, native, workerID)
			if closing {
				continue
			}
			switch native.Kind {
			case event.ResultSubmitted:
				// Successful turn boundary: optionally start one queued next turn.
				boundary := s.startNextTurnAtBoundary(workerCtx, string(runtime.Task.TaskID), exitObserved)
				if boundary.StartedNextTurn {
					// Keep the session alive; do not treat this result as final.
					continue
				}
				out.ResultSeen = true
				closing = true
				if termErr := harness.TerminateSession(context.Background(), session.NativeSessionID); termErr != nil {
					lastTerm.Errors = append(lastTerm.Errors, "adapter terminate after result: "+termErr.Error())
				} else {
					mergeTermination(true, "supervisor_post_result", "adapter_terminate")
				}
				startBoundedDrain()
			case event.TurnFailed:
				// TurnFailed must not launch a queued next turn.
				out.ResultSeen = false
				closing = true
				if termErr := harness.TerminateSession(context.Background(), session.NativeSessionID); termErr != nil {
					lastTerm.Errors = append(lastTerm.Errors, "adapter terminate after turn failure: "+termErr.Error())
				} else {
					mergeTermination(true, "adapter_failure", "adapter_terminate")
				}
				startBoundedDrain()
			}
		case chunk, ok := <-stderr:
			if !ok {
				stderr = nil
				continue
			}
			_ = appendFile(filepath.Join(s.taskDir(runtime.Task), "stderr.log"), chunk.Data)
			at := chunk.Timestamp
			if at.IsZero() {
				at = time.Now().UTC()
			}
			runtime.LastStderr = at
			runtime.LastProgress = at
			if runtime.Worker != nil {
				runtime.Worker.LastProgressAt = at
			}
		}
	}

	out.TimedOut = errors.Is(workerCtx.Err(), context.DeadlineExceeded)

	// If Exited never arrived, attempt one final reliable terminate + tree confirm.
	if !exitObserved {
		terminateReliable(terminationInitiatorForSession(s, runtime, out.TimedOut))
	} else if identity.Complete() && !lastTerm.TreeExited && len(lastTerm.RemainingPIDs) == 0 {
		// Confirm tree gone after observed exit (children may remain).
		result, err := controller.TerminateTree(context.Background(), identity, policy)
		mergeControllerTermination(result, "worker_exit")
		if err == nil {
			lastTerm = result
		} else {
			lastTermErr = err
			lastTerm.Errors = append(lastTerm.Errors, err.Error())
		}
	}

	resolution := WorkerExitResolution{
		ExitObserved:         exitObserved,
		TreeExitConfirmed:    lastTerm.TreeExited || lastTerm.PIDReused,
		RemainingPIDs:        append([]int(nil), lastTerm.RemainingPIDs...),
		OrphanRisk:           lastTerm.OrphanRisk || (!exitObserved && !lastTerm.TreeExited && !lastTerm.PIDReused),
		Errors:               append([]string(nil), lastTerm.Errors...),
		TerminationRequested: terminationRequested,
		TerminationInitiator: terminationInitiator,
		TerminationPhase:     terminationPhase,
	}
	if lastTermErr != nil {
		resolution.Errors = append(resolution.Errors, lastTermErr.Error())
	}
	if exitObserved {
		code := out.Exit.Code
		resolution.ExitCode = &code
		resolution.ExitSignal = out.Exit.Signal
		if resolution.TerminationInitiator == "" {
			resolution.TerminationInitiator = "worker_exit"
		}
	}

	// Map to process dimension honestly.
	// session.Exited proves only the harness root exited — never the full tree by default.
	resolution.ProcessState, resolution.OrphanRisk = mapWorkerExitProcessState(resolution, identity.Complete())
	if resolution.ProcessState == state.ProcessExited && !exitObserved {
		out.Exit = adapter.ExitStatus{Code: -1, Error: "tree exit confirmed without session exit status"}
	}
	out.Resolution = resolution

	if err := s.commitWorkerExitResolution(runtime, workerID, resolution, out.Exit); err != nil {
		return out, err
	}
	// Unknown/orphaned process state degrades the Run; do not pretend clean completion.
	if resolution.OrphanRisk || resolution.ProcessState == state.ProcessOrphaned || resolution.ProcessState == state.ProcessUnknown {
		_ = s.setRunStatus(domain.RunDegraded, "worker process tree exit unconfirmed or orphaned")
	}
	return out, nil
}

// mapWorkerExitProcessState applies the shared exit/tree confirmation matrix used by
// drain and cancel paths.
//
//	ExitObserved + TreeExitConfirmed → ProcessExited
//	ExitObserved + RemainingPIDs     → ProcessOrphaned
//	ExitObserved + unconfirmed tree  → ProcessUnknown + OrphanRisk
//	!ExitObserved + TreeExitConfirmed → ProcessExited
//	RemainingPIDs                    → ProcessOrphaned
//	else                             → ProcessUnknown + OrphanRisk
func mapWorkerExitProcessState(r WorkerExitResolution, identityComplete bool) (state.Process, bool) {
	if len(r.RemainingPIDs) > 0 {
		return state.ProcessOrphaned, true
	}
	if r.TreeExitConfirmed {
		return state.ProcessExited, false
	}
	if r.ExitObserved {
		// Parent exited but tree not confirmed (incomplete identity or controller inconclusive).
		if !identityComplete || r.OrphanRisk {
			return state.ProcessUnknown, true
		}
		// Identity complete but tree still not confirmed after terminate attempts.
		return state.ProcessUnknown, true
	}
	return state.ProcessUnknown, true
}

// commitWorkerExitResolution persists the honest process dimension after drain.
func (s *Service) commitWorkerExitResolution(runtime *TaskState, workerID string, resolution WorkerExitResolution, exit adapter.ExitStatus) error {
	eventType := event.ProcessExited
	severity := "info"
	switch resolution.ProcessState {
	case state.ProcessOrphaned:
		eventType = event.ProcessOrphaned
		severity = "error"
	case state.ProcessUnknown:
		eventType = event.ProcessStateChanged
		severity = "error"
	default:
		severity = severityForExit(exit.Code)
	}
	if resolution.ExitCode != nil {
		code := *resolution.ExitCode
		if runtime.Worker != nil {
			runtime.Worker.ExitCode = &code
		}
	}
	if runtime.Worker != nil {
		runtime.Worker.EndedAt = timePtr(time.Now().UTC())
		runtime.Worker.StatusDimensions.Process = resolution.ProcessState
		runtime.Dimensions = runtime.Worker.StatusDimensions
	} else {
		runtime.Dimensions.Process = resolution.ProcessState
	}
	processEvidence := failureProcessEvidenceFromResolution(resolution)

	return s.commitMutate(context.Background(), event.Input{
		TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor",
		Type: eventType, Severity: severity,
		Payload: map[string]any{
			"from": string(state.ProcessAlive), "to": string(resolution.ProcessState),
			"exit_observed": resolution.ExitObserved, "tree_exit_confirmed": resolution.TreeExitConfirmed,
			"orphan_risk": resolution.OrphanRisk, "remaining": resolution.RemainingPIDs,
			"errors": resolution.Errors, "exit": exit,
			"exit_signal":           resolution.ExitSignal,
			"termination_requested": resolution.TerminationRequested,
			"termination_initiator": resolution.TerminationInitiator,
			"termination_phase":     resolution.TerminationPhase,
		},
	}, func(candidate *Snapshot) error {
		index, err := findTaskIndex(candidate, runtime.Task.TaskID)
		if err != nil {
			return err
		}
		candidate.Tasks[index].Dimensions.Process = resolution.ProcessState
		if candidate.Tasks[index].Worker != nil {
			candidate.Tasks[index].Worker.StatusDimensions.Process = resolution.ProcessState
			candidate.Tasks[index].Worker.EndedAt = timePtr(time.Now().UTC())
			if resolution.ExitCode != nil {
				code := *resolution.ExitCode
				candidate.Tasks[index].Worker.ExitCode = &code
			}
			updateAttemptWorker(&candidate.Tasks[index], *candidate.Tasks[index].Worker)
		}
		candidate.Tasks[index].LastProcessEvidence = &processEvidence
		if resolution.OrphanRisk {
			candidate.LastError = "worker process exit unconfirmed"
		}
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
}

func defaultCancelPolicy(grace time.Duration) process.TerminationPolicy {
	if grace <= 0 {
		grace = 1500 * time.Millisecond
	}
	return process.TerminationPolicy{
		InterruptGrace: grace / 3,
		TermGrace:      grace / 3,
		KillGrace:      grace / 3,
		PollInterval:   50 * time.Millisecond,
	}
}

func terminationPhaseRank(phase string) int {
	switch phase {
	case "adapter_terminate":
		return 1
	case "interrupt":
		return 2
	case "term":
		return 3
	case "kill_tree":
		return 4
	default:
		return 0
	}
}

func terminationInitiatorForSession(s *Service, runtime *TaskState, timedOut bool) string {
	if timedOut {
		return "hard_timeout"
	}
	if runtime != nil && s.isTaskCancelled(string(runtime.Task.TaskID)) {
		return "task_cancel"
	}
	if s.isCancelled() {
		return "run_cancel"
	}
	return ""
}

func updateAttemptWorker(ts *TaskState, w domain.WorkerSession) {
	if ts.ActiveAttempt <= 0 {
		return
	}
	for i := range ts.Attempts {
		if ts.Attempts[i].Number == ts.ActiveAttempt {
			ts.Attempts[i].Worker = w
			return
		}
	}
}

func (s *Service) finishAttempt(runtime *TaskState, outcome workerpkg.AttemptOutcome, reason string) error {
	return s.commitMutate(context.Background(), event.Input{
		TaskID: string(runtime.Task.TaskID), WorkerID: workerID(runtime), Source: "supervisor",
		Type: event.WorkerAttemptFinished, Severity: "info",
		Payload: map[string]any{
			"from": string(workerpkg.AttemptRunning), "to": string(outcome),
			"reason": reason, "attempt": runtime.ActiveAttempt,
		},
	}, func(candidate *Snapshot) error {
		index, err := findTaskIndex(candidate, runtime.Task.TaskID)
		if err != nil {
			return err
		}
		finishActiveAttempt(&candidate.Tasks[index], outcome, reason, time.Now().UTC())
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
}

// applyResultEnvelope maps a published Result Envelope to Task status.
func (s *Service) applyResultEnvelope(runtime *TaskState, result report.Envelope, workerID string) error {
	switch result.Status {
	case report.StatusSucceeded:
		if err := s.transitionTask(runtime, state.TaskReportedComplete); err != nil {
			return err
		}
		if err := s.transitionTask(runtime, state.TaskVerifying); err != nil {
			return err
		}
		if !s.runValidation(context.Background(), runtime) {
			if err := s.transitionTask(runtime, state.TaskVerificationFailed); err != nil {
				return err
			}
			validationErr := fmt.Errorf("task validation failed")
			publication, evidenceErr := s.recordFailureEvidence(runtime, "task_validation", validationErr, true)
			if evidenceErr != nil {
				return joinFatalSupervisorError(validationErr, evidenceErr)
			}
			if saveErr := s.saveRuntime(*runtime); saveErr != nil {
				return joinFatalSupervisorError(validationErr, fmt.Errorf("save validation failure evidence path: %w", saveErr))
			}
			if eventErr := s.appendFailureEvidencePublication(publication); eventErr != nil {
				return joinFatalSupervisorError(validationErr, eventErr)
			}
			if eventErr := s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "verifier", Type: event.TaskVerificationFailed, Severity: "error"}); eventErr != nil {
				return joinFatalSupervisorError(validationErr, eventErr)
			}
			return validationErr
		}
		if err := s.transitionTask(runtime, state.TaskVerifiedSuccess); err != nil {
			return err
		}
		return s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "verifier", Type: event.TaskVerifiedSuccess, Severity: "info"})

	case report.StatusPartial:
		if err := s.transitionTask(runtime, state.TaskReportedComplete); err != nil {
			return err
		}
		if err := s.transitionTask(runtime, state.TaskVerifying); err != nil {
			return err
		}
		if !s.runValidation(context.Background(), runtime) {
			if err := s.transitionTask(runtime, state.TaskVerificationFailed); err != nil {
				return err
			}
			validationErr := fmt.Errorf("task validation failed")
			publication, evidenceErr := s.recordFailureEvidence(runtime, "task_validation", validationErr, true)
			if evidenceErr != nil {
				return joinFatalSupervisorError(validationErr, evidenceErr)
			}
			if saveErr := s.saveRuntime(*runtime); saveErr != nil {
				return joinFatalSupervisorError(validationErr, fmt.Errorf("save validation failure evidence path: %w", saveErr))
			}
			if eventErr := s.appendFailureEvidencePublication(publication); eventErr != nil {
				return joinFatalSupervisorError(validationErr, eventErr)
			}
			if eventErr := s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "verifier", Type: event.TaskVerificationFailed, Severity: "error"}); eventErr != nil {
				return joinFatalSupervisorError(validationErr, eventErr)
			}
			return validationErr
		}
		return s.markPartial(runtime, result.Status)

	case report.StatusBlocked:
		return s.commitMutate(context.Background(), event.Input{
			TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor",
			Type: event.TaskStateChanged, Severity: "warning",
			Payload: map[string]any{
				"from": string(runtime.Task.Status), "to": string(state.TaskBlocked),
				"reason": "envelope_blocked", "block_kind": string(BlockKindFinal),
			},
		}, func(candidate *Snapshot) error {
			index, err := findTaskIndex(candidate, runtime.Task.TaskID)
			if err != nil {
				return err
			}
			candidate.Tasks[index].Task.Status = state.TaskBlocked
			candidate.Tasks[index].Dimensions.Task = state.TaskBlocked
			candidate.Tasks[index].BlockKind = BlockKindFinal
			candidate.Tasks[index].LastError = "worker reported blocked"
			if candidate.Tasks[index].Worker != nil {
				candidate.Tasks[index].Worker.StatusDimensions.Task = state.TaskBlocked
			}
			candidate.UpdatedAt = time.Now().UTC()
			return nil
		})

	case report.StatusFailed:
		return s.commitMutate(context.Background(), event.Input{
			TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor",
			Type: event.TaskStateChanged, Severity: "error",
			Payload: map[string]any{"from": string(runtime.Task.Status), "to": string(state.TaskFailed), "reason": "envelope_failed"},
		}, func(candidate *Snapshot) error {
			index, err := findTaskIndex(candidate, runtime.Task.TaskID)
			if err != nil {
				return err
			}
			// Keep worker-published failed report path; do not overwrite with generic report.
			candidate.Tasks[index].Task.Status = state.TaskFailed
			candidate.Tasks[index].Dimensions.Task = state.TaskFailed
			candidate.Tasks[index].LastError = "worker reported failed"
			if candidate.Tasks[index].Worker != nil {
				candidate.Tasks[index].Worker.StatusDimensions.Task = state.TaskFailed
			}
			candidate.UpdatedAt = time.Now().UTC()
			return nil
		})

	case report.StatusCancelled:
		return s.commitMutate(context.Background(), event.Input{
			TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor",
			Type: event.TaskStateChanged, Severity: "warning",
			Payload: map[string]any{"from": string(runtime.Task.Status), "to": string(state.TaskCancelled), "reason": "envelope_cancelled"},
		}, func(candidate *Snapshot) error {
			index, err := findTaskIndex(candidate, runtime.Task.TaskID)
			if err != nil {
				return err
			}
			candidate.Tasks[index].Task.Status = state.TaskCancelled
			candidate.Tasks[index].Dimensions.Task = state.TaskCancelled
			candidate.Tasks[index].LastError = "worker reported cancelled"
			if candidate.Tasks[index].Worker != nil {
				candidate.Tasks[index].Worker.StatusDimensions.Task = state.TaskCancelled
			}
			candidate.UpdatedAt = time.Now().UTC()
			return nil
		})

	default:
		return s.failTask(runtime, "result", fmt.Errorf("unknown result status %q", result.Status))
	}
}
