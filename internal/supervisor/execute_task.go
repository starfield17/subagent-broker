package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/process"
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

	harness, ok := s.registry.Get(adapter.HarnessName(s.config.Harness))
	if !ok {
		return fmt.Errorf("adapter %q is not registered", s.config.Harness)
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

	workerID := fmt.Sprintf("worker-%d", time.Now().UTC().UnixNano())
	capabilities := harness.Descriptor().Capabilities
	if s.config.SafeMode {
		capabilities.PermissionEvents = false
		capabilities.Hooks = false
	}
	seed := domain.WorkerSession{
		WorkerID: domain.WorkerID(workerID), TaskID: runtime.Task.TaskID, Harness: s.config.Harness,
		AdapterVersion: harness.Descriptor().AdapterVersion, StartedAt: time.Now().UTC(),
		LastEventAt: time.Now().UTC(), LastProgressAt: time.Now().UTC(),
		Capabilities: capabilityMap(capabilities),
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

	// Persist new attempt without dropping history.
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
	interaction := adapter.InteractionConfig{Enabled: !s.config.SafeMode && s.config.Harness == string(adapter.HarnessClaudeCode), BrokerExecutable: executable, RunDir: s.runDir}
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
		_ = s.finishAttempt(runtime, workerpkg.AttemptFailedStart, err.Error())
		if s.isCancelled() {
			return s.cancelTask(runtime, "cancelled before the Worker session started")
		}
		return s.failTask(runtime, "start_session", err)
	}

	identity := process.Identity{PID: session.PID, StartToken: session.ProcessStartToken}
	if session.PID > 0 {
		if inspected, inspectErr := process.Inspect(context.Background(), session.PID); inspectErr == nil {
			identity = inspected
		}
	}

	s.mu.Lock()
	s.active[string(runtime.Task.TaskID)] = activeWorker{
		adapter: harness, sessionID: session.NativeSessionID, cancel: cancel,
		identity: identity, taskID: string(runtime.Task.TaskID), workerID: workerID, attempt: attempt.Number,
	}
	s.mu.Unlock()
	defer func() {
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

	resultSeen, timedOut, exit, drainErr := s.runWorkerSession(workerCtx, runtime, harness, session, workerID, identity)
	if drainErr != nil && !s.AcceptingWork() {
		return drainErr
	}
	s.refreshTaskRuntime(runtime)

	if s.isCancelled() || s.isTaskCancelled(string(runtime.Task.TaskID)) {
		return s.cancelTask(runtime, "cancelled by main agent")
	}
	if timedOut {
		return s.failTask(runtime, "hard_timeout", fmt.Errorf("Worker exceeded the hard timeout"))
	}
	if exit.Code != 0 && !resultSeen {
		return s.failTask(runtime, "process", fmt.Errorf("worker exited with code %d: %s", exit.Code, exit.Error))
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
	if err := report.Publish(taskDir, result, time.Now().UTC()); err != nil {
		return s.failTask(runtime, "result_validation", err)
	}
	runtime.ReportPath = filepath.Join(taskDir, "report.md")
	_ = s.finishAttempt(runtime, workerpkg.AttemptExited, "result collected")
	if err := s.saveRuntime(*runtime); err != nil {
		return err
	}
	if err := s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor", Type: event.ReportPublished, Severity: "info"}); err != nil {
		return err
	}
	return s.applyResultEnvelope(runtime, result, workerID)
}

// runWorkerSession drains Events/Stderr until closed or process exits after result.
func (s *Service) runWorkerSession(
	workerCtx context.Context,
	runtime *TaskState,
	harness adapter.Adapter,
	session adapter.Session,
	workerID string,
	identity process.Identity,
) (resultSeen bool, timedOut bool, exit adapter.ExitStatus, err error) {
	events := session.Events
	stderr := session.Stderr
	contextDone := workerCtx.Done()
	progressTicker := time.NewTicker(250 * time.Millisecond)
	defer progressTicker.Stop()

	closing := false
	drainTimeout := s.config.CancelGrace
	if drainTimeout <= 0 {
		drainTimeout = 1500 * time.Millisecond
	}
	var drainTimer *time.Timer
	var drainC <-chan time.Time
	stopDrainTimer := func() {
		if drainTimer != nil {
			drainTimer.Stop()
			drainTimer = nil
			drainC = nil
		}
	}
	defer stopDrainTimer()

	for events != nil || stderr != nil {
		select {
		case <-contextDone:
			contextDone = nil
			if !resultSeen {
				_ = harness.TerminateSession(context.Background(), session.NativeSessionID)
				if identity.Complete() {
					_, _ = process.Controller{Manager: process.PlatformManager{}}.TerminateTree(context.Background(), identity, defaultCancelPolicy(s.config.CancelGrace))
				}
			}
		case now := <-progressTicker.C:
			if !closing {
				s.updateProgress(runtime, now.UTC())
			}
		case <-drainC:
			if identity.Complete() {
				_, _ = process.Controller{Manager: process.PlatformManager{}}.TerminateTree(context.Background(), identity, defaultCancelPolicy(s.config.CancelGrace))
			}
			events = nil
			stderr = nil
		case native, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			s.handleNative(runtime, harness, native, workerID)
			if (native.Kind == event.ResultSubmitted || native.Kind == event.TurnFailed) && !closing {
				resultSeen = native.Kind == event.ResultSubmitted
				closing = true
				// Do not break: drain remaining events/stderr until channels close or deadline.
				_ = harness.TerminateSession(context.Background(), session.NativeSessionID)
				stopDrainTimer()
				drainTimer = time.NewTimer(drainTimeout)
				drainC = drainTimer.C
			}
		case chunk, ok := <-stderr:
			if !ok {
				stderr = nil
				continue
			}
			_ = appendFile(filepath.Join(s.taskDir(runtime.Task), "stderr.log"), chunk.Data)
		}
	}

	timedOut = errors.Is(workerCtx.Err(), context.DeadlineExceeded)
	exit = adapter.ExitStatus{Code: -1}
	if session.Exited != nil {
		select {
		case value, ok := <-session.Exited:
			if ok {
				exit = value
			}
		case <-time.After(s.config.CancelGrace + time.Second):
			if identity.Complete() {
				_, _ = process.Controller{Manager: process.PlatformManager{}}.TerminateTree(context.Background(), identity, defaultCancelPolicy(s.config.CancelGrace))
			}
		}
	}

	if runtime.Worker != nil {
		runtime.Worker.ExitCode = &exit.Code
		runtime.Worker.EndedAt = timePtr(time.Now().UTC())
		runtime.Worker.StatusDimensions.Process = state.ProcessExited
		runtime.Dimensions = runtime.Worker.StatusDimensions
	}
	_ = s.saveRuntime(*runtime)
	_ = s.appendEvent(event.Input{
		TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor",
		Type: event.ProcessExited, Severity: severityForExit(exit.Code),
		Payload: map[string]any{"from": string(state.ProcessAlive), "to": string(state.ProcessExited), "exit": exit},
	})
	return resultSeen, timedOut, exit, nil
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
			_ = s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "verifier", Type: event.TaskVerificationFailed, Severity: "error"})
			return fmt.Errorf("task validation failed")
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
			_ = s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "verifier", Type: event.TaskVerificationFailed, Severity: "error"})
			return fmt.Errorf("task validation failed")
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
