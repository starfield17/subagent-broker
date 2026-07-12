package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/state"
	workerpkg "github.com/vnai/subagent-broker/internal/worker"
)

// RecoveryClass is the pure classification of one Task after Supervisor restart.
type RecoveryClass string

const (
	RecoveryAlreadyTerminal   RecoveryClass = "already_terminal"
	RecoveryAliveOrphaned     RecoveryClass = "alive_orphaned"
	RecoveryPIDReused         RecoveryClass = "pid_reused"
	RecoveryExitedResumable   RecoveryClass = "exited_resumable"
	RecoveryExitedUnresumable RecoveryClass = "exited_unresumable"
	RecoveryMissingIdentity   RecoveryClass = "missing_identity"
)

// RecoveryDecision is the pure classification result for one Task.
type RecoveryDecision struct {
	TaskID          domain.TaskID
	Class           RecoveryClass
	WorkerID        string
	NativeSessionID string
	Reason          string
	Process         state.Process
	ResumeSessionID string
}

// ClassifyRecovery is pure: no disk writes, no process signals, no Commit.
func ClassifyRecovery(runtime TaskState, inspect process.Identity, inspectErr error, harnessSupportsResume bool) RecoveryDecision {
	decision := RecoveryDecision{
		TaskID:   runtime.Task.TaskID,
		WorkerID: workerID(&runtime),
	}
	if recoveryTaskTerminal(runtime) {
		decision.Class = RecoveryAlreadyTerminal
		decision.Reason = "task is already terminal"
		return decision
	}
	w := runtime.Worker
	if w == nil && len(runtime.Attempts) > 0 {
		copy := runtime.Attempts[len(runtime.Attempts)-1].Worker
		w = &copy
		decision.WorkerID = string(w.WorkerID)
	}
	if w == nil {
		decision.Class = RecoveryMissingIdentity
		decision.Reason = "no worker session recorded"
		return decision
	}
	decision.NativeSessionID = w.NativeSessionID

	if w.PID <= 0 || w.ProcessStartToken == "" {
		return exitedDecision(decision, w, harnessSupportsResume, "process identity missing")
	}
	if inspectErr != nil && isProcessMissing(inspectErr) {
		return exitedDecision(decision, w, harnessSupportsResume, "worker process is gone")
	}
	if inspectErr != nil {
		return exitedDecision(decision, w, harnessSupportsResume, "process inspect failed: "+inspectErr.Error())
	}
	expected := process.Identity{PID: w.PID, StartToken: w.ProcessStartToken, ProcessGroupToken: w.ProcessGroupIdentity}
	if !expected.SameProcess(inspect) {
		decision.Class = RecoveryPIDReused
		decision.Reason = "pid exists but start token does not match"
		decision.Process = state.ProcessUnknown
		return decision
	}
	// Process still alive with matching identity. V1 has no reattach.
	decision.Class = RecoveryAliveOrphaned
	decision.Reason = "worker process is alive but cannot be reattached safely"
	decision.Process = state.ProcessOrphaned
	return decision
}

func exitedDecision(decision RecoveryDecision, w *domain.WorkerSession, supportsResume bool, reason string) RecoveryDecision {
	decision.Process = state.ProcessExited
	if w.NativeSessionID != "" && supportsResume {
		decision.Class = RecoveryExitedResumable
		decision.ResumeSessionID = w.NativeSessionID
		decision.Reason = reason + "; native session is resumable"
		return decision
	}
	decision.Class = RecoveryExitedUnresumable
	decision.Reason = reason + "; session is not resumable"
	return decision
}

func recoveryTaskTerminal(runtime TaskState) bool {
	switch runtime.Task.Status {
	case state.TaskVerifiedSuccess, state.TaskVerifiedPartial, state.TaskVerificationFailed,
		state.TaskFailed, state.TaskCancelled:
		return true
	case state.TaskBlocked:
		// Final blocked (envelope result) is terminal for recovery; waiting_message is not.
		return runtime.BlockKind == BlockKindFinal
	default:
		return false
	}
}

func isProcessMissing(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such process") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "no such file")
}

func (s *Service) reconcileRecovery(ctx context.Context) error {
	if err := s.setRunStatus(domain.RunRecovering, ""); err != nil {
		return err
	}
	if err := s.appendEvent(event.Input{Source: "recovery", Type: "run.recovering", Severity: "warning"}); err != nil {
		return err
	}

	snapshot := s.Snapshot()
	harness, harnessOK := s.registry.Get(adapter.HarnessName(s.config.Harness))
	supportsResume := harnessOK && harness.Descriptor().Capabilities.ResumeSession

	// Classify every Task first — never return after the first Worker.
	decisions := make([]RecoveryDecision, 0, len(snapshot.Tasks))
	for _, runtime := range snapshot.Tasks {
		w := runtime.Worker
		var identity process.Identity
		var inspectErr error
		if w != nil && w.PID > 0 {
			identity, inspectErr = process.Inspect(ctx, w.PID)
		} else if !recoveryTaskTerminal(runtime) {
			inspectErr = os.ErrNotExist
		}
		decisions = append(decisions, ClassifyRecovery(runtime, identity, inspectErr, supportsResume))
	}

	hasOrphan := false
	hasUnrecoverable := false
	for _, d := range decisions {
		if err := s.applyRecoveryDecision(ctx, d); err != nil {
			return err
		}
		switch d.Class {
		case RecoveryAliveOrphaned:
			hasOrphan = true
		case RecoveryExitedUnresumable, RecoveryMissingIdentity, RecoveryPIDReused:
			hasUnrecoverable = true
		}
	}

	// Aggregate Run status from all decisions.
	switch {
	case hasOrphan:
		return s.setRunStatus(domain.RunDegraded, "one or more workers are alive but cannot be reattached safely")
	case hasUnrecoverable:
		return s.setRunStatus(domain.RunRunning, "recovery finished with unrecoverable workers")
	default:
		return s.setRunStatus(domain.RunRunning, "recovery finished")
	}
}

func (s *Service) applyRecoveryDecision(ctx context.Context, d RecoveryDecision) error {
	_ = s.appendEvent(event.Input{
		TaskID: string(d.TaskID), WorkerID: d.WorkerID, Source: "recovery", Type: event.RecoveryClassified, Severity: "warning",
		Payload: map[string]any{"class": string(d.Class), "reason": d.Reason, "from": "", "to": string(d.Class)},
	})

	switch d.Class {
	case RecoveryAlreadyTerminal:
		return nil

	case RecoveryAliveOrphaned:
		return s.commitMutate(ctx, event.Input{
			TaskID: string(d.TaskID), WorkerID: d.WorkerID, Source: "recovery", Type: event.ProcessOrphaned, Severity: "error",
			Payload: map[string]any{"from": string(state.ProcessAlive), "to": string(state.ProcessOrphaned), "reason": d.Reason},
		}, func(candidate *Snapshot) error {
			index, err := findTaskIndex(candidate, d.TaskID)
			if err != nil {
				return err
			}
			candidate.Tasks[index].Dimensions.Process = state.ProcessOrphaned
			if candidate.Tasks[index].Worker != nil {
				candidate.Tasks[index].Worker.StatusDimensions.Process = state.ProcessOrphaned
			}
			finishActiveAttempt(&candidate.Tasks[index], workerpkg.AttemptOrphaned, d.Reason, time.Now().UTC())
			candidate.UpdatedAt = time.Now().UTC()
			return nil
		})

	case RecoveryPIDReused:
		return s.commitMutate(ctx, event.Input{
			TaskID: string(d.TaskID), WorkerID: d.WorkerID, Source: "recovery", Type: event.ProcessStateChanged, Severity: "error",
			Payload: map[string]any{"from": string(state.ProcessAlive), "to": string(state.ProcessUnknown), "reason": d.Reason, "class": string(d.Class)},
		}, func(candidate *Snapshot) error {
			index, err := findTaskIndex(candidate, d.TaskID)
			if err != nil {
				return err
			}
			candidate.Tasks[index].Dimensions.Process = state.ProcessUnknown
			candidate.Tasks[index].LastError = d.Reason
			finishActiveAttempt(&candidate.Tasks[index], workerpkg.AttemptPIDReused, d.Reason, time.Now().UTC())
			if err := state.ValidateTaskTransition(candidate.Tasks[index].Task.Status, state.TaskFailed); err == nil {
				candidate.Tasks[index].Task.Status = state.TaskFailed
				candidate.Tasks[index].Dimensions.Task = state.TaskFailed
			}
			candidate.UpdatedAt = time.Now().UTC()
			return nil
		})

	case RecoveryExitedUnresumable, RecoveryMissingIdentity:
		return s.commitMutate(ctx, event.Input{
			TaskID: string(d.TaskID), WorkerID: d.WorkerID, Source: "recovery", Type: event.TaskStateChanged, Severity: "error",
			Payload: map[string]any{
				"from": string(state.TaskRunning), "to": string(state.TaskFailed),
				"reason": d.Reason, "class": string(d.Class),
			},
		}, func(candidate *Snapshot) error {
			index, err := findTaskIndex(candidate, d.TaskID)
			if err != nil {
				return err
			}
			from := candidate.Tasks[index].Task.Status
			candidate.Tasks[index].Dimensions.Process = state.ProcessExited
			candidate.Tasks[index].LastError = d.Reason
			finishActiveAttempt(&candidate.Tasks[index], workerpkg.AttemptExited, d.Reason, time.Now().UTC())
			if err := state.ValidateTaskTransition(from, state.TaskFailed); err == nil {
				candidate.Tasks[index].Task.Status = state.TaskFailed
				candidate.Tasks[index].Dimensions.Task = state.TaskFailed
			}
			candidate.UpdatedAt = time.Now().UTC()
			return nil
		})

	case RecoveryExitedResumable:
		return s.commitMutate(ctx, event.Input{
			TaskID: string(d.TaskID), WorkerID: d.WorkerID, Source: "recovery", Type: event.RecoveryResumed, Severity: "warning",
			Payload: map[string]any{
				"reason": d.Reason, "native_session_id": d.ResumeSessionID,
				"from": string(state.ProcessAlive), "to": string(state.ProcessExited),
			},
		}, func(candidate *Snapshot) error {
			index, err := findTaskIndex(candidate, d.TaskID)
			if err != nil {
				return err
			}
			candidate.Tasks[index].Dimensions.Process = state.ProcessExited
			if candidate.Tasks[index].Worker != nil {
				candidate.Tasks[index].Worker.StatusDimensions.Process = state.ProcessExited
				candidate.Tasks[index].Worker.NativeSessionID = d.ResumeSessionID
			}
			finishActiveAttempt(&candidate.Tasks[index], workerpkg.AttemptExited, d.Reason, time.Now().UTC())
			// Leave Task non-terminal so executeWave can open recovery_resume attempt.
			candidate.UpdatedAt = time.Now().UTC()
			return nil
		})
	}
	return fmt.Errorf("unknown recovery class %q", d.Class)
}

func finishActiveAttempt(ts *TaskState, outcome workerpkg.AttemptOutcome, reason string, now time.Time) {
	migrateAttempts(ts)
	if ts.ActiveAttempt <= 0 {
		return
	}
	for i := range ts.Attempts {
		if ts.Attempts[i].Number != ts.ActiveAttempt {
			continue
		}
		finished, err := workerpkg.Finish(ts.Attempts[i], outcome, reason, now)
		if err != nil {
			ts.Attempts[i].Outcome = outcome
			ts.Attempts[i].Reason = reason
			ended := now
			ts.Attempts[i].EndedAt = &ended
		} else {
			ts.Attempts[i] = finished
		}
		w := ts.Attempts[i].Worker
		ts.Worker = &w
		ts.ActiveAttempt = 0
		return
	}
}

// migrateAttempts ensures legacy single-Worker snapshots gain an Attempts history.
func migrateAttempts(ts *TaskState) {
	if len(ts.Attempts) > 0 {
		return
	}
	if ts.Worker == nil {
		return
	}
	mode := workerpkg.AttemptFresh
	if ts.Worker.AttemptMode != "" {
		mode = workerpkg.AttemptMode(ts.Worker.AttemptMode)
	}
	number := ts.Worker.Attempt
	if number <= 0 {
		number = 1
	}
	outcome := workerpkg.AttemptRunning
	if ts.Worker.ExitCode != nil || ts.Dimensions.Process == state.ProcessExited {
		outcome = workerpkg.AttemptExited
	}
	if ts.Dimensions.Process == state.ProcessOrphaned {
		outcome = workerpkg.AttemptOrphaned
	}
	ts.Attempts = []workerpkg.Attempt{{
		Number: number, Mode: mode, Worker: *ts.Worker,
		Outcome: outcome, StartedAt: ts.Worker.StartedAt, EndedAt: ts.Worker.EndedAt,
	}}
	if outcome == workerpkg.AttemptRunning {
		ts.ActiveAttempt = number
	}
}

// chooseExecutionMode selects fresh vs recovery_resume for the next worker start.
// explicit_retry is reserved for a future Main Agent API and is never chosen here.
func chooseExecutionMode(runtime TaskState) workerpkg.AttemptMode {
	migrateAttempts(&runtime)
	if len(runtime.Attempts) == 0 {
		return workerpkg.AttemptFresh
	}
	// Resumable: last attempt exited with native session and process is exited.
	if runtime.Worker != nil && runtime.Worker.NativeSessionID != "" && runtime.Dimensions.Process == state.ProcessExited {
		return workerpkg.AttemptRecoveryResume
	}
	// If we already have attempts but no resume path, do not invent explicit_retry.
	// executeTask will fail rather than overwrite history with a silent retry.
	return workerpkg.AttemptRecoveryResume
}
