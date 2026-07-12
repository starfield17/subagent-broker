package supervisor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/state"
)

// findTaskIndex returns the index of taskID in candidate.Tasks.
func findTaskIndex(candidate *Snapshot, taskID domain.TaskID) (int, error) {
	for i := range candidate.Tasks {
		if candidate.Tasks[i].Task.TaskID == taskID {
			return i, nil
		}
	}
	return -1, fmt.Errorf("task %q not found in snapshot", taskID)
}

// commitMutate is the single entry for Snapshot mutations with a state event.
func (s *Service) commitMutate(ctx context.Context, input event.Input, mutate func(candidate *Snapshot) error) error {
	if !s.AcceptingWork() {
		return &CommitError{Stage: CommitStageValidate, Err: fmt.Errorf("supervisor is not accepting work after a fatal persistence failure")}
	}
	_, err := s.Commit(ctx, CommitRequest{Event: input, Mutate: mutate})
	if err != nil {
		s.onCommitFailure(err)
	}
	return err
}

// onCommitFailure cancels active workers after a fatal Commit persistence error.
// Commit itself already fail-closes acceptingWork and reports the channel.
func (s *Service) onCommitFailure(err error) {
	if err == nil {
		return
	}
	var commitErr *CommitError
	if !asCommitStage(err, &commitErr) {
		return
	}
	if commitErr.Stage != CommitStageEvent && commitErr.Stage != CommitStageSnapshot {
		return
	}
	s.cancelAllActiveWorkers()
}

func asCommitStage(err error, target **CommitError) bool {
	if err == nil {
		return false
	}
	return errors.As(err, target)
}

func (s *Service) cancelAllActiveWorkers() {
	s.mu.Lock()
	active := make([]activeWorker, 0, len(s.active))
	for _, worker := range s.active {
		active = append(active, worker)
	}
	s.mu.Unlock()
	for _, worker := range active {
		if worker.cancel != nil {
			worker.cancel()
		}
	}
}

// setRunStatus commits a Run status transition.
func (s *Service) setRunStatus(status domain.RunStatus, reason string) error {
	before := s.Snapshot()
	from := before.Run.Status
	if from == status && (reason == "" || before.LastError == reason) {
		return nil
	}
	return s.commitMutate(context.Background(), event.Input{
		Source: "supervisor", Type: event.RunStateChanged, Severity: "info",
		Payload: map[string]any{"from": string(from), "to": string(status), "reason": reason},
	}, func(candidate *Snapshot) error {
		now := time.Now().UTC()
		candidate.Run.Status = status
		if reason != "" {
			candidate.LastError = reason
		}
		if candidate.Run.StartedAt == nil && status != domain.RunPlanned {
			candidate.Run.StartedAt = &now
		}
		if status == domain.RunCompleted || status == domain.RunFailed || status == domain.RunCancelled || status == domain.RunDegraded {
			candidate.Run.EndedAt = &now
		}
		candidate.UpdatedAt = now
		return nil
	})
}

// setWaveStatus commits the current Wave status transition.
func (s *Service) setWaveStatus(status domain.WaveStatus) error {
	before := s.Snapshot()
	from := before.Wave.Status
	waveID := before.Wave.WaveID
	if from == status {
		return nil
	}
	return s.commitMutate(context.Background(), event.Input{
		Source: "supervisor", Type: event.WaveStateChanged, Severity: "info",
		Payload: map[string]any{
			"wave_id": string(waveID),
			"from":    string(from),
			"to":      string(status),
			"reason":  "",
		},
	}, func(candidate *Snapshot) error {
		now := time.Now().UTC()
		candidate.Wave.Status = status
		if status == domain.WaveRunning {
			candidate.Wave.StartedAt = &now
		}
		if status == domain.WaveVerified || status == domain.WaveFailed || status == domain.WaveCancelled || status == domain.WaveBlocked {
			candidate.Wave.EndedAt = &now
		}
		for index := range candidate.Waves {
			if candidate.Waves[index].WaveID == candidate.Wave.WaveID {
				candidate.Waves[index] = candidate.Wave
				break
			}
		}
		candidate.UpdatedAt = now
		return nil
	})
}

// transitionTask validates and commits a Task status change, refreshing runtime.
func (s *Service) transitionTask(runtime *TaskState, next state.Task) error {
	if runtime == nil {
		return fmt.Errorf("task runtime is required")
	}
	from := runtime.Task.Status
	if err := state.ValidateTaskTransition(from, next); err != nil {
		return err
	}
	taskID := runtime.Task.TaskID
	eventType := event.TaskStateChanged
	if next == state.TaskReportedComplete {
		eventType = event.TaskReportedComplete
	} else if next == state.TaskVerifiedSuccess {
		eventType = event.TaskVerifiedSuccess
	} else if next == state.TaskVerificationFailed {
		eventType = event.TaskVerificationFailed
	}
	err := s.commitMutate(context.Background(), event.Input{
		TaskID: string(taskID), Source: "supervisor", Type: eventType, Severity: "info",
		Payload: map[string]any{
			"from": string(from), "to": string(next), "reason": "",
			"task_id": string(taskID),
		},
	}, func(candidate *Snapshot) error {
		index, err := findTaskIndex(candidate, taskID)
		if err != nil {
			return err
		}
		if err := state.ValidateTaskTransition(candidate.Tasks[index].Task.Status, next); err != nil {
			return err
		}
		candidate.Tasks[index].Task.Status = next
		candidate.Tasks[index].Dimensions.Task = next
		if candidate.Tasks[index].Worker != nil {
			candidate.Tasks[index].Worker.StatusDimensions.Task = next
		}
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
	if err != nil {
		return err
	}
	s.refreshTaskRuntime(runtime)
	return nil
}

// saveRuntime commits a full TaskState replacement for the matching Task ID.
// Prefer narrower helpers when only status dimensions change.
func (s *Service) saveRuntime(runtime TaskState) error {
	taskID := runtime.Task.TaskID
	return s.commitMutate(context.Background(), event.Input{
		TaskID: string(taskID), WorkerID: workerID(&runtime),
		Source: "supervisor", Type: event.TaskRuntimeUpdated, Severity: "info",
		Payload: map[string]any{
			"task_id": string(taskID),
			"reason":  "runtime_update",
			"task":    runtime,
		},
	}, func(candidate *Snapshot) error {
		index, err := findTaskIndex(candidate, taskID)
		if err != nil {
			return err
		}
		// Preserve scope expansions applied while the worker was running.
		runtime.Task.WriteScope = append([]string(nil), candidate.Tasks[index].Task.WriteScope...)
		runtime.Task.AllowPublicInterfaceChange = candidate.Tasks[index].Task.AllowPublicInterfaceChange
		// Re-apply blocked status if messages are still pending.
		// Commit already holds s.mu; read messageIndex without re-locking.
		for _, pending := range s.messageIndex {
			if pending.TaskID == string(taskID) && pending.Status != message.Answered && pending.Status != message.Expired && pending.Status != message.Failed {
				runtime.Task.Status = state.TaskBlocked
				runtime.Dimensions.Task = state.TaskBlocked
				runtime.Dimensions.Progress = state.ProgressQuiet
				break
			}
		}
		candidate.Tasks[index] = cloneTaskState(runtime)
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
}

func (s *Service) refreshTaskRuntime(runtime *TaskState) {
	if runtime == nil {
		return
	}
	if latest, ok := s.taskState(runtime.Task.TaskID); ok {
		*runtime = latest
	}
}

// commitTaskDimensions updates process/protocol/progress for a task.
func (s *Service) commitTaskDimensions(runtime *TaskState, eventType string, from, to string, extra map[string]any) error {
	if runtime == nil {
		return fmt.Errorf("task runtime is required")
	}
	taskID := runtime.Task.TaskID
	payload := map[string]any{"from": from, "to": to, "reason": "", "task_id": string(taskID)}
	for k, v := range extra {
		payload[k] = v
	}
	workerIDValue := workerID(runtime)
	err := s.commitMutate(context.Background(), event.Input{
		TaskID: string(taskID), WorkerID: workerIDValue,
		Source: "supervisor", Type: eventType, Severity: "info", Payload: payload,
	}, func(candidate *Snapshot) error {
		index, err := findTaskIndex(candidate, taskID)
		if err != nil {
			return err
		}
		// Apply the full dimensions from the provided runtime after validating
		// the specific transition that triggered this commit.
		candidate.Tasks[index].Dimensions = runtime.Dimensions
		if candidate.Tasks[index].Worker != nil && runtime.Worker != nil {
			candidate.Tasks[index].Worker = runtime.Worker
			candidate.Tasks[index].Worker.StatusDimensions = runtime.Dimensions
		} else if runtime.Worker != nil {
			candidate.Tasks[index].Worker = runtime.Worker
		}
		if !runtime.LastProgress.IsZero() {
			candidate.Tasks[index].LastProgress = runtime.LastProgress
		}
		if runtime.LastError != "" {
			candidate.Tasks[index].LastError = runtime.LastError
		}
		if runtime.ReportPath != "" {
			candidate.Tasks[index].ReportPath = runtime.ReportPath
		}
		if len(runtime.Validation) > 0 {
			candidate.Tasks[index].Validation = append([]ValidationResult(nil), runtime.Validation...)
		}
		candidate.Tasks[index].Task.Status = runtime.Task.Status
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
	if err != nil {
		return err
	}
	s.refreshTaskRuntime(runtime)
	return nil
}

// appendEvent records a pure telemetry event without mutating Snapshot.
// Persistence failures fail-close the supervisor.
func (s *Service) appendEvent(input event.Input) error {
	if !s.AcceptingWork() {
		return fmt.Errorf("supervisor is not accepting work after a fatal persistence failure")
	}
	_, err := s.events.Append(input)
	if err != nil {
		s.mu.Lock()
		s.acceptingWork = false
		s.reportPersistenceFailure(err)
		s.mu.Unlock()
		s.cancelAllActiveWorkers()
		return err
	}
	return nil
}
