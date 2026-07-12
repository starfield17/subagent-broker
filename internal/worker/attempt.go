package worker

import (
	"fmt"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
)

// AttemptMode classifies why a WorkerSession was started for a Task.
type AttemptMode string

const (
	AttemptFresh          AttemptMode = "fresh"
	AttemptRecoveryResume AttemptMode = "recovery_resume"
	AttemptExplicitRetry  AttemptMode = "explicit_retry"
)

// AttemptOutcome is the lifecycle result of one Worker attempt.
type AttemptOutcome string

const (
	AttemptRunning     AttemptOutcome = "running"
	AttemptExited      AttemptOutcome = "exited"
	AttemptOrphaned    AttemptOutcome = "orphaned"
	AttemptPIDReused   AttemptOutcome = "pid_reused"
	AttemptFailedStart AttemptOutcome = "failed_start"
)

// Attempt is one numbered Worker execution for a Task.
type Attempt struct {
	Number    int                  `json:"number"`
	Mode      AttemptMode          `json:"mode"`
	Worker    domain.WorkerSession `json:"worker"`
	Outcome   AttemptOutcome       `json:"outcome"`
	StartedAt time.Time            `json:"started_at"`
	EndedAt   *time.Time           `json:"ended_at,omitempty"`
	Reason    string               `json:"reason,omitempty"`
}

// NextNumber returns the next strictly increasing attempt number (starting at 1).
func NextNumber(existing []Attempt) int {
	max := 0
	for _, item := range existing {
		if item.Number > max {
			max = item.Number
		}
	}
	return max + 1
}

// Begin starts a new running Attempt for task. existing is never mutated; the
// returned Attempt is independent of the caller's slice backing store.
func Begin(task domain.Task, existing []Attempt, mode AttemptMode, worker domain.WorkerSession, now time.Time) (Attempt, error) {
	history := copyAttempts(existing)
	if active, err := Active(history); err != nil {
		return Attempt{}, err
	} else if active != nil {
		return Attempt{}, fmt.Errorf("task already has running attempt %d", active.Number)
	}

	switch mode {
	case AttemptFresh:
		if len(history) > 0 {
			return Attempt{}, fmt.Errorf("fresh attempt requires no existing attempts")
		}
	case AttemptRecoveryResume, AttemptExplicitRetry:
		if len(history) == 0 {
			return Attempt{}, fmt.Errorf("%s requires an existing attempt", mode)
		}
	default:
		return Attempt{}, fmt.Errorf("unknown attempt mode %q", mode)
	}

	if worker.TaskID == "" {
		worker.TaskID = task.TaskID
	}
	if worker.TaskID != task.TaskID {
		return Attempt{}, fmt.Errorf("worker task id %q does not match task %q", worker.TaskID, task.TaskID)
	}

	if mode == AttemptRecoveryResume {
		// Never clear a previously observed native session id; callers may still
		// decide whether resume is valid when the new id differs.
		if worker.NativeSessionID == "" {
			for i := len(history) - 1; i >= 0; i-- {
				if history[i].Worker.NativeSessionID != "" {
					worker.NativeSessionID = history[i].Worker.NativeSessionID
					break
				}
			}
		}
	}

	number := NextNumber(history)
	worker = copyWorker(worker)
	worker.Attempt = number
	worker.AttemptMode = string(mode)

	return Attempt{
		Number:    number,
		Mode:      mode,
		Worker:    worker,
		Outcome:   AttemptRunning,
		StartedAt: now.UTC(),
	}, nil
}

// Finish marks a running Attempt with a terminal outcome. Finished attempts
// cannot be finished again.
func Finish(value Attempt, outcome AttemptOutcome, reason string, now time.Time) (Attempt, error) {
	if value.Outcome != AttemptRunning {
		return Attempt{}, fmt.Errorf("attempt %d is already finished with outcome %s", value.Number, value.Outcome)
	}
	switch outcome {
	case AttemptExited, AttemptOrphaned, AttemptPIDReused, AttemptFailedStart:
	case AttemptRunning:
		return Attempt{}, fmt.Errorf("cannot finish attempt with running outcome")
	default:
		return Attempt{}, fmt.Errorf("unknown attempt outcome %q", outcome)
	}
	ended := now.UTC()
	value.Worker = copyWorker(value.Worker)
	value.Outcome = outcome
	value.Reason = reason
	value.EndedAt = &ended
	return value, nil
}

// Active returns a copy of the single running Attempt, if any.
// It is an error for more than one attempt to be running.
func Active(existing []Attempt) (*Attempt, error) {
	var found *Attempt
	for _, item := range existing {
		if item.Outcome != AttemptRunning {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("multiple running attempts: %d and %d", found.Number, item.Number)
		}
		copyValue := copyAttempt(item)
		found = &copyValue
	}
	return found, nil
}

func copyAttempts(values []Attempt) []Attempt {
	if values == nil {
		return nil
	}
	result := make([]Attempt, len(values))
	for i, value := range values {
		result[i] = copyAttempt(value)
	}
	return result
}

func copyAttempt(value Attempt) Attempt {
	value.Worker = copyWorker(value.Worker)
	if value.EndedAt != nil {
		ended := *value.EndedAt
		value.EndedAt = &ended
	}
	return value
}

func copyWorker(value domain.WorkerSession) domain.WorkerSession {
	if value.Capabilities != nil {
		capabilities := make(map[string]bool, len(value.Capabilities))
		for key, item := range value.Capabilities {
			capabilities[key] = item
		}
		value.Capabilities = capabilities
	}
	if value.EndedAt != nil {
		ended := *value.EndedAt
		value.EndedAt = &ended
	}
	if value.ExitCode != nil {
		code := *value.ExitCode
		value.ExitCode = &code
	}
	return value
}
