package clioutcome

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/state"
)

// ExitCode is the stable CLI exit taxonomy. Numeric values must not change.
type ExitCode int

const (
	ExitOK            ExitCode = 0
	ExitUsage         ExitCode = 2
	ExitNotFound      ExitCode = 3
	ExitPreflight     ExitCode = 10
	ExitPartial       ExitCode = 20
	ExitBlocked       ExitCode = 21
	ExitFailed        ExitCode = 22
	ExitCancelled     ExitCode = 23
	ExitTimeout       ExitCode = 24
	ExitCommunication ExitCode = 25
	ExitCompatibility ExitCode = 26
	ExitInternal      ExitCode = 70
)

// OutcomeKind is the stable semantic name paired with an exit code. These
// values are intentionally kept in clioutcome so commands do not grow a
// second result model.
type OutcomeKind string

const (
	OutcomeSuccess       OutcomeKind = "success"
	OutcomePartial       OutcomeKind = "partial"
	OutcomeBlocked       OutcomeKind = "blocked"
	OutcomeFailed        OutcomeKind = "failed"
	OutcomeCancelled     OutcomeKind = "cancelled"
	OutcomeTimeout       OutcomeKind = "timeout"
	OutcomeCommunication OutcomeKind = "communication_error"
	OutcomeUsage         OutcomeKind = "usage_error"
	OutcomeInternal      OutcomeKind = "internal_error"
	OutcomeNotFound      OutcomeKind = "not_found"
	OutcomePreflight     OutcomeKind = "preflight_failed"
	OutcomeCompatibility OutcomeKind = "compatibility_error"
)

// Error is a typed CLI failure carrying a stable exit code.
type Error struct {
	Code    ExitCode
	Op      string
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e == nil {
		return "cli error"
	}
	switch {
	case e.Message != "" && e.Err != nil:
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	case e.Message != "":
		return e.Message
	case e.Err != nil:
		return e.Err.Error()
	default:
		return fmt.Sprintf("exit code %d", e.Code)
	}
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// New constructs a typed CLI error.
func New(code ExitCode, op, message string, err error) error {
	return &Error{Code: code, Op: op, Message: message, Err: err}
}

// CodeOf maps an error to an ExitCode.
// nil → ExitOK; typed Error → its Code; anything else → ExitInternal.
func CodeOf(err error) ExitCode {
	if err == nil {
		return ExitOK
	}
	var typed *Error
	if errors.As(err, &typed) && typed != nil {
		return typed.Code
	}
	return ExitInternal
}

// Kind classifies the object an Outcome describes.
type Kind string

const (
	KindRun   Kind = "run"
	KindWave  Kind = "wave"
	KindTask  Kind = "task"
	KindInbox Kind = "inbox"
	KindEvent Kind = "event"
)

// Outcome is the stable result of waiting on or observing a run object.
type Outcome struct {
	Kind       Kind     `json:"target_type"`
	ID         string   `json:"target_id"`
	Status     string   `json:"status"`
	Reason     string   `json:"reason,omitempty"`
	Terminal   bool     `json:"terminal"`
	Successful bool     `json:"successful"`
	Code       ExitCode `json:"exit_code"`
}

// Err returns a typed error for every non-success outcome, including
// non-terminal timeout and communication results.
func (o Outcome) Err(op string) error {
	if o.Code == ExitOK {
		return nil
	}
	msg := fmt.Sprintf("%s ended with status %s", o.Kind, o.Status)
	if o.ID != "" {
		msg = fmt.Sprintf("%s %s ended with status %s", o.Kind, o.ID, o.Status)
	}
	if o.Reason != "" {
		msg += ": " + o.Reason
	}
	return New(o.Code, op, msg, nil)
}

// Name returns the stable, human-readable outcome name associated with an exit
// code. Keep this mapping in one place so JSON and shell exit behavior cannot
// drift apart.
func Name(code ExitCode) string {
	switch code {
	case ExitOK:
		return "success"
	case ExitPartial:
		return "partial"
	case ExitBlocked:
		return "blocked"
	case ExitFailed:
		return "failed"
	case ExitCancelled:
		return "cancelled"
	case ExitTimeout:
		return "timeout"
	case ExitCommunication:
		return "communication_error"
	case ExitUsage:
		return "usage_error"
	case ExitCompatibility:
		return "compatibility_error"
	case ExitPreflight:
		return "preflight_failed"
	case ExitNotFound:
		return "not_found"
	case ExitInternal:
		return "internal_error"
	default:
		return "internal_error"
	}
}

// CLIOutput is the common observable result envelope used by status, events,
// wait, and Barrier commands. It deliberately reuses Outcome rather than
// introducing another result taxonomy.
type CLIOutput struct {
	Outcome            string    `json:"outcome"`
	ExitCode           ExitCode  `json:"exit_code"`
	TargetType         Kind      `json:"target_type,omitempty"`
	TargetID           string    `json:"target_id,omitempty"`
	Terminal           bool      `json:"terminal"`
	Status             string    `json:"status,omitempty"`
	Reason             string    `json:"reason,omitempty"`
	DataSource         string    `json:"data_source,omitempty"`
	Mode               string    `json:"mode,omitempty"`
	Degraded           bool      `json:"degraded"`
	SnapshotTime       time.Time `json:"snapshot_time,omitempty"`
	SupervisorAlive    bool      `json:"supervisor_alive"`
	SupervisorIdentity string    `json:"supervisor_identity,omitempty"`
}

func OutputFor(outcome Outcome, source, reason string, degraded, alive bool, identity string, snapshotTime time.Time) CLIOutput {
	if reason == "" {
		reason = outcome.Reason
	}
	name := Name(outcome.Code)
	if !outcome.Terminal && outcome.Code == ExitOK {
		name = "pending"
	}
	mode := "live"
	if degraded {
		mode = "degraded"
	}
	return CLIOutput{
		Outcome: name, ExitCode: outcome.Code,
		TargetType: outcome.Kind, TargetID: outcome.ID,
		Terminal: outcome.Terminal, Status: outcome.Status, Reason: reason,
		DataSource: source, Mode: mode, Degraded: degraded, SnapshotTime: snapshotTime,
		SupervisorAlive: alive, SupervisorIdentity: identity,
	}
}

// FromRun maps a Run status to an Outcome.
func FromRun(status domain.RunStatus) Outcome {
	return FromRunDetailed(status, "")
}

// FromRunDetailed maps Run status using LastError/reason for degraded outcomes.
func FromRunDetailed(status domain.RunStatus, reason string) Outcome {
	out := Outcome{Kind: KindRun, Status: string(status)}
	switch status {
	case domain.RunCompleted:
		out.Terminal = true
		out.Successful = true
		out.Code = ExitOK
	case domain.RunFailed:
		out.Terminal = true
		out.Code = ExitFailed
	case domain.RunDegraded:
		out.Terminal = true
		out.Reason = reason
		// Map degraded by reason — not a single fixed code.
		switch {
		case containsFold(reason, "timeout") || containsFold(reason, "timed out"):
			out.Code = ExitTimeout
		case containsFold(reason, "cancel"):
			out.Code = ExitCancelled
		case containsFold(reason, "warning") || containsFold(reason, "acceptance") || containsFold(reason, "blocked"):
			out.Code = ExitBlocked
		case containsFold(reason, "supervisor") || containsFold(reason, "unreachable") || containsFold(reason, "communication") || containsFold(reason, "orphan") || containsFold(reason, "reattach"):
			out.Code = ExitCommunication
		case containsFold(reason, "partial"):
			out.Code = ExitPartial
		default:
			// A degraded terminal Run with no durable classification is not
			// success. Preserve the older safe default of failed and require
			// callers to pass the persisted reason for more specific mapping.
			out.Code = ExitFailed
		}
	case domain.RunCancelled:
		out.Terminal = true
		out.Code = ExitCancelled
	}
	return out
}

func containsFold(s, sub string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(sub))
}

// FromWave maps a Wave (including barrier result) to an Outcome.
func FromWave(value domain.Wave) Outcome {
	out := Outcome{Kind: KindWave, ID: string(value.WaveID), Status: string(value.Status)}
	switch value.Status {
	case domain.WaveVerified:
		out.Terminal = true
		switch value.BarrierResult {
		case domain.BarrierPassedWithWarnings:
			if value.BarrierAccepted {
				out.Successful = true
				out.Code = ExitOK
			} else {
				// Warning acceptance is a decision gate, not a partial
				// verification result.
				out.Code = ExitBlocked
			}
		case domain.BarrierFailed:
			out.Code = ExitFailed
		case domain.BarrierBlocked:
			out.Code = ExitBlocked
		case domain.BarrierCancelled:
			out.Code = ExitCancelled
		default:
			// BarrierPassed or empty barrier result on a verified Wave.
			out.Successful = true
			out.Code = ExitOK
		}
	case domain.WaveBlocked:
		out.Terminal = true
		out.Code = ExitBlocked
	case domain.WaveWaiting:
		// Awaiting warning acceptance.
		out.Terminal = true
		out.Code = ExitBlocked
		out.Reason = value.BarrierReason
	case domain.WaveFailed:
		out.Terminal = true
		out.Code = ExitFailed
	case domain.WaveCancelled:
		out.Terminal = true
		out.Code = ExitCancelled
	}
	return out
}

// FromTask maps Task status to an Outcome.
// terminalBlocked enables treating blocked as a terminal ExitBlocked result;
// until the state model exposes final-blocked, callers should pass false.
func FromTask(id string, status state.Task, terminalBlocked bool) Outcome {
	out := Outcome{Kind: KindTask, ID: id, Status: string(status)}
	switch status {
	case state.TaskVerifiedSuccess:
		out.Terminal = true
		out.Successful = true
		out.Code = ExitOK
	case state.TaskVerifiedPartial:
		out.Terminal = true
		out.Code = ExitPartial
	case state.TaskBlocked:
		if terminalBlocked {
			out.Terminal = true
			out.Code = ExitBlocked
		}
	case state.TaskVerificationFailed, state.TaskFailed:
		out.Terminal = true
		out.Code = ExitFailed
	case state.TaskCancelled:
		out.Terminal = true
		out.Code = ExitCancelled
	}
	return out
}
