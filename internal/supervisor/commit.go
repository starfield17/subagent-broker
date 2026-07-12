package supervisor

import (
	"context"
	"fmt"
	"time"

	"github.com/vnai/subagent-broker/internal/event"
)

// CommitStage identifies which step of Commit failed.
//
// Persistence is intentionally not a multi-file database transaction: events
// are appended before the Snapshot projection is written. A snapshot-stage
// failure therefore leaves an event that has no matching installed Snapshot;
// recovery will reconcile via an event reducer in a later PR.
type CommitStage string

const (
	CommitStageValidate CommitStage = "validate"
	CommitStageEvent    CommitStage = "event"
	CommitStageSnapshot CommitStage = "snapshot"
)

// CommitError describes a failed Commit with a stage for callers and tests.
type CommitError struct {
	Stage CommitStage
	Err   error
}

func (e *CommitError) Error() string {
	if e == nil {
		return "commit error"
	}
	if e.Err == nil {
		return fmt.Sprintf("commit failed at stage %s", e.Stage)
	}
	return fmt.Sprintf("commit failed at stage %s: %v", e.Stage, e.Err)
}

func (e *CommitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// CommitRequest is one transactional attempt to mutate the in-memory Snapshot
// candidate, append a run event, and persist the Snapshot projection.
type CommitRequest struct {
	Event    event.Input
	Mutate   func(candidate *Snapshot) error
	Validate func(before Snapshot, candidate Snapshot) error // optional
}

// Commit applies Mutate/Validate against a deep-copied candidate Snapshot, then
// appends the event and persists the candidate. On event or snapshot persistence
// failure the Supervisor fail-closes (acceptingWork=false) and reports via
// FatalPersistenceErrors. Successful commits install the candidate into memory
// only after both event and snapshot persistence succeed.
func (s *Service) Commit(ctx context.Context, request CommitRequest) (event.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return event.Event{}, err
	}
	if !s.acceptingWork {
		return event.Event{}, &CommitError{
			Stage: CommitStageValidate,
			Err:   fmt.Errorf("supervisor is not accepting work after a fatal persistence failure"),
		}
	}

	before, err := cloneSnapshot(s.snapshot)
	if err != nil {
		return event.Event{}, &CommitError{Stage: CommitStageValidate, Err: err}
	}
	candidate, err := cloneSnapshot(s.snapshot)
	if err != nil {
		return event.Event{}, &CommitError{Stage: CommitStageValidate, Err: err}
	}

	if request.Mutate != nil {
		if err := request.Mutate(&candidate); err != nil {
			return event.Event{}, &CommitError{Stage: CommitStageValidate, Err: err}
		}
	}
	if request.Validate != nil {
		if err := request.Validate(before, candidate); err != nil {
			return event.Event{}, &CommitError{Stage: CommitStageValidate, Err: err}
		}
	}
	if request.Event.Source == "" || request.Event.Type == "" {
		return event.Event{}, &CommitError{
			Stage: CommitStageValidate,
			Err:   fmt.Errorf("event source and type are required"),
		}
	}

	appended, err := s.events.Append(request.Event)
	if err != nil {
		s.acceptingWork = false
		s.reportPersistenceFailure(err)
		return event.Event{}, &CommitError{Stage: CommitStageEvent, Err: err}
	}

	// Checkpoint the applied event sequence before persisting the projection so
	// a later crash can replay only events after this seq.
	candidate.AppliedEventSeq = appended.Seq
	candidate.UpdatedAt = time.Now().UTC()

	if err := s.persistSnapshotLocked(candidate); err != nil {
		// Event already exists on disk and must not be rolled back or forged-deleted.
		// Memory keeps the previous snapshot; recovery will reapply via the reducer.
		s.acceptingWork = false
		s.reportPersistenceFailure(err)
		return event.Event{}, &CommitError{Stage: CommitStageSnapshot, Err: err}
	}

	s.snapshot = candidate
	return appended, nil
}

// reportPersistenceFailure non-blockingly notifies observers of a fatal
// persistence error. The caller must already hold s.mu when mutating
// acceptingWork; this helper only writes the channel.
func (s *Service) reportPersistenceFailure(err error) {
	if err == nil || s.fatalPersistence == nil {
		return
	}
	select {
	case s.fatalPersistence <- err:
	default:
	}
}

// AcceptingWork reports whether Commit will still accept new work.
func (s *Service) AcceptingWork() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acceptingWork
}

// FatalPersistenceErrors returns the channel of fatal persistence failures.
// The channel is buffered (at least 1); only the first unread failure is kept
// when the buffer is full.
func (s *Service) FatalPersistenceErrors() <-chan error {
	return s.fatalPersistence
}
