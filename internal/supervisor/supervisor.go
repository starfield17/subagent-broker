package supervisor

import (
	"context"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
)

// EventSink is the only path through which adapters and workers may propose
// Run facts. The Run-scoped Supervisor owns the concrete single-writer
// implementation and state transitions.
//
// For Supervisor-owned state changes that must also update the Snapshot
// projection, prefer Service.Commit: it deep-copies a candidate Snapshot,
// appends one event, persists the projection, then installs memory only on
// full success. Event append still precedes Snapshot write; a snapshot-stage
// failure is fail-closed and is not a multi-file database rollback.
type EventSink interface {
	Append(event.Input) (event.Event, error)
}

// SnapshotStore persists atomic state snapshots. Markdown is never parsed back
// into runtime state.
type SnapshotStore interface {
	SaveRun(context.Context, domain.Run) error
	LoadRun(context.Context, domain.RunID) (domain.Run, error)
}

// Controller is the minimal lifecycle boundary implemented by the Run-scoped
// Supervisor.
type Controller interface {
	Start(context.Context, domain.Run) error
	Cancel(context.Context, domain.RunID) error
	Wait(context.Context, domain.RunID) error
	Shutdown(context.Context, domain.RunID) error
}
