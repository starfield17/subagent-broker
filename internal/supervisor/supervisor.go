package supervisor

import (
	"context"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
)

// EventSink is the only path through which adapters and workers may propose
// Run facts. A future Run-scoped Supervisor owns the concrete single-writer
// implementation and state transitions.
type EventSink interface {
	Append(event.Input) (event.Event, error)
}

// SnapshotStore persists atomic state snapshots. Markdown is never parsed back
// into runtime state.
type SnapshotStore interface {
	SaveRun(context.Context, domain.Run) error
	LoadRun(context.Context, domain.RunID) (domain.Run, error)
}

// Controller is the Phase 0 boundary for the future Run-scoped Supervisor.
// It intentionally omits a production implementation until Phase 1 proves a
// complete single-Harness lifecycle.
type Controller interface {
	Start(context.Context, domain.Run) error
	Cancel(context.Context, domain.RunID) error
	Wait(context.Context, domain.RunID) error
	Shutdown(context.Context, domain.RunID) error
}
