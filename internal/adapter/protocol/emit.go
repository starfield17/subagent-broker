package protocol

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/event"
)

// IsCriticalNativeEvent reports whether silently dropping the event would lose
// lifecycle control (result, failure, permission, session/turn completion, fatal).
func IsCriticalNativeEvent(kind string) bool {
	switch kind {
	case event.ResultSubmitted,
		event.TurnFailed,
		event.PermissionRequested,
		event.TurnCompleted,
		event.TurnStarted,
		event.TurnInterrupted,
		event.SessionStarted,
		event.SessionResumed,
		event.SessionTerminated,
		event.ProcessExited,
		event.ProcessOrphaned,
		event.UserInputRequested,
		event.ScopeExpansionRequested,
		"protocol.error",
		"protocol.progress_dropped":
		return true
	default:
		if strings.HasPrefix(kind, "protocol.") && kind != "protocol.thinking" {
			return true
		}
		return false
	}
}

// EmitOptions configures reliable native event delivery under backpressure.
type EmitOptions struct {
	// Events is the consumer-facing channel (must be non-nil).
	Events chan<- adapter.NativeEvent
	// Shutdown is closed when the session is terminating; unblocks critical send.
	Shutdown <-chan struct{}
	// Mu protects DroppedProgress and optional history append by the caller.
	// Emit itself only uses DroppedProgress under Mu when Mu is non-nil.
	Mu *sync.Mutex
	// DroppedProgress counts non-critical events discarded under pressure.
	DroppedProgress *uint64
}

// EmitNativeEvent delivers a native event with critical-blocking / progress-drop
// policy. Critical events never use a pure non-blocking default drop. Progress
// events may be dropped; a synthetic protocol.progress_dropped warning is
// emitted occasionally when drops occur. Shutdown unblocks critical producers.
func EmitNativeEvent(opts EmitOptions, native adapter.NativeEvent) {
	if opts.Events == nil {
		return
	}
	if IsCriticalNativeEvent(native.Kind) {
		emitCritical(opts, native)
		return
	}
	select {
	case opts.Events <- native:
		return
	default:
	}
	// Progress lost under pressure.
	var dropped uint64
	if opts.Mu != nil && opts.DroppedProgress != nil {
		opts.Mu.Lock()
		*opts.DroppedProgress++
		dropped = *opts.DroppedProgress
		opts.Mu.Unlock()
	} else if opts.DroppedProgress != nil {
		*opts.DroppedProgress++
		dropped = *opts.DroppedProgress
	} else {
		dropped = 1
	}
	// First drop and every 32nd thereafter: best-effort synthetic warning.
	// Never block here — the durable counter already records loss.
	if dropped != 1 && dropped%32 != 0 {
		return
	}
	payload, _ := json.Marshal(map[string]any{
		"dropped_progress": dropped,
		"last_kind":        native.Kind,
	})
	warn := adapter.NativeEvent{
		Kind:      "protocol.progress_dropped",
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
	select {
	case opts.Events <- warn:
	default:
	}
}

func emitCritical(opts EmitOptions, native adapter.NativeEvent) {
	if opts.Shutdown == nil {
		// No shutdown signal: block until the consumer drains.
		opts.Events <- native
		return
	}
	select {
	case opts.Events <- native:
	case <-opts.Shutdown:
		// Session ending: best-effort non-blocking so producers can exit.
		select {
		case opts.Events <- native:
		default:
		}
	}
}
