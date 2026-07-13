package protocol

import (
	"sync"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/event"
)

func TestIsCriticalNativeEvent(t *testing.T) {
	critical := []string{
		event.ResultSubmitted, event.TurnFailed, event.PermissionRequested,
		event.TurnCompleted, event.SessionStarted, "protocol.error",
	}
	for _, kind := range critical {
		if !IsCriticalNativeEvent(kind) {
			t.Fatalf("%s should be critical", kind)
		}
	}
	if IsCriticalNativeEvent(event.ModelOutputDelta) {
		t.Fatal("token deltas are progress, not critical")
	}
}

func TestEmitCriticalSurvivesSaturation(t *testing.T) {
	// Capacity 2; flood with progress then deliver critical events.
	events := make(chan adapter.NativeEvent, 2)
	shutdown := make(chan struct{})
	var dropped uint64
	var mu sync.Mutex

	// Fill with progress (may drop).
	for i := 0; i < 100; i++ {
		EmitNativeEvent(EmitOptions{
			Events: events, Shutdown: shutdown, Mu: &mu, DroppedProgress: &dropped,
		}, adapter.NativeEvent{Kind: event.ModelOutputDelta, Timestamp: time.Now().UTC()})
	}

	// Consumer drains slowly in background so critical can complete.
	seen := make(chan string, 16)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range events {
			seen <- ev.Kind
		}
	}()

	// Critical events must not be silently lost.
	for _, kind := range []string{event.PermissionRequested, event.TurnFailed, event.ResultSubmitted} {
		EmitNativeEvent(EmitOptions{
			Events: events, Shutdown: shutdown, Mu: &mu, DroppedProgress: &dropped,
		}, adapter.NativeEvent{Kind: kind, Timestamp: time.Now().UTC()})
	}

	// Collect with timeout until all three critical kinds appear.
	need := map[string]bool{
		event.PermissionRequested: false,
		event.TurnFailed:          false,
		event.ResultSubmitted:     false,
	}
	deadline := time.After(2 * time.Second)
	for {
		all := true
		for _, ok := range need {
			if !ok {
				all = false
				break
			}
		}
		if all {
			break
		}
		select {
		case kind := <-seen:
			if _, tracked := need[kind]; tracked {
				need[kind] = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for critical events; got=%v dropped=%d", need, dropped)
		}
	}
	if dropped == 0 {
		t.Fatal("expected some progress drops under saturation")
	}

	close(shutdown)
	close(events)
	<-done
}

func TestEmitCriticalUnblocksOnShutdown(t *testing.T) {
	events := make(chan adapter.NativeEvent) // unbuffered, no consumer
	shutdown := make(chan struct{})
	var dropped uint64

	started := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		close(started)
		EmitNativeEvent(EmitOptions{
			Events: events, Shutdown: shutdown, DroppedProgress: &dropped,
		}, adapter.NativeEvent{Kind: event.ResultSubmitted, Timestamp: time.Now().UTC()})
		close(finished)
	}()
	<-started
	// Give the emitter a moment to block on send.
	time.Sleep(20 * time.Millisecond)
	close(shutdown)
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("critical emit did not unblock on shutdown")
	}
}

func TestEmitProgressMayDropWithCounter(t *testing.T) {
	events := make(chan adapter.NativeEvent, 1)
	// Fill channel so the next progress event must drop.
	events <- adapter.NativeEvent{Kind: event.ModelOutputDelta}
	var dropped uint64
	var mu sync.Mutex
	EmitNativeEvent(EmitOptions{
		Events: events, Mu: &mu, DroppedProgress: &dropped,
	}, adapter.NativeEvent{Kind: event.ModelOutputDelta, Timestamp: time.Now().UTC()})
	if dropped != 1 {
		t.Fatalf("dropped=%d", dropped)
	}
	// Only non-critical progress may be dropped; counter records the loss.
	// Synthetic warning is best-effort when the channel is full.
}
