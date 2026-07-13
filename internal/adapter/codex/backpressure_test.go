package codex

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/event"
)

// TestCriticalEventsSurviveChannelSaturation floods a tiny event buffer with
// progress noise then ensures result/permission/failure still surface and the
// adapter can exit cleanly after shutdown.
func TestCriticalEventsSurviveChannelSaturation(t *testing.T) {
	state := &sessionState{
		events:   make(chan adapter.NativeEvent, 2),
		shutdown: make(chan struct{}),
	}

	// Flood progress without a consumer.
	for i := 0; i < 500; i++ {
		a := &Adapter{}
		a.recordEvent(state, adapter.NativeEvent{Kind: event.ModelOutputDelta, Timestamp: time.Now().UTC()})
	}
	if state.droppedProgress == 0 {
		t.Fatal("expected progress drops under saturation")
	}

	// Slow consumer.
	got := make(chan string, 32)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ev := range state.events {
			got <- ev.Kind
		}
	}()

	a := &Adapter{}
	for _, kind := range []string{event.PermissionRequested, event.TurnFailed, event.ResultSubmitted} {
		a.recordEvent(state, adapter.NativeEvent{Kind: kind, Timestamp: time.Now().UTC()})
	}

	need := map[string]bool{
		event.PermissionRequested: false,
		event.TurnFailed:          false,
		event.ResultSubmitted:     false,
	}
	deadline := time.After(3 * time.Second)
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
		case kind := <-got:
			if _, tracked := need[kind]; tracked {
				need[kind] = true
			}
		case <-deadline:
			t.Fatalf("missing critical events: %v dropped=%d", need, state.droppedProgress)
		}
	}

	// Shutdown must release any blocked producer and allow clean close.
	state.shutdownOnce.Do(func() { close(state.shutdown) })
	state.closeEvents.Do(func() {
		state.mu.Lock()
		state.closed = true
		state.mu.Unlock()
		close(state.events)
	})
	wg.Wait()
}

func TestRecordEventUnblocksOnTerminate(t *testing.T) {
	// Unbuffered events with no consumer: critical emit blocks until shutdown.
	state := &sessionState{
		events:   make(chan adapter.NativeEvent),
		shutdown: make(chan struct{}),
	}
	a := &Adapter{}
	finished := make(chan struct{})
	go func() {
		a.recordEvent(state, adapter.NativeEvent{Kind: event.ResultSubmitted, Timestamp: time.Now().UTC()})
		close(finished)
	}()
	time.Sleep(20 * time.Millisecond)
	state.shutdownOnce.Do(func() { close(state.shutdown) })
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("recordEvent did not unblock on shutdown")
	}
	// Ensure context is used so the test stays cancel-aware for future work.
	_ = context.Background()
}
