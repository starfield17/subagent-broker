package codex

import (
	"sync"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/protocol"
	"github.com/vnai/subagent-broker/internal/event"
)

// TestCriticalEventsSurviveChannelSaturation floods progress then ensures
// critical events surface through the single-owner EventStream.
func TestCriticalEventsSurviveChannelSaturation(t *testing.T) {
	state := &sessionState{
		stream: protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 2, ProgressQueueLimit: 4}),
	}
	a := &Adapter{}
	for i := 0; i < 500; i++ {
		a.recordEvent(state, adapter.NativeEvent{Kind: event.ModelOutputDelta, Timestamp: time.Now().UTC()})
	}
	if state.stream.DroppedProgress() == 0 {
		t.Fatal("expected progress drops under saturation")
	}

	got := make(chan string, 32)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ev := range state.stream.Events() {
			got <- ev.Kind
		}
	}()

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
			t.Fatalf("missing critical events: %v dropped=%d", need, state.stream.DroppedProgress())
		}
	}
	state.stream.CloseGracefully()
	wg.Wait()
}

func TestRecordEventUnblocksOnTerminateAbort(t *testing.T) {
	// Unbuffered public channel with no consumer: Abort must not hang publishers.
	state := &sessionState{
		stream: protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 0, ProgressQueueLimit: 8}),
	}
	a := &Adapter{}
	finished := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			a.recordEvent(state, adapter.NativeEvent{Kind: event.ResultSubmitted, Timestamp: time.Now().UTC()})
		}
		close(finished)
	}()
	time.Sleep(20 * time.Millisecond)
	state.stream.Abort()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("recordEvent publishers did not complete after Abort")
	}
}
