package protocol

import (
	"sync"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/event"
)

func TestEventStreamCriticalSurvivesSaturation(t *testing.T) {
	s := NewEventStream(EventStreamOptions{OutputBuffer: 1, ProgressQueueLimit: 4})
	// Flood progress.
	for i := 0; i < 100; i++ {
		s.Publish(adapter.NativeEvent{Kind: event.ModelOutputDelta})
	}
	// Critical events must still be accepted.
	for _, kind := range []string{event.PermissionRequested, event.TurnFailed, event.ResultSubmitted} {
		if s.Publish(adapter.NativeEvent{Kind: kind}) != PublishAccepted {
			t.Fatalf("critical %s not accepted", kind)
		}
	}
	// Consumer drains later.
	got := map[string]bool{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range s.Events() {
			got[ev.Kind] = true
		}
	}()
	s.CloseGracefully()
	<-done
	for _, kind := range []string{event.PermissionRequested, event.TurnFailed, event.ResultSubmitted} {
		if !got[kind] {
			t.Fatalf("missing critical %s; got=%v dropped=%d", kind, got, s.DroppedProgress())
		}
	}
	if s.DroppedProgress() == 0 {
		t.Fatal("expected progress drops under pressure")
	}
}

func TestEventStreamProgressDropAccounting(t *testing.T) {
	s := NewEventStream(EventStreamOptions{OutputBuffer: 0, ProgressQueueLimit: 2})
	// With unbuffered out and no consumer, owner will hold at most one in-flight
	// send; internal progress queue is limited to 2.
	var drops int
	for i := 0; i < 20; i++ {
		if s.Publish(adapter.NativeEvent{Kind: event.ModelOutputDelta}) == PublishDroppedProgress {
			drops++
		}
	}
	if drops == 0 {
		t.Fatal("expected progress drops")
	}
	if s.DroppedProgress() == 0 {
		t.Fatal("dropped counter not updated")
	}
	// Unblock owner and finish.
	go func() {
		for range s.Events() {
		}
	}()
	s.Abort()
}

func TestEventStreamPublishVersusGracefulClose(t *testing.T) {
	s := NewEventStream(EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 64})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = s.Publish(adapter.NativeEvent{Kind: event.ModelOutputDelta})
				_ = s.Publish(adapter.NativeEvent{Kind: event.ResultSubmitted})
			}
		}()
	}
	go func() {
		for range s.Events() {
		}
	}()
	time.Sleep(5 * time.Millisecond)
	var closeWG sync.WaitGroup
	for i := 0; i < 8; i++ {
		closeWG.Add(1)
		go func() {
			defer closeWG.Done()
			s.CloseGracefully()
		}()
	}
	wg.Wait()
	closeWG.Wait()
	if s.Publish(adapter.NativeEvent{Kind: event.ResultSubmitted}) != PublishRejected {
		t.Fatal("publish after close must be rejected")
	}
}

func TestEventStreamPublishVersusAbort(t *testing.T) {
	s := NewEventStream(EventStreamOptions{OutputBuffer: 0, ProgressQueueLimit: 8})
	// No consumer: critical publish is accepted into queue; owner may block on out.
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = s.Publish(adapter.NativeEvent{Kind: event.ResultSubmitted})
			}
		}()
	}
	time.Sleep(5 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		s.Abort()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Abort did not complete")
	}
	wg.Wait()
	if s.Publish(adapter.NativeEvent{Kind: event.TurnFailed}) != PublishRejected {
		t.Fatal("publish after abort must be rejected")
	}
}

func TestEventStreamConcurrentCloseOps(t *testing.T) {
	s := NewEventStream(EventStreamOptions{OutputBuffer: 4, ProgressQueueLimit: 16})
	go func() {
		for range s.Events() {
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				s.CloseGracefully()
			} else {
				s.Abort()
			}
		}(i)
	}
	wg.Wait()
	select {
	case <-s.Done():
	default:
		t.Fatal("Done not closed")
	}
}

func TestEventStreamFIFOSerializedProducer(t *testing.T) {
	s := NewEventStream(EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 32})
	kinds := []string{event.TurnStarted, event.PermissionRequested, event.ResultSubmitted}
	for _, k := range kinds {
		if s.Publish(adapter.NativeEvent{Kind: k}) != PublishAccepted {
			t.Fatal(k)
		}
	}
	go s.CloseGracefully()
	var got []string
	for ev := range s.Events() {
		got = append(got, ev.Kind)
	}
	if len(got) != 3 {
		t.Fatalf("got=%v", got)
	}
	for i, k := range kinds {
		if got[i] != k {
			t.Fatalf("order got=%v want=%v", got, kinds)
		}
	}
}
