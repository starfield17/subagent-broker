package protocol

import (
	"sync"

	"github.com/vnai/subagent-broker/internal/adapter"
)

// PublishResult describes the outcome of EventStream.Publish.
type PublishResult int

const (
	// PublishAccepted means the event was queued for delivery.
	PublishAccepted PublishResult = iota
	// PublishDroppedProgress means a non-critical event was dropped under pressure.
	PublishDroppedProgress
	// PublishRejected means the stream is no longer accepting publications.
	PublishRejected
)

// EventStreamOptions configures a single-owner public event channel.
type EventStreamOptions struct {
	// OutputBuffer is the capacity of the public Session.Events channel.
	OutputBuffer int
	// ProgressQueueLimit bounds non-critical events held internally.
	// Critical events are not subject to this limit (low-volume control plane).
	ProgressQueueLimit int
}

// EventStream owns all sends to and the close of a public Events channel.
// Producers must only call Publish; they must never send to or close Events().
type EventStream struct {
	out chan adapter.NativeEvent

	mu              sync.Mutex
	critical        []adapter.NativeEvent
	progress        []adapter.NativeEvent
	progressLimit   int
	droppedProgress uint64
	accepting       bool
	gracefulClose   bool
	aborting        bool
	ownerExited     bool

	wake      chan struct{} // never closed; non-blocking signal to owner
	abortCh   chan struct{} // closed exactly once on Abort
	abortOnce sync.Once
	done      chan struct{}
	doneOnce  sync.Once

	ownerOnce sync.Once
}

// NewEventStream creates a stream and starts its single owner goroutine.
func NewEventStream(opts EventStreamOptions) *EventStream {
	if opts.OutputBuffer < 0 {
		opts.OutputBuffer = 0
	}
	if opts.ProgressQueueLimit <= 0 {
		opts.ProgressQueueLimit = 256
	}
	s := &EventStream{
		out:           make(chan adapter.NativeEvent, opts.OutputBuffer),
		progressLimit: opts.ProgressQueueLimit,
		accepting:     true,
		wake:          make(chan struct{}, 1),
		abortCh:       make(chan struct{}),
		done:          make(chan struct{}),
	}
	s.ownerOnce.Do(func() { go s.ownerLoop() })
	return s
}

// Events is the public read-only channel. Only the owner goroutine sends/closes it.
func (s *EventStream) Events() <-chan adapter.NativeEvent {
	return s.out
}

// Done is closed when the owner has finished and the public channel is closed.
func (s *EventStream) Done() <-chan struct{} {
	return s.done
}

// DroppedProgress returns the number of progress events discarded under pressure.
func (s *EventStream) DroppedProgress() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.droppedProgress
}

// Publish queues an event for the owner. Never panics after close/abort.
// Does not block on the public consumer.
func (s *EventStream) Publish(native adapter.NativeEvent) PublishResult {
	s.mu.Lock()
	if !s.accepting || s.aborting {
		s.mu.Unlock()
		return PublishRejected
	}
	if IsCriticalNativeEvent(native.Kind) {
		s.critical = append(s.critical, native)
		s.mu.Unlock()
		s.signal()
		return PublishAccepted
	}
	if len(s.progress) >= s.progressLimit {
		s.droppedProgress++
		s.mu.Unlock()
		return PublishDroppedProgress
	}
	s.progress = append(s.progress, native)
	s.mu.Unlock()
	s.signal()
	return PublishAccepted
}

// CloseGracefully stops accepting new events and drains accepted ones, then
// closes the public channel. Idempotent and concurrent-safe with Abort.
func (s *EventStream) CloseGracefully() {
	s.mu.Lock()
	s.accepting = false
	s.gracefulClose = true
	s.mu.Unlock()
	s.signal()
	<-s.done
}

// Abort stops accepting events, unblocks the owner even if the public consumer
// is stalled, and closes the public channel. Queued events may be discarded.
// Idempotent and concurrent-safe with CloseGracefully.
func (s *EventStream) Abort() {
	s.mu.Lock()
	s.accepting = false
	s.aborting = true
	s.mu.Unlock()
	s.abortOnce.Do(func() { close(s.abortCh) })
	s.signal()
	<-s.done
}

// TryCloseGracefully is like CloseGracefully but does not wait for the owner
// when already done (non-blocking check). Prefer CloseGracefully for normal exit.
func (s *EventStream) TryCloseGracefully() {
	s.mu.Lock()
	s.accepting = false
	s.gracefulClose = true
	exited := s.ownerExited
	s.mu.Unlock()
	s.signal()
	if exited {
		return
	}
	// Wait with abort awareness so dual close paths cannot hang forever.
	select {
	case <-s.done:
	case <-s.abortCh:
		<-s.done
	}
}

func (s *EventStream) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *EventStream) finish() {
	s.doneOnce.Do(func() {
		s.mu.Lock()
		s.ownerExited = true
		s.accepting = false
		s.mu.Unlock()
		close(s.out)
		close(s.done)
	})
}

func (s *EventStream) ownerLoop() {
	defer s.finish()
	for {
		// Abort: discard queues and exit promptly.
		s.mu.Lock()
		if s.aborting {
			s.critical = nil
			s.progress = nil
			s.mu.Unlock()
			return
		}
		// Take next event: critical first, then progress.
		var next adapter.NativeEvent
		var have bool
		if len(s.critical) > 0 {
			next = s.critical[0]
			s.critical = s.critical[1:]
			have = true
		} else if len(s.progress) > 0 {
			next = s.progress[0]
			s.progress = s.progress[1:]
			have = true
		}
		draining := s.gracefulClose && !s.accepting
		empty := len(s.critical) == 0 && len(s.progress) == 0
		s.mu.Unlock()

		if have {
			if !s.forward(next) {
				return // aborted while forwarding
			}
			continue
		}
		if draining && empty {
			return
		}
		// Wait for work, graceful close, or abort.
		select {
		case <-s.wake:
		case <-s.abortCh:
			return
		}
	}
}

// forward sends to the public channel. Returns false if aborted (caller should exit).
func (s *EventStream) forward(native adapter.NativeEvent) bool {
	select {
	case s.out <- native:
		return true
	case <-s.abortCh:
		return false
	}
}
