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
	// ProgressQueueLimit bounds non-critical events held internally before acceptance.
	// Once accepted, events keep global order with critical events.
	ProgressQueueLimit int
}

type queuedEvent struct {
	event    adapter.NativeEvent
	critical bool
	seq      uint64
}

// EventStream owns all sends to and the close of a public Events channel.
// Producers must only call Publish; they must never send to or close Events().
// Accepted events preserve global publication order across critical/progress classes.
type EventStream struct {
	out chan adapter.NativeEvent

	mu              sync.Mutex
	queue           []queuedEvent
	progressInQueue int
	progressLimit   int
	nextSeq         uint64
	droppedProgress uint64
	accepting       bool
	gracefulClose   bool
	aborting        bool
	ownerExited     bool

	wake      chan struct{}
	abortCh   chan struct{}
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
// Critical events are never dropped for ordinary saturation.
// Progress may be dropped before acceptance when progress capacity is exhausted.
// Once accepted, events keep global order (critical does not overtake earlier progress).
func (s *EventStream) Publish(native adapter.NativeEvent) PublishResult {
	s.mu.Lock()
	if !s.accepting || s.aborting {
		s.mu.Unlock()
		return PublishRejected
	}
	critical := IsCriticalNativeEvent(native.Kind)
	if !critical {
		if s.progressInQueue >= s.progressLimit {
			s.droppedProgress++
			s.mu.Unlock()
			return PublishDroppedProgress
		}
		s.progressInQueue++
	}
	s.nextSeq++
	s.queue = append(s.queue, queuedEvent{event: native, critical: critical, seq: s.nextSeq})
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
func (s *EventStream) Abort() {
	s.mu.Lock()
	s.accepting = false
	s.aborting = true
	s.mu.Unlock()
	s.abortOnce.Do(func() { close(s.abortCh) })
	s.signal()
	<-s.done
}

// TryCloseGracefully is like CloseGracefully but handles concurrent abort.
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
		s.mu.Lock()
		if s.aborting {
			s.queue = nil
			s.progressInQueue = 0
			s.mu.Unlock()
			return
		}
		// Drain in global acceptance order (FIFO by sequence).
		var next queuedEvent
		var have bool
		if len(s.queue) > 0 {
			next = s.queue[0]
			s.queue = s.queue[1:]
			if !next.critical {
				s.progressInQueue--
				if s.progressInQueue < 0 {
					s.progressInQueue = 0
				}
			}
			have = true
		}
		draining := s.gracefulClose && !s.accepting
		empty := len(s.queue) == 0
		s.mu.Unlock()

		if have {
			if !s.forward(next.event) {
				return
			}
			continue
		}
		if draining && empty {
			return
		}
		select {
		case <-s.wake:
		case <-s.abortCh:
			return
		}
	}
}

func (s *EventStream) forward(native adapter.NativeEvent) bool {
	select {
	case s.out <- native:
		return true
	case <-s.abortCh:
		return false
	}
}
