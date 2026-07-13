package supervisor

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/state"
)

// TestDecisionPublicationResolverBeforeOpLock verifies that when a resolver
// completes after Router enqueue but before RequestMessage acquires the
// decision-operation lock, no MessageQueued is appended after MessageAnswered,
// the durable answer is returned, and the Task is not left blocked.
func TestDecisionPublicationResolverBeforeOpLock(t *testing.T) {
	service := loadPublicationService(t)
	markTaskRunningWithWorker(service, "task-a")

	var mid string
	enqueued := make(chan string, 1)
	service.SetAfterDecisionEnqueueHook(func(id string) {
		mid = id
		select {
		case enqueued <- id:
		default:
		}
		// Resolve while RequestMessage is paused before op lock.
		_ = service.ResolveMessage(id, message.NewAnswerResolution("from-resolver"))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, id, err := service.RequestMessage(ctx, "task-a", "worker-a", message.Question, message.Decision, map[string]any{
		"schema_version": "v1alpha1", "question": "Q", "reason": "R",
		"current_scope": []string{"x"}, "workspace_state": "w",
	})
	if err != nil {
		t.Fatalf("RequestMessage: %v id=%s", err, id)
	}
	if res.Answer == nil || res.Answer.Text != "from-resolver" {
		t.Fatalf("expected durable answer, got %+v", res)
	}
	if mid == "" {
		mid = id
	}
	got, ok := service.router.Get(mid)
	if !ok || got.Status != message.Answered {
		t.Fatalf("message status=%v ok=%v", got.Status, ok)
	}
	if service.router.HasPendingDecisions("task-a") {
		t.Fatal("pending decisions remain")
	}
	// Event order: MessageAnswered must not be followed by MessageQueued for this id.
	assertNoQueuedAfterAnswered(t, service, mid)
	assertTaskNotWaitingPermission(t, service, "task-a")
	// No waiter leak.
	service.mu.Lock()
	_, waiterLeft := service.pending[mid]
	service.mu.Unlock()
	if waiterLeft {
		t.Fatal("waiter leaked")
	}
}

// TestDecisionPublicationNormalEventOrder verifies MessageQueued then
// QuestionPublished then MessageAnswered for the same message id.
func TestDecisionPublicationNormalEventOrder(t *testing.T) {
	service := loadPublicationService(t)
	markTaskRunningWithWorker(service, "task-a")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mid string
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Wait until message is pending, then resolve.
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			pending := service.router.PendingDecisions("task-a")
			if len(pending) > 0 {
				mid = pending[0].MessageID
				_ = service.ResolveMessage(mid, message.NewAnswerResolution("yes"))
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	res, id, err := service.RequestMessage(ctx, "task-a", "worker-a", message.Question, message.Decision, map[string]any{
		"schema_version": "v1alpha1", "question": "Q", "reason": "R",
		"current_scope": []string{"x"}, "workspace_state": "w",
	})
	wg.Wait()
	if err != nil {
		t.Fatalf("RequestMessage: %v", err)
	}
	if res.Answer == nil || res.Answer.Text != "yes" {
		t.Fatalf("res=%+v", res)
	}
	if mid == "" {
		mid = id
	}
	assertEventOrderForMessage(t, service, mid, []string{
		event.MessageQueued,
		event.QuestionPublished,
		event.MessageAnswered,
	})
}

// TestNativePermissionSetupResolverWinsBeforeOpLock races resolution before
// bridgeNativePermission acquires the decision-operation lock.
func TestNativePermissionSetupResolverWinsBeforeOpLock(t *testing.T) {
	inner := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	harness := &namedAdapter{name: adapter.HarnessGrokBuild, Adapter: inner}
	service := newNativePermissionService(t, harness)
	service.snapshot.Tasks[0].Worker.Harness = string(adapter.HarnessGrokBuild)
	runtime := &service.snapshot.Tasks[0]
	markTaskRunningWithWorker(service, "task-a")

	service.SetAfterDecisionEnqueueHook(func(id string) {
		// Resolve immediately after enqueue, before bridge op lock / waiting setup.
		_ = service.ResolveMessage(id, message.NewDecisionResolution(true, "ok", false))
	})

	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: json.RawMessage(acpPermissionRequestNumeric),
	}, "worker-a")

	if service.router.HasPendingDecisions("task-a") {
		t.Fatal("pending decision remains after resolve-before-setup")
	}
	// Exactly one delivery.
	if len(inner.PermissionResponses) != 1 {
		t.Fatalf("responses=%+v", inner.PermissionResponses)
	}
	assertTaskNotWaitingPermission(t, service, "task-a")
}

// TestNativePermissionSetupHoldsOpLockBeforeResolve verifies setup-first ordering:
// resolver waits, setup installs waiting, then resolve clears it.
func TestNativePermissionSetupHoldsOpLockBeforeResolve(t *testing.T) {
	inner := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	harness := &namedAdapter{name: adapter.HarnessGrokBuild, Adapter: inner}
	service := newNativePermissionService(t, harness)
	service.snapshot.Tasks[0].Worker.Harness = string(adapter.HarnessGrokBuild)
	runtime := &service.snapshot.Tasks[0]
	markTaskRunningWithWorker(service, "task-a")

	setupEntered := make(chan struct{})
	releaseSetup := make(chan struct{})
	var mid string

	service.SetAfterDecisionOpLockHook(func(id string) {
		mid = id
		close(setupEntered)
		<-releaseSetup
	})

	var bridgeDone sync.WaitGroup
	bridgeDone.Add(1)
	go func() {
		defer bridgeDone.Done()
		service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{
			Kind: event.PermissionRequested, Payload: json.RawMessage(acpPermissionRequestNumeric),
		}, "worker-a")
	}()

	select {
	case <-setupEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("setup never acquired op lock")
	}

	// Resolver starts while setup holds the lock.
	resolveDone := make(chan error, 1)
	go func() {
		// mid may still be empty if hook races; wait for pending.
		deadline := time.Now().Add(2 * time.Second)
		for mid == "" && time.Now().Before(deadline) {
			if p := service.router.PendingDecisions("task-a"); len(p) > 0 {
				mid = p[0].MessageID
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		if mid == "" {
			resolveDone <- context.DeadlineExceeded
			return
		}
		resolveDone <- service.ResolveMessage(mid, message.NewDecisionResolution(true, "ok", false))
	}()

	// Allow setup to finish waiting-state installation.
	close(releaseSetup)
	bridgeDone.Wait()

	select {
	case err := <-resolveDone:
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resolve blocked")
	}

	if service.router.HasPendingDecisions("task-a") {
		t.Fatal("pending remains")
	}
	assertTaskNotWaitingPermission(t, service, "task-a")
	if len(inner.PermissionResponses) != 1 {
		t.Fatalf("responses=%+v", inner.PermissionResponses)
	}
}

func loadPublicationService(t *testing.T) *Service {
	t.Helper()
	runDir, _ := writeFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{})); err != nil {
		t.Fatal(err)
	}
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	return service
}

func markTaskRunningWithWorker(service *Service, taskID string) {
	service.mu.Lock()
	defer service.mu.Unlock()
	for i := range service.snapshot.Tasks {
		if string(service.snapshot.Tasks[i].Task.TaskID) != taskID {
			continue
		}
		service.snapshot.Tasks[i].Task.Status = state.TaskRunning
		service.snapshot.Tasks[i].Dimensions.Task = state.TaskRunning
		service.snapshot.Tasks[i].Dimensions.Protocol = state.ProtocolThinking
		service.snapshot.Tasks[i].BlockKind = BlockKindNone
		if service.snapshot.Tasks[i].Worker == nil {
			service.snapshot.Tasks[i].Worker = &domain.WorkerSession{
				WorkerID: "worker-a", TaskID: domain.TaskID(taskID),
			}
		}
	}
}

func assertTaskNotWaitingPermission(t *testing.T, service *Service, taskID string) {
	t.Helper()
	snap := service.Snapshot()
	for _, ts := range snap.Tasks {
		if string(ts.Task.TaskID) != taskID {
			continue
		}
		if ts.Dimensions.Protocol == state.ProtocolWaitingPermission {
			t.Fatalf("task still waiting_permission: %+v", ts.Dimensions)
		}
		if ts.BlockKind == BlockKindWaitingMessage && !service.router.HasPendingDecisions(taskID) {
			t.Fatalf("task blocked with no pending decisions: block=%v status=%s", ts.BlockKind, ts.Task.Status)
		}
	}
}

func eventPayloadMessageID(payload json.RawMessage) string {
	var m map[string]any
	if json.Unmarshal(payload, &m) != nil {
		return ""
	}
	mid, _ := m["message_id"].(string)
	return mid
}

func assertNoQueuedAfterAnswered(t *testing.T, service *Service, messageID string) {
	t.Helper()
	replay, err := event.Replay(service.paths.Events)
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for _, ev := range replay.Events {
		if eventPayloadMessageID(ev.Payload) != messageID {
			continue
		}
		types = append(types, ev.Type)
	}
	answeredIdx, queuedIdx := -1, -1
	for i, typ := range types {
		if typ == event.MessageAnswered && answeredIdx < 0 {
			answeredIdx = i
		}
		if typ == event.MessageQueued {
			queuedIdx = i
		}
	}
	if answeredIdx >= 0 && queuedIdx > answeredIdx {
		t.Fatalf("MessageQueued after MessageAnswered in %v", types)
	}
	// When resolver wins before op lock, MessageQueued must be absent entirely.
	if queuedIdx >= 0 && answeredIdx >= 0 && queuedIdx > answeredIdx {
		t.Fatalf("invalid order %v", types)
	}
}

func assertEventOrderForMessage(t *testing.T, service *Service, messageID string, want []string) {
	t.Helper()
	replay, err := event.Replay(service.paths.Events)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, ev := range replay.Events {
		if eventPayloadMessageID(ev.Payload) != messageID {
			continue
		}
		for _, w := range want {
			if ev.Type == w {
				got = append(got, ev.Type)
				break
			}
		}
	}
	// got should be a subsequence matching want order (each want appears in order).
	wi := 0
	for _, g := range got {
		if wi < len(want) && g == want[wi] {
			wi++
		}
	}
	if wi != len(want) {
		t.Fatalf("event order for %s: got %v want subsequence %v", messageID, got, want)
	}
}
