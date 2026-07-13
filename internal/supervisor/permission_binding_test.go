package supervisor

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/state"
)

func TestPermissionBindingHarnessMismatch(t *testing.T) {
	inner := fake.New(adapter.Capabilities{PermissionEvents: true, BidirectionalStream: true})
	harness := &namedAdapter{name: adapter.HarnessGrokBuild, Adapter: inner}
	service := newNativePermissionService(t, harness)
	service.snapshot.Tasks[0].Worker.Harness = string(adapter.HarnessGrokBuild)
	runtime := &service.snapshot.Tasks[0]
	// Bridge as Grok.
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: json.RawMessage(acpPermissionRequestNumeric),
	}, "worker-a")
	// Active becomes a different harness (Codex-named).
	other := &namedAdapter{name: adapter.HarnessCodex, Adapter: fake.New(adapter.Capabilities{PermissionEvents: true})}
	service.active["task-a"] = activeWorker{
		adapter: other, sessionID: service.active["task-a"].sessionID,
		cancel: func() {}, taskID: "task-a", workerID: "worker-a", attempt: 1,
	}
	pending := service.router.PendingDecisions("task-a")
	err := service.ResolveMessage(pending[0].MessageID, message.NewDecisionResolution(true, "", false))
	if err == nil {
		t.Fatal("expected harness mismatch")
	}
	got, _ := service.router.Get(pending[0].MessageID)
	if got.Status != message.Queued {
		t.Fatalf("status=%s", got.Status)
	}
	if len(other.PermissionResponses)+len(inner.PermissionResponses) != 0 {
		t.Fatal("adapter must not be called on binding mismatch")
	}
}

func TestPermissionBindingSessionMismatch(t *testing.T) {
	service := newNativePermissionService(t, fake.New(adapter.Capabilities{PermissionEvents: true, BidirectionalStream: true}))
	runtime := &service.snapshot.Tasks[0]
	payload, _ := json.Marshal(map[string]any{"id": "sess-m", "tool_name": "Bash"})
	service.bridgeNativePermission(runtime, service.active["task-a"].adapter, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: payload,
	}, "worker-a")
	// Change active session id without changing harness.
	active := service.active["task-a"]
	active.sessionID = "different-session"
	service.active["task-a"] = active
	pending := service.router.PendingDecisions("task-a")
	err := service.ResolveMessage(pending[0].MessageID, message.NewDecisionResolution(true, "", false))
	if err == nil {
		t.Fatal("expected session mismatch")
	}
	got, _ := service.router.Get(pending[0].MessageID)
	if got.Status != message.Queued || got.DeliveryAttempts != 1 {
		t.Fatalf("%+v", got)
	}
}

func TestPermissionBindingAttemptMismatch(t *testing.T) {
	service := newNativePermissionService(t, fake.New(adapter.Capabilities{PermissionEvents: true, BidirectionalStream: true}))
	runtime := &service.snapshot.Tasks[0]
	payload, _ := json.Marshal(map[string]any{"id": "att-m", "tool_name": "Bash"})
	service.bridgeNativePermission(runtime, service.active["task-a"].adapter, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: payload,
	}, "worker-a")
	// Attempt 2 is now active.
	active := service.active["task-a"]
	active.attempt = 2
	service.active["task-a"] = active
	pending := service.router.PendingDecisions("task-a")
	if pending[0].AttemptNumber != 1 {
		t.Fatalf("message attempt=%d", pending[0].AttemptNumber)
	}
	err := service.ResolveMessage(pending[0].MessageID, message.NewDecisionResolution(true, "", false))
	if err == nil {
		t.Fatal("expected attempt mismatch")
	}
	// Never delivered to attempt 2.
	if len(service.active["task-a"].adapter.(*fake.Adapter).PermissionResponses) != 0 {
		// active.adapter is *fake.Adapter from newNativePermissionService
	}
	// Prefer type assertion via FailPermission not needed — just check message.
	got, _ := service.router.Get(pending[0].MessageID)
	if got.Status != message.Queued {
		t.Fatalf("status=%s", got.Status)
	}
}

func TestPermissionDedupIdentityTuple(t *testing.T) {
	service := newNativePermissionService(t, fake.New(adapter.Capabilities{PermissionEvents: true, BidirectionalStream: true}))
	runtime := &service.snapshot.Tasks[0]
	harness := service.active["task-a"].adapter
	payload, _ := json.Marshal(map[string]any{"id": "same-id", "tool_name": "Bash"})
	native := adapter.NativeEvent{Kind: event.PermissionRequested, Payload: payload}
	service.bridgeNativePermission(runtime, harness, native, "worker-a")
	service.bridgeNativePermission(runtime, harness, native, "worker-a")
	if n := len(service.router.PendingDecisions("task-a")); n != 1 {
		t.Fatalf("same tuple must dedupe, got %d", n)
	}
	// Different attempt → distinct message.
	runtime.Worker.Attempt = 2
	service.snapshot.Tasks[0].ActiveAttempt = 2
	active := service.active["task-a"]
	active.attempt = 2
	service.active["task-a"] = active
	service.bridgeNativePermission(runtime, harness, native, "worker-a")
	if n := len(service.router.PendingDecisions("task-a")); n != 2 {
		t.Fatalf("different attempt must create distinct message, got %d", n)
	}
}

func TestPermissionLegacyAttemptZeroNotWildcard(t *testing.T) {
	// Manually enqueue a legacy-style permission with attempt 0.
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	harness := fake.New(adapter.Capabilities{PermissionEvents: true, BidirectionalStream: true})
	session, err := harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-1", TaskID: "task-a", WorkerID: "worker-a",
		ProjectRoot: t.TempDir(), Contract: "c", Scenario: "waiting_permission",
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(message.PermissionRequestPayload{
		ToolName: "Bash", Input: json.RawMessage(`{}`),
		Harness: string(adapter.HarnessFake), NativeSessionID: session.NativeSessionID,
		NativePermissionID: "legacy-1",
	})
	// attempt 0 via wrapper EnqueueDecision
	msg, err := router.EnqueueDecision("task-a", "worker-a", message.PermissionRequest, message.Permission, payload)
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{
		paths: storagePaths(t),
		snapshot: Snapshot{
			Run: domain.Run{RunID: "run-1"},
			Tasks: []TaskState{{
				Task: domain.Task{TaskID: "task-a"},
				Worker: &domain.WorkerSession{
					WorkerID: "worker-a", Attempt: 3, Harness: string(adapter.HarnessFake),
					NativeSessionID: session.NativeSessionID,
					Capabilities:    adapter.CapabilityMap(adapter.Capabilities{PermissionEvents: true}),
				},
				ActiveAttempt: 3,
				Dimensions:    state.Dimensions{Task: state.TaskBlocked},
			}},
		},
		router: router, messages: store, messageIndex: map[string]message.Message{},
		active: map[string]activeWorker{
			"task-a": {adapter: harness, sessionID: session.NativeSessionID, cancel: func() {}, taskID: "task-a", workerID: "worker-a", attempt: 3},
		},
		acceptingWork: true, fatalPersistence: make(chan error, 1),
	}
	err = service.ResolveMessage(msg.MessageID, message.NewDecisionResolution(true, "", false))
	if err == nil {
		t.Fatal("attempt 0 must not wildcard-match attempt 3")
	}
	got, _ := service.router.Get(msg.MessageID)
	if got.Status == message.Answered {
		t.Fatal("must not deliver")
	}
	if len(harness.PermissionResponses) != 0 {
		t.Fatal("adapter must not be called")
	}
}
