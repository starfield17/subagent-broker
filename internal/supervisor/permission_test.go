package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/state"
)

func newNativePermissionService(t *testing.T, harness adapter.Adapter) *Service {
	t.Helper()
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	session, err := harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-1", TaskID: "task-a", WorkerID: "worker-a",
		ProjectRoot: t.TempDir(), Contract: "contract", Scenario: "waiting_permission",
	})
	if err != nil {
		t.Fatal(err)
	}
	caps := harness.Descriptor().Capabilities
	// Native permission harnesses declare PermissionEvents without Hooks.
	if !caps.PermissionEvents {
		t.Fatal("test harness must declare PermissionEvents")
	}
	service := &Service{
		paths: storagePaths(t),
		snapshot: Snapshot{
			SchemaVersion: SchemaVersion,
			Run:           domain.Run{RunID: "run-1", ProjectID: "p1", Status: domain.RunRunning},
			Tasks: []TaskState{{
				Task: domain.Task{TaskID: "task-a", WriteScope: []string{"a/**"}, HarnessPreference: string(harness.Descriptor().Name)},
				Worker: &domain.WorkerSession{
					WorkerID:        "worker-a",
					TaskID:          "task-a",
					Harness:         string(harness.Descriptor().Name),
					NativeSessionID: session.NativeSessionID,
					Capabilities:    adapter.CapabilityMap(adapter.Capabilities{PermissionEvents: true, BidirectionalStream: true}),
					StatusDimensions: state.Dimensions{
						Process: state.ProcessAlive, Protocol: state.ProtocolThinking,
						Progress: state.ProgressActive, Task: state.TaskRunning,
					},
				},
				Dimensions: state.Dimensions{Task: state.TaskRunning, Process: state.ProcessAlive},
			}},
		},
		messages:         store,
		messageIndex:     map[string]message.Message{},
		router:           router,
		pending:          map[string]chan message.Resolution{},
		active:           map[string]activeWorker{},
		acceptingWork:    true,
		fatalPersistence: make(chan error, 1),
	}
	service.active["task-a"] = activeWorker{
		adapter:   harness,
		sessionID: session.NativeSessionID,
		cancel:    func() {},
		taskID:    "task-a",
		workerID:  "worker-a",
	}
	return service
}

func TestNativePermissionEventCreatesExactlyOneMessage(t *testing.T) {
	harness := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true, SessionHistory: true,
	})
	service := newNativePermissionService(t, harness)
	runtime := &service.snapshot.Tasks[0]
	payload, _ := json.Marshal(map[string]any{
		"id": "native-req-1", "tool_name": "Bash", "input": map[string]string{"command": "ls"},
	})
	native := adapter.NativeEvent{Kind: event.PermissionRequested, Payload: payload}

	// Unit tests exercise the bridge directly (handleNative also updates runtime
	// via Commit which needs an event sink).
	service.bridgeNativePermission(runtime, harness, native, "worker-a")
	service.bridgeNativePermission(runtime, harness, native, "worker-a") // replay

	pending := service.router.PendingDecisions("task-a")
	if len(pending) != 1 {
		t.Fatalf("want exactly 1 permission message, got %d: %+v", len(pending), pending)
	}
	if pending[0].Type != message.PermissionRequest {
		t.Fatalf("type=%s", pending[0].Type)
	}
	var body message.PermissionRequestPayload
	if err := json.Unmarshal(pending[0].Payload, &body); err != nil {
		t.Fatal(err)
	}
	if body.NativePermissionID != "native-req-1" {
		t.Fatalf("native id=%q", body.NativePermissionID)
	}
	if body.NativeSessionID == "" || body.Harness == "" {
		t.Fatalf("missing routing metadata: %+v", body)
	}
	if body.ToolName != "Bash" {
		t.Fatalf("tool=%q", body.ToolName)
	}
}

func TestNativePermissionAllowReachesAdapter(t *testing.T) {
	harness := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	service := newNativePermissionService(t, harness)
	runtime := &service.snapshot.Tasks[0]
	payload, _ := json.Marshal(map[string]any{"id": "allow-1", "tool_name": "Write"})
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{Kind: event.PermissionRequested, Payload: payload}, "worker-a")

	pending := service.router.PendingDecisions("task-a")
	if len(pending) != 1 {
		t.Fatalf("pending=%d", len(pending))
	}
	if err := service.ResolveMessage(pending[0].MessageID, message.Resolution{
		Decision: message.DecisionPayload{Allowed: true, Reason: "ok"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(harness.PermissionResponses) != 1 {
		t.Fatalf("responses=%+v", harness.PermissionResponses)
	}
	got := harness.PermissionResponses[0]
	if !got.Allowed || got.RequestID != "allow-1" {
		t.Fatalf("decision=%+v", got)
	}
	answered, ok := service.router.Get(pending[0].MessageID)
	if !ok || answered.Status != message.Answered {
		t.Fatalf("status=%+v", answered)
	}
}

func TestNativePermissionDenyReachesAdapter(t *testing.T) {
	harness := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	service := newNativePermissionService(t, harness)
	runtime := &service.snapshot.Tasks[0]
	payload, _ := json.Marshal(map[string]any{"id": "deny-1", "tool_name": "Bash"})
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{Kind: event.PermissionRequested, Payload: payload}, "worker-a")

	pending := service.router.PendingDecisions("task-a")
	if err := service.ResolveMessage(pending[0].MessageID, message.Resolution{
		Decision: message.DecisionPayload{Allowed: false, Reason: "no"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(harness.PermissionResponses) != 1 || harness.PermissionResponses[0].Allowed {
		t.Fatalf("responses=%+v", harness.PermissionResponses)
	}
	if harness.PermissionResponses[0].RequestID != "deny-1" {
		t.Fatalf("id=%q", harness.PermissionResponses[0].RequestID)
	}
}

func TestNativePermissionAdapterFailureNotRecordedAnswered(t *testing.T) {
	harness := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	harness.FailPermissionResponse = errors.New("adapter transport down")
	service := newNativePermissionService(t, harness)
	runtime := &service.snapshot.Tasks[0]
	payload, _ := json.Marshal(map[string]any{"id": "fail-1", "tool_name": "Bash"})
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{Kind: event.PermissionRequested, Payload: payload}, "worker-a")

	pending := service.router.PendingDecisions("task-a")
	err := service.ResolveMessage(pending[0].MessageID, message.Resolution{
		Decision: message.DecisionPayload{Allowed: true, Reason: "ok"},
	})
	if err == nil {
		t.Fatal("expected delivery error")
	}
	got, ok := service.router.Get(pending[0].MessageID)
	if !ok {
		t.Fatal("message missing")
	}
	if got.Status == message.Answered {
		t.Fatal("must not record Answered after adapter delivery failure")
	}
	if got.Status != message.Failed {
		t.Fatalf("status=%s want failed", got.Status)
	}
	if got.Error == "" {
		t.Fatal("expected error text on failed message")
	}
}

func TestParseNativePermissionPayloadJSONRPC(t *testing.T) {
	raw := json.RawMessage(`{"id":42,"method":"item/commandExecution/requestApproval","params":{"command":"rm -rf /"}}`)
	id, tool, input := parseNativePermissionPayload(raw)
	if id != "42" {
		t.Fatalf("id=%q", id)
	}
	if tool == "" {
		t.Fatal("expected tool/method")
	}
	if len(input) == 0 {
		t.Fatal("expected input")
	}
}
