package supervisor

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
)

func TestInvalidNativeAnswerResolutionHasNoAdapterSideEffects(t *testing.T) {
	harness := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	service := newNativePermissionService(t, harness)
	runtime := &service.snapshot.Tasks[0]
	payload, _ := json.Marshal(map[string]any{"id": "invalid-answer", "tool_name": "Bash"})
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{Kind: event.PermissionRequested, Payload: payload}, "worker-a")
	pending := service.router.PendingDecisions("task-a")
	if len(pending) != 1 {
		t.Fatalf("pending=%d", len(pending))
	}

	err := service.ResolveMessage(pending[0].MessageID, message.NewAnswerResolution("not a permission decision"))
	if err == nil {
		t.Fatal("answer resolution must be rejected for permission")
	}
	got, ok := service.router.Get(pending[0].MessageID)
	if !ok || got.Status != message.Queued || len(got.Resolution) != 0 {
		t.Fatalf("invalid resolution changed message: ok=%v value=%+v", ok, got)
	}
	if len(harness.PermissionResponses) != 0 {
		t.Fatalf("invalid resolution called adapter: %+v", harness.PermissionResponses)
	}
}

func TestMissingDecisionIsNotInterpretedAsDenial(t *testing.T) {
	harness := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	service := newNativePermissionService(t, harness)
	runtime := &service.snapshot.Tasks[0]
	payload, _ := json.Marshal(map[string]any{"id": "missing-decision", "tool_name": "Bash"})
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{Kind: event.PermissionRequested, Payload: payload}, "worker-a")
	pending := service.router.PendingDecisions("task-a")
	if len(pending) != 1 {
		t.Fatalf("pending=%d", len(pending))
	}

	err := service.ResolveMessage(pending[0].MessageID, message.Resolution{Kind: message.ResolutionKindDecision})
	if err == nil {
		t.Fatal("missing decision must be rejected")
	}
	got, _ := service.router.Get(pending[0].MessageID)
	if got.Status != message.Queued || len(got.Resolution) != 0 {
		t.Fatalf("missing decision changed message: %+v", got)
	}
	if len(harness.PermissionResponses) != 0 {
		t.Fatalf("missing decision called adapter: %+v", harness.PermissionResponses)
	}
}

func TestQuestionDecisionResolutionLeavesMessagePending(t *testing.T) {
	service := loadResolutionValidationService(t)
	value, err := service.router.EnqueueDecision(
		"task-a", "worker-a", message.Question, message.Decision,
		json.RawMessage(`{"schema_version":"v1alpha1","question":"Q","reason":"R","current_scope":["x"],"workspace_state":"w"}`),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := service.ResolveMessage(value.MessageID, message.NewDecisionResolution(true, "ok", false)); err == nil {
		t.Fatal("question decision must be rejected")
	}
	got, _ := service.router.Get(value.MessageID)
	if got.Status != message.Queued || len(got.Resolution) != 0 {
		t.Fatalf("invalid question resolution changed message: %+v", got)
	}
}

func TestScopeAnswerResolutionLeavesMessagePending(t *testing.T) {
	service := loadResolutionValidationService(t)
	value, err := service.router.EnqueueDecision(
		"task-a", "worker-a", message.ScopeExpansionRequest, message.Scope,
		json.RawMessage(`{"requested_scope":["new.txt"],"reason":"need","consequence":"blocked","partial_modifications":"none"}`),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := service.ResolveMessage(value.MessageID, message.NewAnswerResolution("wrong type")); err == nil {
		t.Fatal("scope answer must be rejected")
	}
	got, _ := service.router.Get(value.MessageID)
	if got.Status != message.Queued || len(got.Resolution) != 0 {
		t.Fatalf("invalid scope resolution changed message: %+v", got)
	}
}

func TestScopeDenialIsAcceptedWithoutScopeMutation(t *testing.T) {
	service := loadResolutionValidationService(t)
	value, err := service.router.EnqueueDecision(
		"task-a", "worker-a", message.ScopeExpansionRequest, message.Scope,
		json.RawMessage(`{"requested_scope":["new.txt"],"reason":"need","consequence":"blocked","partial_modifications":"none"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ResolveMessage(value.MessageID, message.NewDecisionResolution(false, "not needed", false)); err != nil {
		t.Fatal(err)
	}
	got, _ := service.router.Get(value.MessageID)
	if got.Status != message.Answered {
		t.Fatalf("status=%s want answered", got.Status)
	}
	if strings.Contains(string(got.Resolution), `"kind":"answer"`) {
		t.Fatalf("scope denial stored as answer: %s", got.Resolution)
	}
}

func TestUnsupportedMessageTypeResolutionRejected(t *testing.T) {
	service := loadResolutionValidationService(t)
	value, err := service.router.EnqueueInstruction("task-a", "worker-a", "instruction", message.DeliveryImmediate)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ResolveMessage(value.MessageID, message.NewAnswerResolution("not resolvable")); err == nil {
		t.Fatal("instruction must not be resolvable")
	}
	got, _ := service.router.Get(value.MessageID)
	if got.Status != message.Queued || len(got.Resolution) != 0 {
		t.Fatalf("unsupported resolution changed message: %+v", got)
	}
}

func loadResolutionValidationService(t *testing.T) *Service {
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
	return service
}
