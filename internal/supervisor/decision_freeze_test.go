package supervisor

import (
	"encoding/json"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/message"
)

// TestFreezeResolutionIdenticalRetry verifies identical resolution retry is idempotent.
func TestFreezeResolutionIdenticalRetry(t *testing.T) {
	runDir, _ := writeFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{})); err != nil {
		t.Fatal(err)
	}
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	// Enqueue a scope expansion request.
	val, err := service.router.EnqueueDecision("task-a", "worker-a", message.ScopeExpansionRequest, message.Scope, json.RawMessage(`{"requested_scope":["new.txt"],"reason":"need","consequence":"none","partial_modifications":"none"}`))
	if err != nil {
		t.Fatal(err)
	}
	res := message.NewDecisionResolution(true, "", false)
	resJSON, _ := json.Marshal(res)

	// First freeze.
	_, result, err := service.router.FreezeResolution(val.MessageID, resJSON)
	if err != nil || result != message.ResolutionFrozen {
		t.Fatalf("first freeze should succeed: err=%v result=%d", err, result)
	}

	// Second identical freeze.
	_, result, err = service.router.FreezeResolution(val.MessageID, resJSON)
	if err != nil {
		t.Fatalf("identical retry should succeed: %v", err)
	}
	if result != message.ResolutionAlreadyIdentical {
		t.Fatalf("identical retry should be AlreadyIdentical, got %d", result)
	}
}

// TestFreezeResolutionConflict verifies conflicting resolution is rejected.
func TestFreezeResolutionConflict(t *testing.T) {
	runDir, _ := writeFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{})); err != nil {
		t.Fatal(err)
	}
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	val, err := service.router.EnqueueDecision("task-a", "worker-a", message.Question, message.Decision, json.RawMessage(`{"schema_version":"v1alpha1","question":"Q","reason":"R","current_scope":["x"],"workspace_state":"w"}`))
	if err != nil {
		t.Fatal(err)
	}

	allow := message.NewAnswerResolution("allow")
	deny := message.NewAnswerResolution("deny")
	allowJSON, _ := json.Marshal(allow)
	denyJSON, _ := json.Marshal(deny)

	// Freeze allow.
	_, _, err = service.router.FreezeResolution(val.MessageID, allowJSON)
	if err != nil {
		t.Fatal(err)
	}

	// Conflicting deny must be rejected.
	_, result, err := service.router.FreezeResolution(val.MessageID, denyJSON)
	if err == nil {
		t.Fatal("conflicting freeze should be rejected")
	}
	if result != message.ResolutionConflict {
		t.Fatalf("conflict should be ResolutionConflict, got %d", result)
	}
}

// TestResolutionConflictRejectsLosingSideEffect verifies conflicting resolution
// is rejected at the Router level — the freeze is the durability boundary.
func TestResolutionConflictRejectsLosingSideEffect(t *testing.T) {
	runDir, _ := writeFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{})); err != nil {
		t.Fatal(err)
	}
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	val, err := service.router.EnqueueDecision("task-a", "worker-a", message.ScopeExpansionRequest, message.Scope, json.RawMessage(`{"requested_scope":["new.txt"],"reason":"need","consequence":"none","partial_modifications":"none"}`))
	if err != nil {
		t.Fatal(err)
	}

	// Freeze a deny decision.
	denyJSON, _ := json.Marshal(message.NewDecisionResolution(false, "not needed", false))
	_, result, err := service.router.FreezeResolution(val.MessageID, denyJSON)
	if err != nil || result != message.ResolutionFrozen {
		t.Fatalf("deny freeze should succeed: err=%v result=%d", err, result)
	}

	// Allow after deny freeze must conflict.
	allowJSON, _ := json.Marshal(message.NewDecisionResolution(true, "", false))
	_, result, err = service.router.FreezeResolution(val.MessageID, allowJSON)
	if err == nil {
		t.Fatal("allow after deny freeze should conflict")
	}
	if result != message.ResolutionConflict {
		t.Fatalf("should be ResolutionConflict, got %d", result)
	}
}

// TestScopeExpansionResolutionIdempotency verifies resolution semantics at the Router level —
// the scope expansion path is exercised through the decision freeze mechanism.
func TestScopeExpansionResolutionIdempotency(t *testing.T) {
	runDir, _ := writeFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{})); err != nil {
		t.Fatal(err)
	}
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	val, err := service.router.EnqueueDecision("task-a", "worker-a", message.ScopeExpansionRequest, message.Scope, json.RawMessage(`{"requested_scope":["new.txt"],"reason":"need","consequence":"none","partial_modifications":"none"}`))
	if err != nil {
		t.Fatal(err)
	}

	allowJSON, _ := json.Marshal(message.NewDecisionResolution(true, "", false))
	denyJSON, _ := json.Marshal(message.NewDecisionResolution(false, "no", false))

	// First freeze allow.
	_, result, err := service.router.FreezeResolution(val.MessageID, allowJSON)
	if err != nil || result != message.ResolutionFrozen {
		t.Fatalf("first freeze should succeed: err=%v result=%d", err, result)
	}

	// Identical allow retry must be idempotent.
	_, result, err = service.router.FreezeResolution(val.MessageID, allowJSON)
	if err != nil {
		t.Fatalf("identical allow retry should succeed: %v", err)
	}
	if result != message.ResolutionAlreadyIdentical {
		t.Fatalf("identical retry should be AlreadyIdentical, got %d", result)
	}

	// Deny after allow freeze must conflict.
	_, result, err = service.router.FreezeResolution(val.MessageID, denyJSON)
	if err == nil {
		t.Fatal("deny after allow should conflict")
	}
	if result != message.ResolutionConflict {
		t.Fatalf("should be ResolutionConflict, got %d", result)
	}

	// Transition to Answered with the allow resolution.
	answered, err := service.router.Transition(val.MessageID, message.Answered, "", allowJSON, nil)
	if err != nil {
		t.Fatal(err)
	}
	if answered.Status != message.Answered {
		t.Fatalf("expected Answered, got %s", answered.Status)
	}

	// Allow retry on Answered must be idempotent.
	_, result, err = service.router.FreezeResolution(val.MessageID, allowJSON)
	if err != nil {
		t.Fatalf("allow on Answered should be idempotent: %v", err)
	}
	if result != message.ResolutionAlreadyIdentical {
		t.Fatalf("Answered allow retry should be AlreadyIdentical, got %d", result)
	}

	// Deny on Answered must conflict.
	_, result, err = service.router.FreezeResolution(val.MessageID, denyJSON)
	if err == nil {
		t.Fatal("deny on Answered allow should conflict")
	}
	if result != message.ResolutionConflict {
		t.Fatalf("Answered deny should be ResolutionConflict, got %d", result)
	}
}

// TestNativePermissionAnsweredIdempotency verifies terminal idempotency at the Router level.
func TestNativePermissionAnsweredIdempotency(t *testing.T) {
	runDir, _ := writeFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{PermissionEvents: true})); err != nil {
		t.Fatal(err)
	}
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}

	// Create a native permission message.
	payload := message.PermissionRequestPayload{
		ToolName: "test_tool", Input: json.RawMessage(`{}`),
		Harness: "fake", NativeSessionID: "sess-1", NativePermissionID: "perm-1",
	}
	raw, _ := json.Marshal(payload)
	val, err := service.router.EnqueueDecisionWithAttempt("task-a", "worker-a", 1, message.PermissionRequest, message.Permission, raw)
	if err != nil {
		t.Fatal(err)
	}

	allowJSON, _ := json.Marshal(message.NewDecisionResolution(true, "", false))
	denyJSON, _ := json.Marshal(message.NewDecisionResolution(false, "no", false))

	// Freeze allow, then transition to Answered (simulating successful delivery).
	frozen, _, err := service.router.FreezeResolution(val.MessageID, allowJSON)
	if err != nil {
		t.Fatal(err)
	}
	answered, err := service.router.Transition(val.MessageID, message.Answered, "", frozen.Resolution, nil)
	if err != nil {
		t.Fatal(err)
	}
	if answered.Status != message.Answered {
		t.Fatalf("expected Answered, got %s", answered.Status)
	}

	// Allow retry on Answered — must be idempotent.
	_, result, err := service.router.FreezeResolution(val.MessageID, allowJSON)
	if err != nil {
		t.Fatalf("allow retry on Answered should be idempotent: %v", err)
	}
	if result != message.ResolutionAlreadyIdentical {
		t.Fatalf("Answered allow retry should be AlreadyIdentical, got %d", result)
	}

	// Deny retry on Answered — must conflict.
	_, result, err = service.router.FreezeResolution(val.MessageID, denyJSON)
	if err == nil {
		t.Fatal("deny retry on Answered allow should conflict")
	}
	if result != message.ResolutionConflict {
		t.Fatalf("should be ResolutionConflict, got %d", result)
	}

	// Freeze with intent on a fresh message, verify delivery failure freezes correctly.
	val2, _ := service.router.EnqueueDecisionWithAttempt("task-a", "worker-a", 1, message.PermissionRequest, message.Permission, raw)
	frozen2, result2, err2 := service.router.FreezeResolution(val2.MessageID, allowJSON)
	if err2 != nil || result2 != message.ResolutionFrozen {
		t.Fatalf("intent freeze should succeed: err=%v result=%d", err2, result2)
	}
	if len(frozen2.Resolution) == 0 {
		t.Fatal("resolution was not frozen")
	}

	// Conflicting intent on queued message must fail.
	_, result3, err3 := service.router.FreezeResolution(val2.MessageID, denyJSON)
	if err3 == nil {
		t.Fatal("conflicting freeze on queued with intent should fail")
	}
	if result3 != message.ResolutionConflict {
		t.Fatalf("should be ResolutionConflict, got %d", result3)
	}
}
