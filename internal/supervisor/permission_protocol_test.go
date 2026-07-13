package supervisor

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
)

const acpPermissionRequestNumeric = `{
  "jsonrpc": "2.0",
  "id": 5,
  "method": "session/request_permission",
  "params": {
    "sessionId": "sess-1",
    "toolCall": {"toolCallId": "call-1", "title": "Bash"},
    "options": [
      {"optionId": "native-allow-42", "name": "Allow once", "kind": "allow_once"},
      {"optionId": "native-reject-42", "name": "Reject", "kind": "reject_once"}
    ]
  }
}`

const acpPermissionRequestStringID = `{
  "jsonrpc": "2.0",
  "id": "req-str-7",
  "method": "session/request_permission",
  "params": {
    "sessionId": "sess-1",
    "toolCall": {"toolCallId": "call-2"},
    "options": [
      {"optionId": "allow-always-id", "name": "Always", "kind": "allow_always"},
      {"optionId": "reject-always-id", "name": "Reject always", "kind": "reject_always"}
    ]
  }
}`

const openCodePermissionObjectTool = `{
  "id": "perm-obj-99",
  "tool": {"messageID": "m1", "callID": "c1"},
  "permission": "edit",
  "patterns": ["src/**"]
}`

func TestParseGrokACPPermissionNumericAndOptions(t *testing.T) {
	p, ok := parseGrokACPPermission(json.RawMessage(acpPermissionRequestNumeric))
	if !ok {
		t.Fatal("parse failed")
	}
	if p.RequestID != "5" {
		t.Fatalf("id=%q", p.RequestID)
	}
	if len(p.Options) != 2 {
		t.Fatalf("options=%+v", p.Options)
	}
	if p.Options[0].OptionID != "native-allow-42" || p.Options[0].Kind != "allow_once" {
		t.Fatalf("opt0=%+v", p.Options[0])
	}
	if p.ToolName != "Bash" {
		t.Fatalf("tool=%q", p.ToolName)
	}
}

func TestParseGrokACPPermissionStringID(t *testing.T) {
	p, ok := parseGrokACPPermission(json.RawMessage(acpPermissionRequestStringID))
	if !ok || p.RequestID != "req-str-7" {
		t.Fatalf("p=%+v ok=%v", p, ok)
	}
}

func TestParseOpenCodePermissionObjectTool(t *testing.T) {
	p, ok := parseOpenCodePermission(json.RawMessage(openCodePermissionObjectTool))
	if !ok {
		t.Fatal("parse failed")
	}
	if p.RequestID != "perm-obj-99" {
		t.Fatalf("id=%q", p.RequestID)
	}
	// Object tool must not break parse; tool name is derived safely.
	if p.ToolName == "" {
		t.Fatal("expected non-empty tool name derivation")
	}
}

func TestParseOpenCodeDoesNotPickNestedUnrelatedIDs(t *testing.T) {
	// Nested tool object has callID but top-level id is the permission id.
	raw := json.RawMessage(`{"id":"top-perm","tool":{"messageID":"msg","callID":"nested-not-perm-id"}}`)
	p, ok := parseOpenCodePermission(raw)
	if !ok || p.RequestID != "top-perm" {
		t.Fatalf("p=%+v ok=%v", p, ok)
	}
}

func TestBridgeGrokPermissionPreservesOptions(t *testing.T) {
	// Use a named adapter so harness is grok-build for parsing dispatch.
	inner := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	harness := &namedAdapter{name: adapter.HarnessGrokBuild, Adapter: inner}
	service := newNativePermissionService(t, harness)
	// Override worker harness to match named adapter.
	service.snapshot.Tasks[0].Worker.Harness = string(adapter.HarnessGrokBuild)
	runtime := &service.snapshot.Tasks[0]

	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: json.RawMessage(acpPermissionRequestNumeric),
	}, "worker-a")

	pending := service.router.PendingDecisions("task-a")
	if len(pending) != 1 {
		t.Fatalf("pending=%d", len(pending))
	}
	var body message.PermissionRequestPayload
	if err := json.Unmarshal(pending[0].Payload, &body); err != nil {
		t.Fatal(err)
	}
	if body.NativePermissionID != "5" {
		t.Fatalf("id=%q", body.NativePermissionID)
	}
	if len(body.NativeOptions) != 2 {
		t.Fatalf("options=%+v", body.NativeOptions)
	}
}

func TestResolveGrokAllowSelectsAllowOnceOptionID(t *testing.T) {
	inner := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	harness := &namedAdapter{name: adapter.HarnessGrokBuild, Adapter: inner}
	service := newNativePermissionService(t, harness)
	service.snapshot.Tasks[0].Worker.Harness = string(adapter.HarnessGrokBuild)
	runtime := &service.snapshot.Tasks[0]
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: json.RawMessage(acpPermissionRequestNumeric),
	}, "worker-a")
	pending := service.router.PendingDecisions("task-a")
	if err := service.ResolveMessage(pending[0].MessageID, message.NewDecisionResolution(true, "ok", false)); err != nil {
		t.Fatal(err)
	}
	if len(inner.PermissionResponses) != 1 {
		t.Fatalf("responses=%+v", inner.PermissionResponses)
	}
	got := inner.PermissionResponses[0]
	if !got.Allowed || got.RequestID != "5" || got.OptionID != "native-allow-42" {
		t.Fatalf("decision=%+v", got)
	}
	answered, _ := service.router.Get(pending[0].MessageID)
	if answered.Status != message.Answered {
		t.Fatalf("status=%s", answered.Status)
	}
}

func TestResolveGrokDenySelectsRejectOnceOptionID(t *testing.T) {
	inner := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	harness := &namedAdapter{name: adapter.HarnessGrokBuild, Adapter: inner}
	service := newNativePermissionService(t, harness)
	service.snapshot.Tasks[0].Worker.Harness = string(adapter.HarnessGrokBuild)
	runtime := &service.snapshot.Tasks[0]
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: json.RawMessage(acpPermissionRequestNumeric),
	}, "worker-a")
	pending := service.router.PendingDecisions("task-a")
	if err := service.ResolveMessage(pending[0].MessageID, message.NewDecisionResolution(false, "no", false)); err != nil {
		t.Fatal(err)
	}
	got := inner.PermissionResponses[0]
	if got.Allowed || got.OptionID != "native-reject-42" {
		t.Fatalf("decision=%+v", got)
	}
}

func TestResolveGrokAllowFallsBackToAllowAlways(t *testing.T) {
	inner := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	harness := &namedAdapter{name: adapter.HarnessGrokBuild, Adapter: inner}
	service := newNativePermissionService(t, harness)
	service.snapshot.Tasks[0].Worker.Harness = string(adapter.HarnessGrokBuild)
	runtime := &service.snapshot.Tasks[0]
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: json.RawMessage(acpPermissionRequestStringID),
	}, "worker-a")
	pending := service.router.PendingDecisions("task-a")
	if err := service.ResolveMessage(pending[0].MessageID, message.NewDecisionResolution(true, "", false)); err != nil {
		t.Fatal(err)
	}
	got := inner.PermissionResponses[0]
	if got.OptionID != "allow-always-id" || got.RequestID != "req-str-7" {
		t.Fatalf("decision=%+v", got)
	}
}

func TestResolveGrokMissingOptionNotAnswered(t *testing.T) {
	inner := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	harness := &namedAdapter{name: adapter.HarnessGrokBuild, Adapter: inner}
	service := newNativePermissionService(t, harness)
	service.snapshot.Tasks[0].Worker.Harness = string(adapter.HarnessGrokBuild)
	runtime := &service.snapshot.Tasks[0]
	// Only reject options — allow must fail construction.
	raw := `{
	  "jsonrpc":"2.0","id":9,"method":"session/request_permission",
	  "params":{"options":[{"optionId":"r1","kind":"reject_once"}]}
	}`
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: json.RawMessage(raw),
	}, "worker-a")
	pending := service.router.PendingDecisions("task-a")
	err := service.ResolveMessage(pending[0].MessageID, message.NewDecisionResolution(true, "", false))
	if err == nil {
		t.Fatal("expected missing allow option error")
	}
	got, _ := service.router.Get(pending[0].MessageID)
	if got.Status == message.Answered {
		t.Fatal("must not record Answered when option selection fails")
	}
	// Option construction failure freezes decision and keeps pending for reconciliation.
	if got.Status != message.Queued {
		t.Fatalf("status=%s want queued", got.Status)
	}
	if len(inner.PermissionResponses) != 0 {
		t.Fatal("adapter must not be called when option selection fails")
	}
}

func TestResolveOpenCodeObjectToolPermissionID(t *testing.T) {
	inner := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	harness := &namedAdapter{name: adapter.HarnessOpenCode, Adapter: inner}
	service := newNativePermissionService(t, harness)
	service.snapshot.Tasks[0].Worker.Harness = string(adapter.HarnessOpenCode)
	runtime := &service.snapshot.Tasks[0]
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: json.RawMessage(openCodePermissionObjectTool),
	}, "worker-a")
	pending := service.router.PendingDecisions("task-a")
	if len(pending) != 1 {
		t.Fatalf("pending=%d", len(pending))
	}
	var body message.PermissionRequestPayload
	_ = json.Unmarshal(pending[0].Payload, &body)
	if body.NativePermissionID != "perm-obj-99" {
		t.Fatalf("id=%q", body.NativePermissionID)
	}
	if err := service.ResolveMessage(pending[0].MessageID, message.NewDecisionResolution(true, "", false)); err != nil {
		t.Fatal(err)
	}
	if inner.PermissionResponses[0].RequestID != "perm-obj-99" || !inner.PermissionResponses[0].Allowed {
		t.Fatalf("%+v", inner.PermissionResponses[0])
	}
}

func TestClaudeHookPermissionSkipsNativeRespond(t *testing.T) {
	// Claude harness name must not bridge native events into RespondPermission path.
	inner := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true, Hooks: true,
	})
	harness := &namedAdapter{name: adapter.HarnessClaudeCode, Adapter: inner}
	service := newNativePermissionService(t, harness)
	service.snapshot.Tasks[0].Worker.Harness = string(adapter.HarnessClaudeCode)
	runtime := &service.snapshot.Tasks[0]
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: json.RawMessage(acpPermissionRequestNumeric),
	}, "worker-a")
	if len(service.router.PendingDecisions("task-a")) != 0 {
		t.Fatal("Claude must not create native permission messages from protocol events")
	}
}

func TestAdapterDeliveryFailureNotAnswered(t *testing.T) {
	inner := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	inner.FailPermissionResponse = errors.New("wire failed")
	harness := &namedAdapter{name: adapter.HarnessGrokBuild, Adapter: inner}
	service := newNativePermissionService(t, harness)
	service.snapshot.Tasks[0].Worker.Harness = string(adapter.HarnessGrokBuild)
	service.active["task-a"] = activeWorker{
		adapter: harness, sessionID: service.active["task-a"].sessionID,
		cancel: func() {}, taskID: "task-a", workerID: "worker-a", attempt: 1,
	}
	runtime := &service.snapshot.Tasks[0]
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: json.RawMessage(acpPermissionRequestNumeric),
	}, "worker-a")
	pending := service.router.PendingDecisions("task-a")
	err := service.ResolveMessage(pending[0].MessageID, message.NewDecisionResolution(true, "", false))
	if err == nil {
		t.Fatal("expected delivery error")
	}
	got, _ := service.router.Get(pending[0].MessageID)
	if got.Status == message.Answered {
		t.Fatal("must not Answered after delivery failure")
	}
	if got.Status != message.Queued || len(got.Resolution) == 0 {
		t.Fatalf("want queued with frozen resolution: %+v", got)
	}
}
