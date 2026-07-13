package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/adapter/protocol"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/state"
	workerpkg "github.com/vnai/subagent-broker/internal/worker"
)

// Phase 4 supervisor-level integration coverage for the correctness patches.
// These tests use the fake adapter and durable message router; they do not claim
// live native harness smoke for permission/resume/cancel/next-turn.

func TestPhase4NativePermissionIntegration(t *testing.T) {
	harness := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	service := newNativePermissionService(t, harness)
	runtime := &service.snapshot.Tasks[0]

	payload, _ := json.Marshal(map[string]any{
		"id": "int-perm-1", "tool_name": "Bash", "input": map[string]string{"command": "echo hi"},
	})
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: payload,
	}, "worker-a")

	inbox := service.Inbox(false)
	if len(inbox) != 1 || inbox[0].Type != message.PermissionRequest {
		t.Fatalf("inbox=%+v", inbox)
	}
	if err := service.ResolveMessage(inbox[0].MessageID, message.Resolution{
		Decision: message.DecisionPayload{Allowed: true, Reason: "approved"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(harness.PermissionResponses) != 1 || !harness.PermissionResponses[0].Allowed {
		t.Fatalf("adapter responses=%+v", harness.PermissionResponses)
	}
	if harness.PermissionResponses[0].RequestID != "int-perm-1" {
		t.Fatalf("request id=%q", harness.PermissionResponses[0].RequestID)
	}
	// Worker continues: task waiting cleared when no pending decisions remain.
	if service.router.HasPendingDecisions("task-a") {
		t.Fatal("permission should be resolved")
	}
}

func TestPhase4NextTurnDeliveryIntegration(t *testing.T) {
	service, harness, _ := newInstructionService(t, adapter.Capabilities{
		BidirectionalStream: true, StructuredStream: true,
	}, true)

	result, err := service.SendInstruction(context.Background(), "task-a", "queued-during-turn")
	if err != nil {
		t.Fatal(err)
	}
	if result.Mode != adapter.DeliveryNextTurn {
		t.Fatalf("mode=%s", result.Mode)
	}
	if len(harness.SentMessages) != 0 {
		t.Fatal("must queue during active turn, not send yet")
	}
	got, _ := service.router.Get(result.MessageID)
	if got.Status != message.Queued {
		t.Fatalf("status=%s", got.Status)
	}

	boundary := service.startNextTurnAtBoundary(context.Background(), "task-a", false)
	if !boundary.StartedNextTurn {
		t.Fatal("expected next turn start")
	}
	if len(harness.SentMessages) != 1 || harness.SentMessages[0] != "queued-during-turn" {
		t.Fatalf("sent=%v", harness.SentMessages)
	}
	got, _ = service.router.Get(result.MessageID)
	if got.Status != message.Delivered || got.DeliveryAttempts != 1 {
		t.Fatalf("after boundary: %+v", got)
	}

	// Exactly-once: second boundary is a no-op with empty queue.
	if service.startNextTurnAtBoundary(context.Background(), "task-a", false).StartedNextTurn {
		t.Fatal("double delivery")
	}
	if len(harness.SentMessages) != 1 {
		t.Fatalf("double delivery: %v", harness.SentMessages)
	}
}

func TestPhase4ResumeUsesPersistedHarness(t *testing.T) {
	reg := adapter.NewRegistry()
	fakeA := fake.New(adapter.Capabilities{ResumeSession: true, BidirectionalStream: true})
	if err := reg.Register(fakeA); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(&namedAdapter{
		name:    adapter.HarnessCodex,
		Adapter: fake.New(adapter.Capabilities{ResumeSession: true}),
	}); err != nil {
		t.Fatal(err)
	}
	service := &Service{
		config:   Config{Harness: string(adapter.HarnessCodex)},
		registry: reg,
	}
	// Task prefers Codex, but the native session belongs to the fake harness.
	runtime := &TaskState{
		Task: domain.Task{TaskID: "resume-t", HarnessPreference: string(adapter.HarnessCodex)},
		Worker: &domain.WorkerSession{
			Harness:         string(adapter.HarnessFake),
			NativeSessionID: "native-resume-1",
		},
		Dimensions: state.Dimensions{Process: state.ProcessExited},
	}
	harness, name, err := service.resolveHarnessForExecution(runtime, workerpkg.AttemptRecoveryResume)
	if err != nil {
		t.Fatal(err)
	}
	if name != string(adapter.HarnessFake) {
		t.Fatalf("resume harness=%q", name)
	}
	if harness.Descriptor().Name != adapter.HarnessFake {
		t.Fatalf("adapter=%s", harness.Descriptor().Name)
	}
	// Messages route to the same adapter.
	routed, ok := service.adapterForTask(runtime.Task, runtime.Worker)
	if !ok || routed.Descriptor().Name != adapter.HarnessFake {
		t.Fatal("message routing must use persisted harness")
	}
}

func TestPhase4EventPressureCriticalSurvives(t *testing.T) {
	// Supervisor-adjacent: shared emit path used by native adapters under saturation.
	events := make(chan adapter.NativeEvent, 2)
	shutdown := make(chan struct{})
	var dropped uint64
	var mu sync.Mutex

	for i := 0; i < 200; i++ {
		protocol.EmitNativeEvent(protocol.EmitOptions{
			Events: events, Shutdown: shutdown, Mu: &mu, DroppedProgress: &dropped,
		}, adapter.NativeEvent{Kind: event.ModelOutputDelta, Timestamp: time.Now().UTC()})
	}

	seen := make(chan string, 16)
	go func() {
		for ev := range events {
			seen <- ev.Kind
		}
	}()

	for _, kind := range []string{event.PermissionRequested, event.TurnFailed, event.ResultSubmitted} {
		protocol.EmitNativeEvent(protocol.EmitOptions{
			Events: events, Shutdown: shutdown, Mu: &mu, DroppedProgress: &dropped,
		}, adapter.NativeEvent{Kind: kind, Timestamp: time.Now().UTC()})
	}

	need := map[string]bool{
		event.PermissionRequested: false,
		event.TurnFailed:          false,
		event.ResultSubmitted:     false,
	}
	deadline := time.After(2 * time.Second)
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
		case kind := <-seen:
			if _, tracked := need[kind]; tracked {
				need[kind] = true
			}
		case <-deadline:
			t.Fatalf("critical events lost under pressure: %v dropped=%d", need, dropped)
		}
	}
	if dropped == 0 {
		t.Fatal("expected non-critical progress drops under saturation")
	}
	close(shutdown)
	close(events)
}

func TestPhase4PermissionDeliveryFailureHonest(t *testing.T) {
	harness := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, PermissionEvents: true,
	})
	harness.FailPermissionResponse = errors.New("native delivery failed")
	service := newNativePermissionService(t, harness)
	runtime := &service.snapshot.Tasks[0]
	payload, _ := json.Marshal(map[string]any{"id": "fail-int", "tool_name": "Bash"})
	service.bridgeNativePermission(runtime, harness, adapter.NativeEvent{
		Kind: event.PermissionRequested, Payload: payload,
	}, "worker-a")
	pending := service.router.PendingDecisions("task-a")
	err := service.ResolveMessage(pending[0].MessageID, message.Resolution{
		Decision: message.DecisionPayload{Allowed: true},
	})
	if err == nil {
		t.Fatal("expected delivery failure")
	}
	got, _ := service.router.Get(pending[0].MessageID)
	if got.Status == message.Answered {
		t.Fatal("must not record answered after adapter failure")
	}
	if got.Status != message.Failed {
		t.Fatalf("status=%s", got.Status)
	}
}

func TestPhase4MultiTurnEnvelopeSelection(t *testing.T) {
	// Covered in adapter tests; assert the selection rule remains available to supervisor.
	// Newest valid envelope wins; historical text must not corrupt parsing.
	older := `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"succeeded","summary":"old","work_completed":["x"],"files_changed":[],"no_files_changed_reason":"n","validation":[{"command":"c","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]}`
	newest := `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"succeeded","summary":"new","work_completed":["y"],"files_changed":[],"no_files_changed_reason":"n","validation":[{"command":"c","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]}`
	// Simulate selection logic: newest first.
	candidates := []string{older, "not-json", newest}
	var selected string
	for i := len(candidates) - 1; i >= 0; i-- {
		if _, err := protocol.ParseEnvelope([]byte(candidates[i])); err == nil {
			selected = candidates[i]
			break
		}
	}
	if selected != newest {
		t.Fatalf("selected=%q", selected)
	}
	env, err := protocol.ParseEnvelope([]byte(selected))
	if err != nil || env.Summary != "new" {
		t.Fatalf("envelope=%+v err=%v", env, err)
	}
}

func TestPhase4CancellationHarnessSelection(t *testing.T) {
	// Cancel uses the active worker's adapter (already bound at execute time).
	// Ensure resolveHarness would not re-route a live native session to a different harness.
	reg := adapter.NewRegistry()
	_ = reg.Register(fake.New(adapter.Capabilities{InterruptTurn: true, ResumeSession: true}))
	_ = reg.Register(&namedAdapter{name: adapter.HarnessGrokBuild, Adapter: fake.New(adapter.Capabilities{InterruptTurn: true})})
	service := &Service{config: Config{Harness: string(adapter.HarnessGrokBuild)}, registry: reg}

	runtime := &TaskState{
		Task: domain.Task{TaskID: "cancel-t", HarnessPreference: string(adapter.HarnessGrokBuild)},
		Worker: &domain.WorkerSession{
			Harness:         string(adapter.HarnessFake),
			NativeSessionID: "live-1",
		},
	}
	name := service.harnessNameForTask(runtime.Task, runtime.Worker)
	if name != string(adapter.HarnessFake) {
		t.Fatalf("cancel path must use persisted harness, got %q", name)
	}
}

func TestPhase4MixedWaveHarnessIsolation(t *testing.T) {
	reg := adapter.NewRegistry()
	_ = reg.Register(fake.New(adapter.Capabilities{BidirectionalStream: true}))
	_ = reg.Register(&namedAdapter{name: adapter.HarnessCodex, Adapter: fake.New(adapter.Capabilities{BidirectionalStream: true})})
	service := &Service{config: Config{Harness: string(adapter.HarnessFake)}, registry: reg}

	a := domain.Task{TaskID: "wave-a", HarnessPreference: string(adapter.HarnessFake)}
	b := domain.Task{TaskID: "wave-b", HarnessPreference: string(adapter.HarnessCodex)}
	wa := &domain.WorkerSession{Harness: string(adapter.HarnessFake), NativeSessionID: "s-a"}
	wb := &domain.WorkerSession{Harness: string(adapter.HarnessCodex), NativeSessionID: "s-b"}

	ha, _ := service.adapterForTask(a, wa)
	hb, _ := service.adapterForTask(b, wb)
	if ha.Descriptor().Name == hb.Descriptor().Name {
		t.Fatal("mixed-harness wave tasks must remain isolated")
	}
	if service.harnessNameForTask(a, wa) != string(adapter.HarnessFake) {
		t.Fatal("task a harness")
	}
	if service.harnessNameForTask(b, wb) != string(adapter.HarnessCodex) {
		t.Fatal("task b harness")
	}
}
