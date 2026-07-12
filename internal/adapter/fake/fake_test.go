package fake

import (
	"context"
	"errors"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/report"
)

func fullCapabilities() adapter.Capabilities {
	return adapter.Capabilities{StructuredStream: true, BidirectionalStream: true, ResumeSession: true, SteerActiveTurn: true, InterruptTurn: true, StructuredFinalOutput: true, PermissionEvents: true, DiffEvents: true, UsageEvents: true, SessionHistory: true}
}

func TestBuiltinScenariosCoverManualFixtures(t *testing.T) {
	required := []string{"normal_stream", "long_thinking", "long_command_quiet", "waiting_permission", "waiting_user", "scope_request", "nonzero_exit", "stalled", "orphan_child", "partial_json", "resume", "active_steer", "invalid_result", "supervisor_crash", "pid_reuse"}
	available := BuiltinScenarios()
	for _, name := range required {
		if _, ok := available[name]; !ok {
			t.Fatalf("missing fake scenario %q", name)
		}
	}
}

func TestFakeAdapterStreamsAndCollectsResult(t *testing.T) {
	a := New(fullCapabilities())
	session, err := a.StartSession(context.Background(), adapter.StartRequest{Scenario: "normal_stream"})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for native := range session.Events {
		count++
		if _, err := a.NormalizeEvent(native); err != nil {
			t.Fatal(err)
		}
	}
	if count == 0 {
		t.Fatal("expected scripted events")
	}
	result, err := a.CollectFinalResult(context.Background(), session.NativeSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := report.ValidateEnvelope(result); err != nil {
		t.Fatalf("normal scenario should return a valid envelope: %v", err)
	}
}

func TestFakeAdapterTellsTruthAboutUnsupportedSteer(t *testing.T) {
	a := New(adapter.Capabilities{BidirectionalStream: true})
	session, err := a.StartSession(context.Background(), adapter.StartRequest{Scenario: "active_steer"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := a.SteerActiveTurn(context.Background(), session.NativeSessionID, "change course")
	if !errors.Is(err, adapter.ErrUnsupported) || result.Mode != adapter.DeliveryUnsupported {
		t.Fatalf("expected explicit unsupported mode, got result=%+v err=%v", result, err)
	}
	_ = a.TerminateSession(context.Background(), session.NativeSessionID)
}

func TestNonzeroExitScenarioExposesExitStatus(t *testing.T) {
	a := New(fullCapabilities())
	session, err := a.StartSession(context.Background(), adapter.StartRequest{Scenario: "nonzero_exit"})
	if err != nil {
		t.Fatal(err)
	}
	for range session.Events {
	}
	exit, ok := <-session.Exited
	if !ok || exit.Code != 1 {
		t.Fatalf("unexpected exit status: ok=%v status=%+v", ok, exit)
	}
}
