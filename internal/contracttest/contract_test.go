package contracttest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
)

func TestFakeSteerContractImmediate(t *testing.T) {
	a := fake.New(adapter.Capabilities{
		StructuredStream: true, BidirectionalStream: true, SteerActiveTurn: true, ResumeSession: true,
	})
	session, err := a.StartSession(context.Background(), adapter.StartRequest{
		RunID: "r", TaskID: "t", WorkerID: "w", ProjectRoot: t.TempDir(), Contract: "c", Scenario: "active_steer",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Fake SteerActiveTurn returns DeliveryImmediate — models verified same-turn steer.
	result, err := a.SteerActiveTurn(context.Background(), session.NativeSessionID, "nudge")
	if err != nil {
		t.Fatal(err)
	}
	if result.Mode != adapter.DeliveryImmediate {
		t.Fatalf("fake steer must be immediate, got %s", result.Mode)
	}
	Record(Result{
		Harness: string(adapter.HarnessFake), Version: "fake-1.0.0",
		Contract: "steer_active_turn", Status: "passed",
		Evidence: "fake.SteerActiveTurn returns DeliveryImmediate during active_steer scenario",
	})
	if !SteerVerified(string(adapter.HarnessFake), "fake-1.0.0") {
		t.Fatal("registry should mark steer verified")
	}
}

func TestFakePermissionCapabilityWithoutHooksDegrades(t *testing.T) {
	declared := adapter.Capabilities{PermissionEvents: true, Hooks: true, SteerActiveTurn: true}
	set := adapter.DeriveEffective(declared, declared, adapter.SessionConfigFact{
		HooksInstalled: false,
		SteerVerified:  true,
	})
	if set.Effective.PermissionEvents {
		t.Fatal("permission_events must be false without hooks")
	}
}

func TestRealHarnessContractSkippedWithoutEnv(t *testing.T) {
	if os.Getenv("BROKER_REAL_HARNESS_TEST") == "1" {
		t.Skip("real harness path exercised by TestRealClaudeContracts when env set")
	}
	// Document skipped status for auditors.
	Record(Result{
		Harness:  os.Getenv("BROKER_TEST_HARNESS"),
		Contract: "steer_active_turn",
		Status:   "skipped",
		Reason:   "BROKER_REAL_HARNESS_TEST not set",
		Evidence: "no real harness process launched",
	})
	if os.Getenv("BROKER_TEST_HARNESS") == "" {
		// Default skip record for claude-code.
		Record(Result{
			Harness: string(adapter.HarnessClaudeCode), Contract: "steer_active_turn",
			Status: "skipped", Reason: "BROKER_REAL_HARNESS_TEST not set",
		})
		Record(Result{
			Harness: string(adapter.HarnessClaudeCode), Contract: "permission_routing",
			Status: "skipped", Reason: "BROKER_REAL_HARNESS_TEST not set",
		})
		Record(Result{
			Harness: string(adapter.HarnessClaudeCode), Contract: "resume_session",
			Status: "skipped", Reason: "BROKER_REAL_HARNESS_TEST not set",
		})
	}
}

func TestRealClaudeContracts(t *testing.T) {
	if os.Getenv("BROKER_REAL_HARNESS_TEST") != "1" {
		t.Skip("set BROKER_REAL_HARNESS_TEST=1 to run real Claude contracts")
	}
	harness := os.Getenv("BROKER_TEST_HARNESS")
	if harness == "" {
		harness = string(adapter.HarnessClaudeCode)
	}
	if harness != string(adapter.HarnessClaudeCode) {
		t.Skipf("unsupported harness %s for this contract suite", harness)
	}
	// Real tests require `claude` on PATH and authenticated environment.
	// We only attempt a probe here; full active-turn verification needs a long-running scenario.
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "README"), []byte("contract"), 0o600)
	// Mark as unverified unless a full script is provided — do not claim pass.
	Record(Result{
		Harness: harness, Contract: "steer_active_turn", Status: "unverified",
		Reason:     "real active-turn observation harness not automated in this environment; probe-only",
		Evidence:   "BROKER_REAL_HARNESS_TEST=1 set but full turn observation requires manual/extended harness harness",
		VerifiedAt: time.Now().UTC(),
	})
	t.Log("real harness contracts recorded as unverified; implement extended observation before claiming steer truth")
}
