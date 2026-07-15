package doctor

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/report"
)

func TestInventorySortsDescriptors(t *testing.T) {
	items := Inventory([]adapter.Descriptor{{Name: "z"}, {Name: "a"}})
	if len(items) != 2 || items[0].Harness != "a" {
		t.Fatalf("unexpected inventory: %+v", items)
	}
}

func doctorFakeRegistry(t *testing.T, scenario string, identity adapter.RuntimeIdentity) *adapter.Registry {
	t.Helper()
	harness := fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})
	if err := harness.RegisterScenario(fake.Scenario{
		Name: scenario, Events: fake.BuiltinScenarios()[scenario].Events, Final: fake.BuiltinScenarios()[scenario].Final,
		KeepOpen: fake.BuiltinScenarios()[scenario].KeepOpen, ProcessGroupToken: "doctor-test-group", RuntimeIdentity: identity,
	}); err != nil {
		t.Fatal(err)
	}
	registry := adapter.NewRegistry()
	if err := registry.Register(harness); err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestSuccessfulSmokeUsesOnlyExercisedCapabilityEvidence(t *testing.T) {
	registry := doctorFakeRegistry(t, "normal_stream", adapter.RuntimeIdentity{ObservedProvider: "fake", ObservedModel: "fixture-model"})
	home := t.TempDir()
	result, err := Run(context.Background(), registry, Config{Mode: ModeSmoke, Harnesses: []adapter.HarnessName{adapter.HarnessFake}, Scenario: "normal_stream", Model: "fixture-model", BrokerHome: home, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("smoke error: %v; result=%+v", err, result)
	}
	if len(result.Harnesses) != 1 || result.Harnesses[0].OverallStatus != "passed" {
		t.Fatalf("unexpected smoke result: %+v", result)
	}
	item := result.Harnesses[0]
	if !item.Result.Observed || !item.TaskWorker.TaskIDMatch || !item.TaskWorker.WorkerIDMatch {
		t.Fatalf("result identity evidence missing: %+v", item)
	}
	if !item.CapabilityEvidence.RuntimeVerified.StructuredStream || !item.CapabilityEvidence.RuntimeVerified.StructuredFinalOutput {
		t.Fatalf("basic smoke did not verify exercised capabilities: %+v", item.CapabilityEvidence)
	}
	if item.CapabilityEvidence.RuntimeVerified.ResumeSession {
		t.Fatal("smoke fabricated resume verification")
	}
	if item.IdentityStatus != IdentityVerified || item.RuntimeIdentity.ProviderStatus != IdentityVerified {
		t.Fatalf("identity evidence=%+v", item.RuntimeIdentity)
	}
	if item.WorkspaceStatus != "passed" || item.CleanupStatus != "passed" {
		t.Fatalf("workspace/cleanup evidence=%+v/%+v", item.Workspace, item.Cleanup)
	}
	if _, err := os.Stat(filepath.Join(home, "doctor", "runs", result.DoctorRunID, "doctor.json")); err != nil {
		t.Fatal(err)
	}
	if item.Workspace.Retained {
		t.Fatal("clean smoke workspace should be removed by default")
	}
}

func TestSmokeInvalidEnvelopeStillCleansUpAndRecordsResultBoundary(t *testing.T) {
	registry := doctorFakeRegistry(t, "invalid_result", adapter.RuntimeIdentity{})
	result, err := Run(context.Background(), registry, Config{Mode: ModeSmoke, Harnesses: []adapter.HarnessName{adapter.HarnessFake}, Scenario: "invalid_result", BrokerHome: t.TempDir(), Timeout: 2 * time.Second})
	if err == nil {
		t.Fatal("invalid envelope smoke unexpectedly passed")
	}
	item := result.Harnesses[0]
	if !item.Result.Observed || item.Result.Status != string("succeeded") {
		t.Fatalf("result boundary/selected status missing: %+v", item.Result)
	}
	if item.Stages["result_validation"].Status != "failed" || item.CleanupStatus != "passed" {
		t.Fatalf("invalid envelope evidence=%+v cleanup=%+v", item.Stages, item.Cleanup)
	}
}

type resultMismatchDoctorAdapter struct {
	adapter.Adapter
	mutate func(*report.Envelope)
}

func (a *resultMismatchDoctorAdapter) CollectFinalResult(ctx context.Context, id string) (report.Envelope, error) {
	result, err := a.Adapter.CollectFinalResult(ctx, id)
	if err == nil && a.mutate != nil {
		a.mutate(&result)
	}
	return result, err
}

func TestSmokeIdentityMismatchesFailWithoutLosingCleanupEvidence(t *testing.T) {
	resultCases := []struct {
		name   string
		mutate func(*report.Envelope)
	}{
		{name: "task", mutate: func(result *report.Envelope) { result.TaskID = "wrong-task" }},
		{name: "worker", mutate: func(result *report.Envelope) { result.WorkerID = "wrong-worker" }},
	}
	for _, testCase := range resultCases {
		t.Run("result_"+testCase.name, func(t *testing.T) {
			base := fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})
			scenario := fake.BuiltinScenarios()["normal_stream"]
			scenario.ProcessGroupToken = "doctor-test-group"
			if err := base.RegisterScenario(scenario); err != nil {
				t.Fatal(err)
			}
			registry := adapter.NewRegistry()
			if err := registry.Register(&resultMismatchDoctorAdapter{Adapter: base, mutate: testCase.mutate}); err != nil {
				t.Fatal(err)
			}
			result, err := Run(context.Background(), registry, Config{Mode: ModeSmoke, Harnesses: []adapter.HarnessName{adapter.HarnessFake}, Scenario: "normal_stream", BrokerHome: t.TempDir(), Timeout: 2 * time.Second})
			if err == nil || result.Harnesses[0].ProtocolSmokeStatus == "passed" {
				t.Fatalf("%s mismatch unexpectedly passed: %+v err=%v", testCase.name, result, err)
			}
			if result.Harnesses[0].CleanupStatus != "passed" {
				t.Fatalf("%s mismatch lost cleanup evidence: %+v", testCase.name, result.Harnesses[0].Cleanup)
			}
		})
	}

	identityCases := []struct {
		name     string
		identity adapter.RuntimeIdentity
	}{
		{name: "provider", identity: adapter.RuntimeIdentity{ObservedProvider: "xai", ObservedModel: "requested-model"}},
		{name: "model", identity: adapter.RuntimeIdentity{ObservedProvider: "openai", ObservedModel: "different-model"}},
	}
	for _, testCase := range identityCases {
		t.Run("identity_"+testCase.name, func(t *testing.T) {
			base := fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})
			scenario := fake.BuiltinScenarios()["normal_stream"]
			scenario.ProcessGroupToken = "doctor-test-group"
			scenario.RuntimeIdentity = testCase.identity
			if err := base.RegisterScenario(scenario); err != nil {
				t.Fatal(err)
			}
			registry := adapter.NewRegistry()
			if err := registry.Register(&namedDoctorAdapter{Adapter: base, descriptor: adapter.Descriptor{
				Name: adapter.HarnessCodex, AdapterVersion: "fake", RuntimeImplemented: true,
				Compatibility: "verified", Capabilities: base.Descriptor().Capabilities,
			}}); err != nil {
				t.Fatal(err)
			}
			result, err := Run(context.Background(), registry, Config{Mode: ModeSmoke, Harnesses: []adapter.HarnessName{adapter.HarnessCodex}, Scenario: "normal_stream", Model: "requested-model", BrokerHome: t.TempDir(), Timeout: 2 * time.Second})
			if err == nil || result.Harnesses[0].IdentityStatus != IdentityMismatch {
				t.Fatalf("%s identity mismatch unexpectedly passed: %+v err=%v", testCase.name, result, err)
			}
			if result.Harnesses[0].CleanupStatus != "passed" {
				t.Fatalf("%s identity mismatch lost cleanup evidence: %+v", testCase.name, result.Harnesses[0].Cleanup)
			}
		})
	}
}

type mutatingDoctorAdapter struct{ *fake.Adapter }

func (a *mutatingDoctorAdapter) StartSession(ctx context.Context, req adapter.StartRequest) (adapter.Session, error) {
	if err := os.WriteFile(filepath.Join(req.ProjectRoot, "doctor-mutated.txt"), []byte("changed\n"), 0o600); err != nil {
		return adapter.Session{}, err
	}
	return a.Adapter.StartSession(ctx, req)
}

func TestSmokeWorkspaceMutationIsNeverHiddenByNormalAuditPolicy(t *testing.T) {
	base := fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})
	scenario := fake.BuiltinScenarios()["normal_stream"]
	scenario.ProcessGroupToken = "doctor-test-group"
	_ = base.RegisterScenario(scenario)
	registry := adapter.NewRegistry()
	if err := registry.Register(&mutatingDoctorAdapter{Adapter: base}); err != nil {
		t.Fatal(err)
	}
	result, err := Run(context.Background(), registry, Config{Mode: ModeSmoke, Harnesses: []adapter.HarnessName{adapter.HarnessFake}, Scenario: "normal_stream", BrokerHome: t.TempDir(), KeepWorkspace: true, Timeout: 2 * time.Second})
	if err == nil {
		t.Fatal("workspace mutation smoke unexpectedly passed")
	}
	item := result.Harnesses[0]
	if item.WorkspaceStatus != "failed" || len(item.Workspace.ChangedPaths) != 1 || item.Workspace.ChangedPaths[0] != "doctor-mutated.txt" {
		t.Fatalf("workspace mutation evidence=%+v", item.Workspace)
	}
	if !item.Workspace.Retained || item.Artifacts.Workspace == "" {
		t.Fatal("requested mutated workspace was not retained")
	}
}

type namedDoctorAdapter struct {
	adapter.Adapter
	descriptor adapter.Descriptor
	scenario   string
}

func (a *namedDoctorAdapter) Descriptor() adapter.Descriptor { return a.descriptor }

func (a *namedDoctorAdapter) StartSession(ctx context.Context, req adapter.StartRequest) (adapter.Session, error) {
	if a.scenario != "" {
		req.Scenario = a.scenario
	}
	return a.Adapter.StartSession(ctx, req)
}

func (a *namedDoctorAdapter) RuntimeIdentity(ctx context.Context, id string) (adapter.RuntimeIdentity, error) {
	provider, ok := a.Adapter.(adapter.RuntimeIdentityProvider)
	if !ok {
		return adapter.RuntimeIdentity{}, adapter.ErrUnsupported
	}
	return provider.RuntimeIdentity(ctx, id)
}

type stubbornTree struct{}

func (stubbornTree) Inspect(_ context.Context, pid int) (process.Identity, error) {
	return process.Identity{PID: pid, StartToken: "fake-start-fake-session-1", ProcessGroupToken: "doctor-test-group"}, nil
}
func (stubbornTree) GroupMembers(_ context.Context, identity process.Identity) ([]process.Identity, error) {
	return []process.Identity{{PID: identity.PID + 1, StartToken: "child", ProcessGroupToken: identity.ProcessGroupToken}}, nil
}
func (stubbornTree) Interrupt(context.Context, process.Identity) error           { return nil }
func (stubbornTree) TerminateGracefully(context.Context, process.Identity) error { return nil }
func (stubbornTree) KillTree(context.Context, process.Identity) error            { return nil }

func TestSmokeCleanupFailureIsNotReportedAsPass(t *testing.T) {
	registry := doctorFakeRegistry(t, "normal_stream", adapter.RuntimeIdentity{})
	result, err := Run(context.Background(), registry, Config{Mode: ModeSmoke, Harnesses: []adapter.HarnessName{adapter.HarnessFake}, Scenario: "normal_stream", BrokerHome: t.TempDir(), TreeManager: stubbornTree{}, Timeout: 300 * time.Millisecond})
	if err == nil {
		t.Fatal("cleanup failure smoke unexpectedly passed")
	}
	item := result.Harnesses[0]
	if item.CleanupStatus == "passed" || !item.Cleanup.OrphanRisk || len(item.Cleanup.RemainingPIDs) == 0 {
		t.Fatalf("cleanup failure was hidden: %+v", item.Cleanup)
	}
}

func TestMultiHarnessSmokeAggregatesAndSortsAfterFailure(t *testing.T) {
	good := fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})
	goodScenario := fake.BuiltinScenarios()["normal_stream"]
	goodScenario.ProcessGroupToken = "doctor-test-group"
	_ = good.RegisterScenario(goodScenario)
	bad := fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})
	badScenario := fake.BuiltinScenarios()["invalid_result"]
	badScenario.ProcessGroupToken = "doctor-test-group"
	_ = bad.RegisterScenario(badScenario)
	registry := adapter.NewRegistry()
	if err := registry.Register(&namedDoctorAdapter{Adapter: good, descriptor: adapter.Descriptor{Name: adapter.HarnessCodex, AdapterVersion: "fake", RuntimeImplemented: true, Compatibility: "verified", Capabilities: good.Descriptor().Capabilities}}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(&namedDoctorAdapter{Adapter: bad, scenario: "invalid_result", descriptor: adapter.Descriptor{Name: adapter.HarnessGrokBuild, AdapterVersion: "fake", RuntimeImplemented: true, Compatibility: "verified", Capabilities: bad.Descriptor().Capabilities}}); err != nil {
		t.Fatal(err)
	}
	result, err := Run(context.Background(), registry, Config{Mode: ModeSmoke, Harnesses: []adapter.HarnessName{adapter.HarnessGrokBuild, adapter.HarnessCodex}, Scenario: "normal_stream", BrokerHome: t.TempDir(), Timeout: 2 * time.Second})
	if err == nil || len(result.Harnesses) != 2 {
		t.Fatalf("aggregate result=%+v err=%v", result, err)
	}
	names := []string{string(result.Harnesses[0].Harness), string(result.Harnesses[1].Harness)}
	if !sort.StringsAreSorted(names) || result.Harnesses[0].Harness != adapter.HarnessCodex {
		t.Fatalf("Harness results not deterministic: %v", names)
	}
	if result.Harnesses[1].ProtocolSmokeStatus != "failed" {
		t.Fatalf("later failed Harness was not checked: %+v", result.Harnesses[1])
	}
}

func TestSecretHygieneRedactsSentinelLogs(t *testing.T) {
	if got := redactLog("DOCTOR_SENTINEL_TOKEN=top-secret\nordinary diagnostic\n"); strings.Contains(got, "DOCTOR_SENTINEL_TOKEN") || strings.Contains(got, "top-secret") {
		t.Fatalf("sentinel leaked from stderr projection: %q", got)
	}
	if got := sanitizeText("provider token=DOCTOR_SENTINEL_TOKEN"); strings.Contains(got, "DOCTOR_SENTINEL_TOKEN") {
		t.Fatalf("sentinel leaked from text projection: %q", got)
	}
	if got := sanitizeText("authorization: Bearer DOCTOR_SENTINEL_VALUE"); strings.Contains(got, "DOCTOR_SENTINEL_VALUE") {
		t.Fatalf("bearer sentinel leaked from text projection: %q", got)
	}
}

func TestPersistedDoctorEvidenceRedactsSentinelsFromEveryProjection(t *testing.T) {
	const sentinel = "DOCTOR_SENTINEL_TOKEN"
	const secret = "doctor-secret-value"
	evidenceDir := t.TempDir()
	item := HarnessResult{
		SchemaVersion: SchemaVersion, DoctorRunID: "doctor-secret-test", Harness: adapter.HarnessFake,
		ProbeStatus: "passed", ProtocolSmokeStatus: "failed", IdentityStatus: IdentityUnavailable,
		WorkspaceStatus: "passed", CleanupStatus: "failed", OverallStatus: "failed",
		Warnings: []string{sentinel + "=" + secret}, Errors: []string{"authorization: Bearer " + secret},
		Probe:           adapter.ProbeResult{Warnings: []string{sentinel + "=" + secret}},
		Stages:          map[string]StageResult{"smoke": {Status: "failed", Error: sentinel + "=" + secret}},
		RuntimeIdentity: IdentityAssessment{Warnings: []string{sentinel + "=" + secret}},
		Cleanup:         CleanupEvidence{AdapterTerminateError: sentinel + "=" + secret, Errors: []string{sentinel + "=" + secret}},
		Artifacts: ArtifactPaths{
			HarnessDir:       filepath.Join(evidenceDir, "fake"),
			EvidenceJSON:     filepath.Join(evidenceDir, "fake", "evidence.json"),
			EvidenceMarkdown: filepath.Join(evidenceDir, "fake", "evidence.md"),
			NormalizedEvents: filepath.Join(evidenceDir, "fake", "normalized-events.jsonl"),
			Stderr:           filepath.Join(evidenceDir, "fake", "stderr.log"),
		},
		normalizedEventsLog: sentinel + "=" + secret + "\n",
		stderrLog:           sentinel + "=" + secret + "\n",
	}
	if err := PersistRun(evidenceDir, RunResult{SchemaVersion: SchemaVersion, DoctorRunID: item.DoctorRunID, Mode: ModeSmoke, EvidenceDir: evidenceDir, Harnesses: []HarnessResult{item}}); err != nil {
		t.Fatal(err)
	}
	paths := []string{
		filepath.Join(evidenceDir, "doctor.json"), filepath.Join(evidenceDir, "summary.md"),
		item.Artifacts.EvidenceJSON, item.Artifacts.EvidenceMarkdown, item.Artifacts.NormalizedEvents, item.Artifacts.Stderr,
	}
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("Doctor artifact %s mode=%o, want 600", path, info.Mode().Perm())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), sentinel) || strings.Contains(string(data), secret) {
			t.Fatalf("secret leaked into %s: %s", path, data)
		}
	}
}

func TestSmokeRejectsHistoricalResultWithoutCurrentTurnBoundary(t *testing.T) {
	harness := fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})
	scenario := fake.BuiltinScenarios()["normal_stream"]
	scenario.Name = "historical_result_only"
	scenario.Events = []adapter.NativeEvent{{Kind: event.ResultSubmitted, Timestamp: time.Unix(0, 0).UTC(), Payload: []byte(`{"turnId":"old-turn"}`)}}
	scenario.ProcessGroupToken = "doctor-test-group"
	if err := harness.RegisterScenario(scenario); err != nil {
		t.Fatal(err)
	}
	registry := adapter.NewRegistry()
	if err := registry.Register(harness); err != nil {
		t.Fatal(err)
	}
	result, err := Run(context.Background(), registry, Config{Mode: ModeSmoke, Harnesses: []adapter.HarnessName{adapter.HarnessFake}, Scenario: scenario.Name, BrokerHome: t.TempDir(), Timeout: 2 * time.Second})
	if err == nil {
		t.Fatal("historical result without current boundary unexpectedly passed")
	}
	item := result.Harnesses[0]
	if item.Result.Observed || item.ProtocolSmokeStatus == "passed" || item.Stages["result_boundary"].Status == "passed" {
		t.Fatalf("stale result was treated as current smoke success: %+v", item)
	}
	if !hasDoctorString(item.CapabilityEvidence.Contradicted, "structured_stream") || !hasDoctorString(item.CapabilityEvidence.Contradicted, "structured_final_output") {
		t.Fatalf("missing capability contradiction evidence: %+v", item.CapabilityEvidence)
	}
}

func hasDoctorString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func TestSmokeTimeoutReturnsWithCleanupEvidence(t *testing.T) {
	registry := doctorFakeRegistry(t, "long_thinking", adapter.RuntimeIdentity{})
	result, err := Run(context.Background(), registry, Config{Mode: ModeSmoke, Harnesses: []adapter.HarnessName{adapter.HarnessFake}, Scenario: "long_thinking", BrokerHome: t.TempDir(), KeepWorkspace: true, Timeout: 30 * time.Millisecond})
	if err == nil {
		t.Fatal("timeout smoke unexpectedly passed")
	}
	item := result.Harnesses[0]
	if item.Stages["timeout"].Status != "failed" || !item.Cleanup.AdapterTerminateAttempted {
		t.Fatalf("timeout evidence incomplete: %+v stages=%+v", item.Cleanup, item.Stages)
	}
}

func TestSmokeProbeOnlyDoesNotStartSession(t *testing.T) {
	registry := doctorFakeRegistry(t, "normal_stream", adapter.RuntimeIdentity{})
	result, err := Run(context.Background(), registry, Config{Mode: ModeProbe, Harnesses: []adapter.HarnessName{adapter.HarnessFake}, BrokerHome: t.TempDir()})
	if err != nil || result.Mode != ModeProbe || result.Harnesses[0].ProtocolSmokeStatus != "not_run" {
		t.Fatalf("probe-only result=%+v err=%v", result, err)
	}
	item := result.Harnesses[0]
	if item.CapabilityEvidence.RuntimeVerified.StructuredStream || !hasDoctorString(item.CapabilityEvidence.NotExercised, "structured_stream") {
		t.Fatalf("probe-only capability evidence fabricated runtime verification: %+v", item.CapabilityEvidence)
	}
}

func TestIdentityUnavailableIsNotFabricated(t *testing.T) {
	registry := doctorFakeRegistry(t, "normal_stream", adapter.RuntimeIdentity{})
	result, err := Run(context.Background(), registry, Config{Mode: ModeSmoke, Harnesses: []adapter.HarnessName{adapter.HarnessFake}, Scenario: "normal_stream", Model: "requested-model", BrokerHome: t.TempDir(), Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("identity unavailable should not fail protocol smoke: %v", err)
	}
	identity := result.Harnesses[0].RuntimeIdentity
	if identity.ProviderStatus != IdentityUnavailable || identity.ModelStatus != IdentityRequestedOnly || identity.ObservedProvider != "" || identity.ObservedModel != "" || identity.ProviderSource != adapter.EvidenceUnavailable || identity.ModelSource != adapter.EvidenceUnavailable {
		t.Fatalf("identity was fabricated: %+v", identity)
	}
}

func TestOpenCodeIdentityUsesNativeProviderWithoutFixedProviderAssumption(t *testing.T) {
	assessment := assessIdentity(adapter.HarnessOpenCode, "requested-alias", adapter.RuntimeIdentity{
		RequestedModel:   "requested-alias",
		ObservedProvider: "anthropic",
		ObservedModel:    "claude-native",
		ProviderSource:   adapter.EvidenceNativeProtocol,
		ModelSource:      adapter.EvidenceNativeProtocol,
	}, nil)
	if assessment.ProviderStatus != IdentityVerified || assessment.ModelStatus != IdentityMismatch {
		t.Fatalf("OpenCode provider/model assessment used a fixed-provider or unsafe model assumption: %+v", assessment)
	}
}
