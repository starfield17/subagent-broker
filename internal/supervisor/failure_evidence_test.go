package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
)

type evidenceTestAdapter struct {
	*fake.Adapter
	once sync.Once
}

type startFailureEvidenceAdapter struct {
	*fake.Adapter
}

func (a *startFailureEvidenceAdapter) StartSession(ctx context.Context, request adapter.StartRequest) (adapter.Session, error) {
	if err := os.WriteFile(filepath.Join(request.ProjectRoot, "start-failure.txt"), []byte("observed\n"), 0o600); err != nil {
		return adapter.Session{}, err
	}
	return adapter.Session{}, errors.New("deterministic start failure")
}

func (a *evidenceTestAdapter) StartSession(ctx context.Context, request adapter.StartRequest) (adapter.Session, error) {
	a.once.Do(func() {
		_ = os.WriteFile(filepath.Join(request.ProjectRoot, "a.txt"), []byte("observed-a\n"), 0o600)
		_ = os.WriteFile(filepath.Join(request.ProjectRoot, "b.txt"), []byte("observed-b\n"), 0o600)
	})
	if request.TaskID == "task-a" {
		request.Scenario = "invalid_result"
	} else {
		request.Scenario = "normal_stream"
	}
	return a.Adapter.StartSession(ctx, request)
}

func evidenceRegistry(t *testing.T) *adapter.Registry {
	t.Helper()
	harness := &evidenceTestAdapter{Adapter: fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})}
	registry := adapter.NewRegistry()
	if err := registry.Register(harness); err != nil {
		t.Fatal(err)
	}
	return registry
}

func startFailureEvidenceRegistry(t *testing.T) *adapter.Registry {
	t.Helper()
	harness := &startFailureEvidenceAdapter{Adapter: fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})}
	registry := adapter.NewRegistry()
	if err := registry.Register(harness); err != nil {
		t.Fatal(err)
	}
	return registry
}

func setRunScenario(t *testing.T, runDir, scenario string) {
	t.Helper()
	runValue, err := readRunForTest(runDir)
	if err != nil {
		t.Fatal(err)
	}
	var config Config
	if err := json.Unmarshal(runValue.ConfigSnapshot, &config); err != nil {
		t.Fatal(err)
	}
	config.Scenario = scenario
	runValue.ConfigSnapshot, err = json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.AtomicWriteJSON(filepath.Join(runDir, "run.json"), runValue, 0o600); err != nil {
		t.Fatal(err)
	}
}

func startEvidenceService(t *testing.T, runDir string, registry *adapter.Registry) (*Service, Snapshot, storage.Layout) {
	t.Helper()
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Initialize(); err != nil {
		_ = service.Close()
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := service.Snapshot()
	layout, err := storage.NewLayout(service.config.BrokerHome)
	if err != nil {
		t.Fatal(err)
	}
	return service, snapshot, layout
}

func TestFailureEvidencePreservesWorkspaceObservationAndRunLink(t *testing.T) {
	runDir, _ := writeMultiWaveFixture(t)
	setRunScenario(t, runDir, "invalid_result")
	service, snapshot, layout := startEvidenceService(t, runDir, evidenceRegistry(t))
	if snapshot.Run.Status != domain.RunFailed || snapshot.Wave.Status != domain.WaveFailed {
		t.Fatalf("terminal status run=%s wave=%s", snapshot.Run.Status, snapshot.Wave.Status)
	}
	var failed TaskState
	for _, runtime := range snapshot.Tasks {
		if runtime.Task.TaskID == "task-a" {
			failed = runtime
		}
	}
	if failed.Task.Status != state.TaskFailed {
		t.Fatalf("task-a status=%s", failed.Task.Status)
	}
	if failed.FailureEvidencePath == "" {
		t.Fatal("TaskState did not retain FailureEvidencePath")
	}
	if _, err := os.Stat(filepath.Join(service.config.BrokerHome, "projects")); err != nil {
		// The check only guards that evidence is under Broker Home in this
		// fixture; the precise artifact paths are asserted below.
		t.Fatal(err)
	}
	paths, err := layout.TaskPaths(string(snapshot.Run.ProjectID), string(snapshot.Run.RunID), "task-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(paths.FailureEvidence); err != nil {
		t.Fatalf("failure evidence JSON missing: %v", err)
	}
	if _, err := os.Stat(paths.FailureEvidenceMarkdown); err != nil {
		t.Fatalf("failure evidence Markdown missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(failed.Task.ProjectRoot, "a.txt")); err != nil {
		t.Fatalf("residual workspace file missing: %v", err)
	}
	var evidence FailureEvidence
	raw, err := os.ReadFile(paths.FailureEvidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"result_observed": true`) {
		t.Fatalf("invalid collected result must record exact result_observed=true JSON: %s", raw)
	}
	if evidence.Workspace.BaselineSource != "wave" || !evidence.Workspace.DiffAvailable {
		t.Fatalf("unexpected baseline evidence: %+v", evidence.Workspace)
	}
	if _, ok := evidence.Workspace.After["a.txt"]; !ok {
		t.Fatalf("after state missing: %+v", evidence.Workspace)
	}
	if evidence.Workspace.ScopeAudit.Authorized[0].Path != "a.txt" || len(evidence.Workspace.ScopeAudit.Authorized) != 2 {
		t.Fatalf("ownership candidates not preserved: %+v", evidence.Workspace.ScopeAudit)
	}
	if !evidence.ResultObserved {
		t.Fatal("observed invalid result should remain visible in evidence")
	}
	if evidence.ReportPath != "" {
		t.Fatalf("failed Task unexpectedly has canonical report path: %q", evidence.ReportPath)
	}
	md, err := os.ReadFile(paths.FailureEvidenceMarkdown)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(md), "JSON SHA-256:") || !strings.Contains(string(md), "a.txt") {
		t.Fatalf("human evidence projection is incomplete:\n%s", md)
	}
	var summary RunSummary
	summaryRaw, err := os.ReadFile(filepath.Join(service.runDir, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(summaryRaw, &summary); err != nil {
		t.Fatal(err)
	}
	if len(summary.FailureEvidence) != 1 || summary.FailureEvidence[0] != paths.FailureEvidence {
		t.Fatalf("run summary did not link evidence: %+v", summary)
	}
	for _, task := range summary.Tasks {
		if task.TaskID == "task-a" && task.FailureEvidencePath != paths.FailureEvidence {
			t.Fatalf("task summary evidence path=%q", task.FailureEvidencePath)
		}
	}
	replay, err := event.Replay(filepath.Join(service.runDir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var published bool
	for _, item := range replay.Events {
		if item.Type == event.FailureEvidencePublished {
			published = true
		}
	}
	if !published {
		t.Fatal("failure_evidence.published event missing")
	}
}

func TestGenericFailureReportLeavesOwnershipUnverified(t *testing.T) {
	runDir, layout := writeFixture(t)
	setRunScenario(t, runDir, "unused")
	_, snapshot, _ := startEvidenceService(t, runDir, startFailureEvidenceRegistry(t))
	if snapshot.Run.Status != domain.RunFailed || snapshot.Tasks[0].Task.Status != state.TaskFailed {
		t.Fatalf("unexpected terminal state run=%s task=%s", snapshot.Run.Status, snapshot.Tasks[0].Task.Status)
	}
	paths, err := layout.TaskPaths(string(snapshot.Run.ProjectID), string(snapshot.Run.RunID), "task-a")
	if err != nil {
		t.Fatal(err)
	}
	reportRaw, err := os.ReadFile(paths.Report)
	if err != nil {
		t.Fatal(err)
	}
	var failed report.Envelope
	if err := json.Unmarshal(mustReadReportEnvelope(t, paths.Root), &failed); err != nil {
		t.Fatal(err)
	}
	evidenceRaw, err := os.ReadFile(paths.FailureEvidence)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(evidenceRaw), `"result_observed": false`) {
		t.Fatalf("start failure must record exact result_observed=false JSON: %s", evidenceRaw)
	}
	if len(failed.FilesChanged) != 0 {
		t.Fatalf("generic failure report claimed files: %+v", failed.FilesChanged)
	}
	if !strings.Contains(string(reportRaw), "failure-evidence.json") || !strings.Contains(string(reportRaw), "not automatically attributed") {
		t.Fatalf("generic report did not reference unowned evidence:\n%s", reportRaw)
	}
}

func mustReadReportEnvelope(t *testing.T, taskDir string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(taskDir, "report.envelope.json"))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestValidationFailurePublishesEvidenceWithoutReplacingReport(t *testing.T) {
	runDir, layout := writeFixture(t)
	taskPaths, err := layout.TaskPaths("project--abc123", "run-1", "task-a")
	if err != nil {
		t.Fatal(err)
	}
	var item domain.Task
	raw, err := os.ReadFile(taskPaths.Task)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &item); err != nil {
		t.Fatal(err)
	}
	item.ValidationCommands = []domain.ValidationCommand{{Command: "false", Scope: "local"}}
	if err := storage.AtomicWriteJSON(taskPaths.Task, item, 0o600); err != nil {
		t.Fatal(err)
	}
	_, snapshot, _ := startEvidenceService(t, runDir, fakeRegistryForRuntimeTest(t))
	var failed TaskState
	for _, runtime := range snapshot.Tasks {
		if runtime.Task.TaskID == "task-a" {
			failed = runtime
		}
	}
	if failed.Task.Status != state.TaskVerificationFailed {
		t.Fatalf("status=%s", failed.Task.Status)
	}
	if failed.ReportPath == "" || failed.FailureEvidencePath == "" {
		t.Fatalf("report/evidence paths missing: %+v", failed)
	}
	if _, err := os.Stat(failed.ReportPath); err != nil {
		t.Fatal(err)
	}
	var evidence FailureEvidence
	evidenceRaw, err := os.ReadFile(failed.FailureEvidencePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(evidenceRaw, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.FailureStage != "task_validation" || !evidence.ResultObserved || len(evidence.Validation) != 1 || evidence.Validation[0].Passed {
		t.Fatalf("validation evidence missing: %+v", evidence)
	}
	if evidence.ReportPath != failed.ReportPath {
		t.Fatalf("evidence report path=%q report=%q", evidence.ReportPath, failed.ReportPath)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(failed.ReportPath), "report.envelope.json")); err != nil {
		t.Fatalf("original worker report was not retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(failed.ReportPath), "failure-evidence.md")); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerExitTerminationProvenanceUsesObservedControlFlow(t *testing.T) {
	harness := fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})
	if err := harness.RegisterScenario(fake.Scenario{
		Name: "post_result", Events: []adapter.NativeEvent{{Kind: event.ResultSubmitted}},
		Final: validEnvelope("task-a", "worker-a", "post-result"), KeepOpen: true,
	}); err != nil {
		t.Fatal(err)
	}
	service, _ := newLifecycleService(t, harness, "task-a")
	session, err := harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-life", TaskID: "task-a", WorkerID: "worker-a", ProjectRoot: t.TempDir(), Contract: "c", Scenario: "post_result",
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := service.snapshot.Tasks[0]
	runtime.Worker.NativeSessionID = session.NativeSessionID
	result, err := service.runWorkerSession(context.Background(), &runtime, harness, session, "worker-a", process.Identity{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Resolution.TerminationRequested || result.Resolution.TerminationInitiator != "supervisor_post_result" || result.Resolution.TerminationPhase != "adapter_terminate" {
		t.Fatalf("post-result provenance=%+v", result.Resolution)
	}

	harness = fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})
	service, _ = newLifecycleService(t, harness, "task-a")
	session, err = harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-life", TaskID: "task-a", WorkerID: "worker-a", ProjectRoot: t.TempDir(), Contract: "c", Scenario: "nonzero_exit",
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime = service.snapshot.Tasks[0]
	runtime.Worker.NativeSessionID = session.NativeSessionID
	result, err = service.runWorkerSession(context.Background(), &runtime, harness, session, "worker-a", process.Identity{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Resolution.TerminationRequested || result.Resolution.TerminationInitiator != "worker_exit" || result.Resolution.TerminationPhase != "" {
		t.Fatalf("normal exit provenance=%+v", result.Resolution)
	}

	harness = fake.New(adapter.Capabilities{StructuredStream: true, StructuredFinalOutput: true})
	service, _ = newLifecycleService(t, harness, "task-a")
	session, err = harness.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-life", TaskID: "task-a", WorkerID: "worker-a", ProjectRoot: t.TempDir(), Contract: "c", Scenario: "long_thinking",
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime = service.snapshot.Tasks[0]
	runtime.Worker.NativeSessionID = session.NativeSessionID
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result, err = service.runWorkerSession(ctx, &runtime, harness, session, "worker-a", process.Identity{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Resolution.TerminationRequested || result.Resolution.TerminationInitiator != "hard_timeout" || result.Resolution.TerminationPhase != "adapter_terminate" {
		t.Fatalf("timeout provenance=%+v", result.Resolution)
	}
}
