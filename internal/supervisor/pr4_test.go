package supervisor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
	"github.com/vnai/subagent-broker/internal/verify"
	"github.com/vnai/subagent-broker/internal/wave"
)

func TestAssessTaskReportMissingMeta(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "tasks", "task-a")
	_ = os.MkdirAll(taskDir, 0o700)
	service := &Service{
		paths:  storage.RunPaths{Root: root, Tasks: filepath.Join(root, "tasks")},
		config: Config{BrokerHome: root},
		snapshot: Snapshot{Tasks: []TaskState{{
			Task: domain.Task{TaskID: "task-a", Status: state.TaskVerifiedSuccess, ProjectRoot: root},
		}}},
	}
	assessment := service.assessTaskReport(service.snapshot.Tasks[0])
	if assessment.Present || assessment.Error == "" {
		t.Fatalf("%+v", assessment)
	}
}

func TestAssessTaskReportIdentityMismatch(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "tasks", "task-a")
	_ = os.MkdirAll(taskDir, 0o700)
	meta := report.Meta{SchemaVersion: report.SchemaVersion, TaskID: "task-a", WorkerID: "other-worker", Status: report.StatusSucceeded, PublishedAt: time.Now().UTC()}
	raw, _ := json.MarshalIndent(meta, "", "  ")
	_ = os.WriteFile(filepath.Join(taskDir, "report.meta.json"), append(raw, '\n'), 0o600)
	_ = os.WriteFile(filepath.Join(taskDir, "report.md"), []byte("# Task Report\n\n## Status\n\nsucceeded\n"), 0o600)
	service := &Service{
		paths: storage.RunPaths{Root: root, Tasks: filepath.Join(root, "tasks")},
		snapshot: Snapshot{Tasks: []TaskState{{
			Task:   domain.Task{TaskID: "task-a", Status: state.TaskVerifiedSuccess, ProjectRoot: root},
			Worker: &domain.WorkerSession{WorkerID: "worker-a", TaskID: "task-a"},
		}}},
	}
	assessment := service.assessTaskReport(service.snapshot.Tasks[0])
	if assessment.IdentityValid {
		t.Fatalf("expected identity mismatch: %+v", assessment)
	}
}

func TestCollectPendingDecisionsBlocksBarrier(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, _ := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	_, _ = router.EnqueueDecision("task-a", "w1", message.Question, message.Decision, json.RawMessage(`{"schema_version":"v1alpha1","question":"q","reason":"r","current_scope":["a"],"workspace_state":"ok"}`))
	service := &Service{router: router}
	pending := service.collectPendingDecisions(domain.WavePlan{WaveID: "w1", Tasks: []domain.Task{{TaskID: "task-a"}}})
	if len(pending) != 1 {
		t.Fatalf("%+v", pending)
	}
	verification := wave.EvaluateBarrier(wave.BarrierInputs{
		WaveID: "w1",
		Reports: []wave.ReportAssessment{{
			TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true,
			RuntimeStatus: state.TaskVerifiedSuccess, EnvelopeStatus: report.StatusSucceeded,
		}},
		Pending: pending,
	}, time.Now().UTC())
	if verification.Result != domain.BarrierBlocked {
		t.Fatalf("result=%s", verification.Result)
	}
}

func TestHighRiskUnauthorizedIsError(t *testing.T) {
	changes := classifyHighRiskChanges([]string{"go.mod"}, verify.ScopeAudit{Unauthorized: []string{"go.mod"}})
	if len(changes) != 1 || changes[0].Severity != wave.SeverityError {
		t.Fatalf("%+v", changes)
	}
}

func TestAcceptBarrierWarningsRequiresReasonAndHash(t *testing.T) {
	home := t.TempDir()
	layout, err := storage.NewLayout(home)
	if err != nil {
		t.Fatal(err)
	}
	projectID, runID := "proj", "run-1"
	runDir, err := layout.EnsureRun(projectID, runID)
	if err != nil {
		t.Fatal(err)
	}
	wavePaths, err := layout.WavePaths(projectID, runID, "wave-1")
	if err != nil {
		t.Fatal(err)
	}
	_ = os.MkdirAll(wavePaths.Root, 0o700)
	input := wave.BarrierInputs{WaveID: "wave-1", ExistingWarnings: []string{"w"}}
	hash := hashBarrierInputs(input)
	verification := wave.EvaluateBarrier(input, time.Now().UTC())
	verification.InputHash = hash
	if verification.Result != domain.BarrierPassedWithWarnings {
		t.Fatalf("%s", verification.Result)
	}
	raw, _ := json.Marshal(verification)
	_ = os.WriteFile(wavePaths.Verification, raw, 0o600)
	inRaw, _ := json.Marshal(input)
	_ = os.WriteFile(filepath.Join(wavePaths.Root, "barrier-input.json"), inRaw, 0o600)

	runPaths, _ := layout.RunPaths(projectID, runID)
	service := &Service{
		config: Config{BrokerHome: home},
		runDir: runDir,
		paths:  runPaths,
		snapshot: Snapshot{
			Run:   domain.Run{RunID: domain.RunID(runID), ProjectID: domain.ProjectID(projectID)},
			Waves: []domain.Wave{{WaveID: "wave-1", Status: domain.WaveWaiting, BarrierResult: domain.BarrierPassedWithWarnings}},
		},
		acceptingWork:     true,
		fatalPersistence:  make(chan error, 1),
		events:            &fakeEventAppender{},
		persistSnapshotFn: func(Snapshot) error { return nil },
	}
	if err := service.AcceptBarrierWarnings("wave-1", "agent", ""); err == nil {
		t.Fatal("empty reason should fail")
	}
	if err := service.AcceptBarrierWarnings("wave-1", "agent", "reviewed risk"); err != nil {
		t.Fatal(err)
	}
	if !service.Snapshot().Waves[0].BarrierAccepted {
		t.Fatal("expected acceptance recorded")
	}
}

func TestBuildRunSummaryAggregates(t *testing.T) {
	root := t.TempDir()
	_ = os.WriteFile(filepath.Join(root, "a.txt"), []byte("x"), 0o600)
	baseline, err := verify.CaptureWorkspace(root)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(root, "go.mod"), []byte("module x\n"), 0o600)
	service := &Service{
		config: Config{BrokerHome: filepath.Join(root, "broker")},
		snapshot: Snapshot{
			Run:   domain.Run{RunID: "run-1", Status: domain.RunCompleted, ProjectID: "p"},
			Waves: []domain.Wave{{WaveID: "wave-1", Status: domain.WaveVerified, BarrierResult: domain.BarrierPassed}},
			Tasks: []TaskState{{
				Task: domain.Task{TaskID: "task-a", WriteScope: []string{"a.txt"}, ProjectRoot: root, Status: state.TaskVerifiedSuccess},
			}},
		},
		runBaseline: baseline,
		paths:       storage.RunPaths{Root: root, RunSummary: filepath.Join(root, "run-summary.md"), Waves: filepath.Join(root, "waves"), Tasks: filepath.Join(root, "tasks")},
	}
	summary, err := service.buildRunSummary(baseline)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RunID != "run-1" || len(summary.Tasks) != 1 {
		t.Fatalf("%+v", summary)
	}
	found := false
	for _, f := range summary.ChangedFiles {
		if f == "go.mod" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected go.mod in changed files: %v", summary.ChangedFiles)
	}
}

func TestPlaceholderResponsibilityRejected(t *testing.T) {
	a := domain.Task{
		TaskID: "a", Title: "A", Objective: "o", CompletionCriteria: []string{"c"},
		WriteScope: []string{"a/**"}, ValidationCommands: []domain.ValidationCommand{{Command: "true"}},
		ProjectRoot: "/r", ParallelResponsibilities: map[domain.TaskID]string{"b": "None declared."},
	}
	b := domain.Task{
		TaskID: "b", Title: "B", Objective: "o", CompletionCriteria: []string{"c"},
		WriteScope: []string{"b/**"}, ValidationCommands: []domain.ValidationCommand{{Command: "true"}},
		ProjectRoot: "/r", ParallelResponsibilities: map[domain.TaskID]string{"a": "owns a"},
	}
	result := wave.Preflight([]domain.Task{a, b})
	if result.Allowed {
		t.Fatalf("placeholder should fail: %+v", result.Issues)
	}
}

func TestWaveAlreadyVerifiedWithAcceptedWarnings(t *testing.T) {
	service := &Service{snapshot: Snapshot{Waves: []domain.Wave{{
		WaveID: "w1", Status: domain.WaveVerified,
		BarrierResult: domain.BarrierPassedWithWarnings, BarrierAccepted: true,
	}}}}
	if !service.waveAlreadyVerified("w1") {
		t.Fatal("expected verified")
	}
}

var _ = context.Background
