package wave

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/report"
)

func baseTask(id, scopePattern string) domain.Task {
	return domain.Task{
		TaskID: domain.TaskID(id), Title: id, Objective: "implement " + id,
		CompletionCriteria: []string{"tests pass"}, WriteScope: []string{scopePattern},
		ValidationCommands: []domain.ValidationCommand{{Command: "go test ./internal/...", Scope: "local"}},
		ProjectRoot:        "/repo",
	}
}

func withResponsibilities(tasks ...domain.Task) []domain.Task {
	for i := range tasks {
		tasks[i].ParallelResponsibilities = map[domain.TaskID]string{}
		for j := range tasks {
			if tasks[i].TaskID == tasks[j].TaskID {
				continue
			}
			tasks[i].ParallelResponsibilities[tasks[j].TaskID] = "owns " + string(tasks[j].TaskID)
		}
	}
	return tasks
}

func TestPreflightRejectsOverlap(t *testing.T) {
	result := Preflight(withResponsibilities(baseTask("auth", "internal/auth/**"), baseTask("token", "internal/auth/token.go")))
	if result.Allowed {
		t.Fatal("overlapping scopes must be rejected")
	}
}

func TestPreflightRejectsSameWaveDependency(t *testing.T) {
	a := baseTask("a", "internal/a/**")
	b := baseTask("b", "internal/b/**")
	b.DependsOn = []domain.TaskID{"a"}
	if Preflight(withResponsibilities(a, b)).Allowed {
		t.Fatal("same-wave output dependency must be rejected")
	}
}

func TestPreflightAllowsIndependentTasks(t *testing.T) {
	result := Preflight(withResponsibilities(baseTask("auth", "internal/auth/**"), baseTask("cache", "internal/cache/**")))
	if !result.Allowed {
		t.Fatalf("independent tasks should pass: %+v", result.Issues)
	}
}

func TestPreflightRejectsWriteReadDependency(t *testing.T) {
	writer := baseTask("api", "internal/api/**")
	reader := baseTask("tests", "tests/**")
	reader.KnownReadDependencies = []string{"internal/api/**"}
	result := Preflight(withResponsibilities(writer, reader))
	if result.Allowed {
		t.Fatal("write-read dependency must be rejected")
	}
}

func TestPreflightRejectsRepositoryWideValidation(t *testing.T) {
	task := baseTask("auth", "internal/auth/**")
	task.ValidationCommands = []domain.ValidationCommand{{Command: "go test ./...", Scope: "repository"}}
	if Preflight([]domain.Task{task}).Allowed {
		t.Fatal("repository-wide validation is unsafe during a Wave")
	}
}

func TestPreflightRejectsIncompleteContract(t *testing.T) {
	task := baseTask("auth", "internal/auth/**")
	task.ProjectRoot = ""
	task.ValidationCommands = nil
	result := Preflight([]domain.Task{task})
	if result.Allowed || len(result.Issues) == 0 {
		t.Fatalf("incomplete contract must be rejected: %+v", result)
	}
}

func TestPreflightRequiresCompleteParallelResponsibilities(t *testing.T) {
	tasks := withResponsibilities(baseTask("auth", "internal/auth/**"), baseTask("cache", "internal/cache/**"))
	if !Preflight(tasks).Allowed {
		t.Fatalf("complete responsibilities should pass: %+v", Preflight(tasks).Issues)
	}
}

func TestPreflightRejectsPlaceholderResponsibilities(t *testing.T) {
	a := baseTask("a", "internal/a/**")
	b := baseTask("b", "internal/b/**")
	a.ParallelResponsibilities = map[domain.TaskID]string{"b": "N/A"}
	b.ParallelResponsibilities = map[domain.TaskID]string{"a": "owns a"}
	if Preflight([]domain.Task{a, b}).Allowed {
		t.Fatal("placeholder N/A must fail")
	}
}

func TestPreflightRejectsMissingEmptySelfAndOutOfWaveResponsibilities(t *testing.T) {
	a := baseTask("a", "internal/a/**")
	b := baseTask("b", "internal/b/**")
	a.ParallelResponsibilities = map[domain.TaskID]string{}
	b.ParallelResponsibilities = map[domain.TaskID]string{"a": "owns a"}
	if Preflight([]domain.Task{a, b}).Allowed {
		t.Fatal("missing responsibility must fail")
	}

	a.ParallelResponsibilities = map[domain.TaskID]string{"b": "   "}
	b.ParallelResponsibilities = map[domain.TaskID]string{"a": "owns a"}
	if Preflight([]domain.Task{a, b}).Allowed {
		t.Fatal("empty responsibility must fail")
	}

	a.ParallelResponsibilities = map[domain.TaskID]string{"b": "owns b", "a": "self"}
	b.ParallelResponsibilities = map[domain.TaskID]string{"a": "owns a"}
	result := Preflight([]domain.Task{a, b})
	if result.Allowed {
		t.Fatal("self responsibility must fail")
	}

	a.ParallelResponsibilities = map[domain.TaskID]string{"b": "owns b", "outside": "nope"}
	b.ParallelResponsibilities = map[domain.TaskID]string{"a": "owns a"}
	if Preflight([]domain.Task{a, b}).Allowed {
		t.Fatal("out-of-wave responsibility must fail")
	}
}

type stubResolver struct {
	adapters map[adapter.HarnessName]adapter.Adapter
}

func (s stubResolver) Get(name adapter.HarnessName) (adapter.Adapter, bool) {
	a, ok := s.adapters[name]
	return a, ok
}

type stubAdapter struct {
	probeCount *atomic.Int32
	result     adapter.ProbeResult
	err        error
	delay      time.Duration
}

func (s *stubAdapter) Descriptor() adapter.Descriptor {
	return adapter.Descriptor{Name: "stub", RuntimeImplemented: true}
}
func (s *stubAdapter) Probe(ctx context.Context, _ adapter.ProbeRequest) (adapter.ProbeResult, error) {
	if s.probeCount != nil {
		s.probeCount.Add(1)
	}
	if s.delay > 0 {
		select {
		case <-ctx.Done():
			return adapter.ProbeResult{}, ctx.Err()
		case <-time.After(s.delay):
		}
	}
	if s.err != nil {
		return adapter.ProbeResult{}, s.err
	}
	return s.result, nil
}
func (s *stubAdapter) StartSession(context.Context, adapter.StartRequest) (adapter.Session, error) {
	return adapter.Session{}, adapter.ErrUnsupported
}
func (s *stubAdapter) ResumeSession(context.Context, adapter.ResumeRequest) (adapter.Session, error) {
	return adapter.Session{}, adapter.ErrUnsupported
}
func (s *stubAdapter) SendMessage(context.Context, string, string) (adapter.DeliveryResult, error) {
	return adapter.DeliveryResult{}, adapter.ErrUnsupported
}
func (s *stubAdapter) SteerActiveTurn(context.Context, string, string) (adapter.DeliveryResult, error) {
	return adapter.DeliveryResult{}, adapter.ErrUnsupported
}
func (s *stubAdapter) InterruptTurn(context.Context, string) error { return adapter.ErrUnsupported }
func (s *stubAdapter) TerminateSession(context.Context, string) error {
	return adapter.ErrUnsupported
}
func (s *stubAdapter) ReadHistory(context.Context, string) ([]adapter.NativeEvent, error) {
	return nil, adapter.ErrUnsupported
}
func (s *stubAdapter) RespondPermission(context.Context, string, adapter.PermissionDecision) error {
	return adapter.ErrUnsupported
}
func (s *stubAdapter) GetDiff(context.Context, string) ([]string, error) {
	return nil, adapter.ErrUnsupported
}
func (s *stubAdapter) GetUsage(context.Context, string) (adapter.Usage, error) {
	return adapter.Usage{}, adapter.ErrUnsupported
}
func (s *stubAdapter) NormalizeEvent(adapter.NativeEvent) (event.Input, error) {
	return event.Input{}, adapter.ErrUnsupported
}
func (s *stubAdapter) CollectFinalResult(context.Context, string) (report.Envelope, error) {
	return report.Envelope{}, adapter.ErrUnsupported
}

func authFalse() *bool {
	v := false
	return &v
}

func TestEvaluatePreflightAdapterNotRegistered(t *testing.T) {
	task := baseTask("a", "internal/a/**")
	task.HarnessPreference = "missing-harness"
	result := EvaluatePreflight(context.Background(), []domain.Task{task}, PreflightEnvironment{
		Registry: stubResolver{adapters: map[adapter.HarnessName]adapter.Adapter{}},
	})
	if result.Allowed {
		t.Fatal("unregistered harness must fail")
	}
	if _, ok := result.Harnesses["missing-harness"]; !ok {
		t.Fatal("expected harness entry")
	}
}

func TestEvaluatePreflightNotInstalled(t *testing.T) {
	task := baseTask("a", "internal/a/**")
	task.HarnessPreference = "fake"
	result := EvaluatePreflight(context.Background(), []domain.Task{task}, PreflightEnvironment{
		Registry: stubResolver{adapters: map[adapter.HarnessName]adapter.Adapter{
			"fake": &stubAdapter{result: adapter.ProbeResult{Installed: false, Compatibility: "verified"}},
		}},
	})
	if result.Allowed {
		t.Fatal("not installed must fail")
	}
}

func TestEvaluatePreflightNotAuthenticated(t *testing.T) {
	task := baseTask("a", "internal/a/**")
	task.HarnessPreference = "fake"
	result := EvaluatePreflight(context.Background(), []domain.Task{task}, PreflightEnvironment{
		Registry: stubResolver{adapters: map[adapter.HarnessName]adapter.Adapter{
			"fake": &stubAdapter{result: adapter.ProbeResult{Installed: true, Authenticated: authFalse(), Compatibility: "verified"}},
		}},
	})
	if result.Allowed {
		t.Fatal("not authenticated must fail")
	}
}

func TestEvaluatePreflightProbeErrorAndTimeout(t *testing.T) {
	task := baseTask("a", "internal/a/**")
	task.HarnessPreference = "fake"
	result := EvaluatePreflight(context.Background(), []domain.Task{task}, PreflightEnvironment{
		Registry: stubResolver{adapters: map[adapter.HarnessName]adapter.Adapter{
			"fake": &stubAdapter{err: errors.New("probe boom")},
		}},
	})
	if result.Allowed {
		t.Fatal("probe error must fail")
	}

	timeoutTask := baseTask("b", "internal/b/**")
	timeoutTask.HarnessPreference = "slow"
	result = EvaluatePreflight(context.Background(), []domain.Task{timeoutTask}, PreflightEnvironment{
		Registry: stubResolver{adapters: map[adapter.HarnessName]adapter.Adapter{
			"slow": &stubAdapter{delay: 200 * time.Millisecond, result: adapter.ProbeResult{Installed: true, Compatibility: "verified"}},
		}},
		ProbeTimeout: 20 * time.Millisecond,
	})
	if result.Allowed {
		t.Fatal("probe timeout must fail")
	}
}

func TestEvaluatePreflightCompatibilityUnverifiedIsWarning(t *testing.T) {
	task := baseTask("a", "internal/a/**")
	task.HarnessPreference = "fake"
	result := EvaluatePreflight(context.Background(), []domain.Task{task}, PreflightEnvironment{
		Registry: stubResolver{adapters: map[adapter.HarnessName]adapter.Adapter{
			"fake": &stubAdapter{result: adapter.ProbeResult{
				Installed: true, Compatibility: "compatibility_unverified",
				Warnings: []string{"version drift"},
			}},
		}},
	})
	if !result.Allowed {
		t.Fatalf("unverified should only warn: %+v", result.Issues)
	}
	found := false
	for _, issue := range result.Issues {
		if issue.Severity == SeverityWarning {
			found = true
		}
		if issueSeverity(issue) == SeverityError {
			t.Fatalf("unexpected error issue: %+v", issue)
		}
	}
	if !found {
		t.Fatal("expected warning issues")
	}
}

func TestEvaluatePreflightProbesUniqueHarnessOnce(t *testing.T) {
	var count atomic.Int32
	stub := &stubAdapter{
		probeCount: &count,
		result:     adapter.ProbeResult{Installed: true, Compatibility: "verified"},
	}
	a := baseTask("a", "internal/a/**")
	b := baseTask("b", "internal/b/**")
	a.HarnessPreference = "shared"
	b.HarnessPreference = "shared"
	tasks := withResponsibilities(a, b)
	result := EvaluatePreflight(context.Background(), tasks, PreflightEnvironment{
		Registry: stubResolver{adapters: map[adapter.HarnessName]adapter.Adapter{"shared": stub}},
	})
	if !result.Allowed {
		t.Fatalf("expected allowed: %+v", result.Issues)
	}
	if count.Load() != 1 {
		t.Fatalf("expected one probe, got %d", count.Load())
	}
}
