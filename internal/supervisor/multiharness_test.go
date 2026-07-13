package supervisor

import (
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/state"
	workerpkg "github.com/vnai/subagent-broker/internal/worker"
)

func TestHarnessRoutingPrefersPersistedWorkerThenTaskThenRun(t *testing.T) {
	service := &Service{config: Config{Harness: string(adapter.HarnessClaudeCode)}}
	task := domain.Task{HarnessPreference: string(adapter.HarnessCodex)}
	if got := service.harnessNameForTask(task, nil); got != string(adapter.HarnessCodex) {
		t.Fatalf("task preference route = %q", got)
	}
	worker := &domain.WorkerSession{Harness: string(adapter.HarnessGrokBuild)}
	if got := service.harnessNameForTask(task, worker); got != string(adapter.HarnessGrokBuild) {
		t.Fatalf("persisted worker route = %q", got)
	}
	if got := service.harnessNameForTask(domain.Task{}, nil); got != string(adapter.HarnessClaudeCode) {
		t.Fatalf("run default route = %q", got)
	}
}

func TestResolveHarnessExecuteUsesPersistedWorker(t *testing.T) {
	reg := adapter.NewRegistry()
	// Register two fakes under different names via thin wrappers is hard; use
	// descriptor-backed real harness names with fake adapters re-registered is not
	// supported. Instead register one fake and assert selection string + mismatch.
	// Multi-adapter ambiguity is covered by registryAdapterCount via Descriptors.
	codexLike := fake.New(adapter.Capabilities{ResumeSession: true, BidirectionalStream: true})
	// Fake always reports HarnessFake; register under fake name.
	if err := reg.Register(codexLike); err != nil {
		t.Fatal(err)
	}
	// Second adapter: wrap by using another fake is blocked (same name).
	// Use a named stub for a second harness identity.
	stub := &namedAdapter{name: adapter.HarnessCodex, Adapter: fake.New(adapter.Capabilities{ResumeSession: true})}
	if err := reg.Register(stub); err != nil {
		t.Fatal(err)
	}

	service := &Service{
		config:   Config{Harness: string(adapter.HarnessClaudeCode)},
		registry: reg,
	}
	// Task prefers Codex, but persisted Worker says fake — Worker wins.
	runtime := &TaskState{
		Task: domain.Task{TaskID: "t1", HarnessPreference: string(adapter.HarnessCodex)},
		Worker: &domain.WorkerSession{
			Harness:         string(adapter.HarnessFake),
			NativeSessionID: "native-1",
		},
		Dimensions: state.Dimensions{Process: state.ProcessExited},
	}
	harness, name, err := service.resolveHarnessForExecution(runtime, workerpkg.AttemptRecoveryResume)
	if err != nil {
		t.Fatal(err)
	}
	if name != string(adapter.HarnessFake) {
		t.Fatalf("name=%q want fake", name)
	}
	if harness.Descriptor().Name != adapter.HarnessFake {
		t.Fatalf("adapter=%s", harness.Descriptor().Name)
	}
}

func TestResolveHarnessRejectsEmptyPersistedOnMultiAdapterResume(t *testing.T) {
	reg := adapter.NewRegistry()
	_ = reg.Register(fake.New(adapter.Capabilities{ResumeSession: true}))
	_ = reg.Register(&namedAdapter{name: adapter.HarnessCodex, Adapter: fake.New(adapter.Capabilities{ResumeSession: true})})
	service := &Service{config: Config{Harness: string(adapter.HarnessCodex)}, registry: reg}
	runtime := &TaskState{
		Task: domain.Task{TaskID: "t1", HarnessPreference: string(adapter.HarnessCodex)},
		Worker: &domain.WorkerSession{
			Harness:         "", // empty persisted
			NativeSessionID: "native-x",
		},
	}
	_, _, err := service.resolveHarnessForExecution(runtime, workerpkg.AttemptRecoveryResume)
	if err == nil {
		t.Fatal("expected rejection of empty harness with multiple adapters")
	}
}

func TestResolveHarnessMessagesRouteSameAdapter(t *testing.T) {
	reg := adapter.NewRegistry()
	_ = reg.Register(fake.New(adapter.Capabilities{BidirectionalStream: true, PermissionEvents: true}))
	_ = reg.Register(&namedAdapter{name: adapter.HarnessGrokBuild, Adapter: fake.New(adapter.Capabilities{BidirectionalStream: true})})
	service := &Service{config: Config{Harness: string(adapter.HarnessGrokBuild)}, registry: reg}
	task := domain.Task{TaskID: "mix-a", HarnessPreference: string(adapter.HarnessGrokBuild)}
	worker := &domain.WorkerSession{Harness: string(adapter.HarnessFake), NativeSessionID: "s1"}
	// Message delivery / cancel / recovery all use adapterForTask → harnessNameForTask.
	a, ok := service.adapterForTask(task, worker)
	if !ok || a.Descriptor().Name != adapter.HarnessFake {
		t.Fatalf("messages must route to persisted harness, got ok=%v name=%v", ok, a)
	}
	// Mixed-wave isolation: sibling task with different preference is independent.
	sibling := domain.Task{TaskID: "mix-b", HarnessPreference: string(adapter.HarnessGrokBuild)}
	b, ok := service.adapterForTask(sibling, nil)
	if !ok || b.Descriptor().Name != adapter.HarnessGrokBuild {
		t.Fatalf("sibling task preference = %v ok=%v", b, ok)
	}
}

// namedAdapter re-labels a fake adapter under a different harness name for routing tests.
type namedAdapter struct {
	name adapter.HarnessName
	*fake.Adapter
}

func (a *namedAdapter) Descriptor() adapter.Descriptor {
	d := a.Adapter.Descriptor()
	d.Name = a.name
	return d
}
