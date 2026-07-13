package supervisor

import (
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/domain"
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
