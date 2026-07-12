package run

import (
	"testing"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/state"
)

func TestDecodeLegacyTasksAsSingleWave(t *testing.T) {
	plan, err := DecodePlan([]byte(`[{"task_id":"a"}]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Waves) != 1 || plan.Waves[0].WaveID != "wave-1" {
		t.Fatalf("unexpected plan: %+v", plan)
	}
}

func TestValidatePlanAllowsOnlyEarlierWaveDependencies(t *testing.T) {
	task := func(id string, waveID domain.WaveID) domain.Task {
		return domain.Task{TaskID: domain.TaskID(id), Title: id, Objective: id, CompletionCriteria: []string{"done"}, WriteScope: []string{id + "/**"}, ValidationCommands: []domain.ValidationCommand{{Command: "true", Scope: "local"}}, ProjectRoot: "/tmp/project", WaveID: waveID, Status: state.TaskPlanned}
	}
	a := task("a", "wave-1")
	b := task("b", "wave-2")
	b.DependsOn = []domain.TaskID{"a"}
	plan := domain.RunPlan{SchemaVersion: SchemaVersion, Waves: []domain.WavePlan{{WaveID: "wave-1", Tasks: []domain.Task{a}}, {WaveID: "wave-2", Tasks: []domain.Task{b}}}}
	if err := ValidatePlan(plan); err != nil {
		t.Fatal(err)
	}
	a.DependsOn = []domain.TaskID{"b"}
	plan.Waves[0].Tasks[0] = a
	if err := ValidatePlan(plan); err == nil {
		t.Fatal("forward dependency should fail")
	}
}
