package run

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/wave"
)

func DecodePlan(data []byte) (domain.RunPlan, error) {
	var plan domain.RunPlan
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return plan, fmt.Errorf("task plan is empty")
	}
	if trimmed[0] == '[' {
		var tasks []domain.Task
		if err := json.Unmarshal(data, &tasks); err != nil {
			return plan, err
		}
		plan = domain.RunPlan{SchemaVersion: SchemaVersion, Waves: []domain.WavePlan{{WaveID: "wave-1", Tasks: tasks}}}
		return plan, nil
	}
	var probe struct {
		SchemaVersion string            `json:"schema_version"`
		Waves         []domain.WavePlan `json:"waves"`
		Tasks         []domain.Task     `json:"tasks"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return plan, err
	}
	if len(probe.Waves) == 0 {
		plan = domain.RunPlan{SchemaVersion: SchemaVersion, Waves: []domain.WavePlan{{WaveID: "wave-1", Tasks: probe.Tasks}}}
		return plan, nil
	}
	if err := json.Unmarshal(data, &plan); err != nil {
		return plan, err
	}
	if plan.SchemaVersion == "" {
		plan.SchemaVersion = SchemaVersion
	}
	return plan, nil
}

func ValidatePlan(plan domain.RunPlan) error {
	if plan.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported plan schema version %q", plan.SchemaVersion)
	}
	if len(plan.Waves) == 0 {
		return fmt.Errorf("run plan requires at least one Wave")
	}
	seenWaves := map[domain.WaveID]bool{}
	taskWave := map[domain.TaskID]int{}
	for ordinal, item := range plan.Waves {
		if strings.TrimSpace(string(item.WaveID)) == "" {
			return fmt.Errorf("Wave %d has no wave_id", ordinal+1)
		}
		if seenWaves[item.WaveID] {
			return fmt.Errorf("duplicate wave_id %q", item.WaveID)
		}
		seenWaves[item.WaveID] = true
		if len(item.Tasks) == 0 {
			return fmt.Errorf("Wave %q has no tasks", item.WaveID)
		}
		for _, task := range item.Tasks {
			if previous, exists := taskWave[task.TaskID]; exists {
				return fmt.Errorf("task %q appears in Waves %d and %d", task.TaskID, previous+1, ordinal+1)
			}
			taskWave[task.TaskID] = ordinal
		}
		result := wave.Preflight(item.Tasks)
		if !result.Allowed {
			return fmt.Errorf("Wave %q preflight rejected: %s", item.WaveID, formatPlanIssues(result.Issues))
		}
	}
	for ordinal, item := range plan.Waves {
		for _, task := range item.Tasks {
			for _, dependency := range task.DependsOn {
				dependencyWave, exists := taskWave[dependency]
				if !exists {
					return fmt.Errorf("task %q depends on unknown task %q", task.TaskID, dependency)
				}
				if dependencyWave >= ordinal {
					return fmt.Errorf("task %q depends on task %q outside an earlier Wave", task.TaskID, dependency)
				}
			}
		}
	}
	return nil
}

func formatPlanIssues(issues []wave.Issue) string {
	parts := make([]string, 0, len(issues))
	for _, issue := range issues {
		parts = append(parts, string(issue.Kind)+": "+issue.Details)
	}
	return strings.Join(parts, "; ")
}
