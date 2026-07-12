package run

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
)

const SchemaVersion = "v1alpha1"

func New(id domain.RunID, projectID domain.ProjectID, goal string, taskIDs []domain.TaskID, config any, now time.Time) (domain.Run, error) {
	if strings.TrimSpace(string(id)) == "" {
		return domain.Run{}, fmt.Errorf("run id is required")
	}
	if strings.TrimSpace(string(projectID)) == "" {
		return domain.Run{}, fmt.Errorf("project id is required")
	}
	if strings.TrimSpace(goal) == "" {
		return domain.Run{}, fmt.Errorf("run goal is required")
	}
	if len(taskIDs) == 0 {
		return domain.Run{}, fmt.Errorf("run requires at least one task")
	}
	raw, err := json.Marshal(config)
	if err != nil {
		return domain.Run{}, fmt.Errorf("config snapshot: %w", err)
	}
	return domain.Run{
		RunID:          id,
		ProjectID:      projectID,
		CreatedAt:      now.UTC(),
		Goal:           goal,
		Status:         domain.RunPlanned,
		TaskIDs:        append([]domain.TaskID(nil), taskIDs...),
		ConfigSnapshot: raw,
		SchemaVersion:  SchemaVersion,
	}, nil
}

func Validate(r domain.Run) error {
	if r.SchemaVersion == "" || r.RunID == "" || r.ProjectID == "" || strings.TrimSpace(r.Goal) == "" {
		return fmt.Errorf("run is missing required identity fields")
	}
	if len(r.TaskIDs) == 0 {
		return fmt.Errorf("run has no tasks")
	}
	seen := map[domain.TaskID]bool{}
	for _, id := range r.TaskIDs {
		if id == "" {
			return fmt.Errorf("run contains an empty task id")
		}
		if seen[id] {
			return fmt.Errorf("run contains duplicate task id %q", id)
		}
		seen[id] = true
	}
	return nil
}
