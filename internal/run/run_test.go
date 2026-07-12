package run

import (
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
)

func TestNewStoresConfigSnapshot(t *testing.T) {
	r, err := New("run-1", "project-1", "goal", []domain.TaskID{"task-a"}, map[string]int{"concurrency": 2}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(r.ConfigSnapshot) == 0 {
		t.Fatal("effective config must be snapshotted")
	}
	if err := Validate(r); err != nil {
		t.Fatal(err)
	}
}
