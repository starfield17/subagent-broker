package interaction

import (
	"fmt"
	"os"
	"strings"
)

// WorkerProcessIdentity is the process-level identity for MCP workers and Claude hooks.
// The Worker token is never part of this struct — it remains environment-only.
type WorkerProcessIdentity struct {
	RunDir          string
	RunID           string
	TaskID          string
	WorkerID        string
	NativeSessionID string
}

// ResolveWorkerProcessIdentity merges explicit flags with environment values.
//
// Rules:
//   - Each field may come from flag or env.
//   - If both are present they must match exactly (fail closed).
//   - Empty/missing required identity fails before any socket connect.
//   - BROKER_RUN_ID must match run.json when both are available.
func ResolveWorkerProcessIdentity(
	flagRunDir string,
	flagTaskID string,
	flagWorkerID string,
	getenv func(string) string,
) (WorkerProcessIdentity, error) {
	if getenv == nil {
		getenv = os.Getenv
	}

	runDir, err := resolveField("run directory", flagRunDir, getenv("BROKER_RUN_DIR"))
	if err != nil {
		return WorkerProcessIdentity{}, err
	}
	taskID, err := resolveField("task id", flagTaskID, getenv("BROKER_TASK_ID"))
	if err != nil {
		return WorkerProcessIdentity{}, err
	}
	workerID, err := resolveField("worker id", flagWorkerID, getenv("BROKER_WORKER_ID"))
	if err != nil {
		return WorkerProcessIdentity{}, err
	}
	if runDir == "" || taskID == "" || workerID == "" {
		return WorkerProcessIdentity{}, fmt.Errorf("worker process identity incomplete: run-dir, task, and worker are required (flags or BROKER_* env)")
	}

	envRunID := strings.TrimSpace(getenv("BROKER_RUN_ID"))
	runID := envRunID
	diskRunID, diskErr := LoadRunID(runDir)
	if diskErr != nil {
		if runID == "" {
			return WorkerProcessIdentity{}, fmt.Errorf("load run id: %w", diskErr)
		}
		// Disk unavailable but env present — keep env (unit tests may inject only env).
	} else {
		if envRunID != "" && envRunID != diskRunID {
			return WorkerProcessIdentity{}, fmt.Errorf("identity mismatch: BROKER_RUN_ID %q != run.json %q", envRunID, diskRunID)
		}
		runID = diskRunID
	}
	if runID == "" {
		return WorkerProcessIdentity{}, fmt.Errorf("run id is required (BROKER_RUN_ID or run.json)")
	}

	return WorkerProcessIdentity{
		RunDir:          runDir,
		RunID:           runID,
		TaskID:          taskID,
		WorkerID:        workerID,
		NativeSessionID: strings.TrimSpace(getenv("BROKER_NATIVE_SESSION_ID")),
	}, nil
}

func resolveField(name, flagVal, envVal string) (string, error) {
	flagVal = strings.TrimSpace(flagVal)
	envVal = strings.TrimSpace(envVal)
	switch {
	case flagVal != "" && envVal != "" && flagVal != envVal:
		return "", fmt.Errorf("identity mismatch: %s flag %q != env %q", name, flagVal, envVal)
	case flagVal != "":
		return flagVal, nil
	default:
		return envVal, nil
	}
}
