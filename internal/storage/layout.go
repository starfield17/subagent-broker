package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Layout struct {
	Home string
}

func BrokerHome() (string, error) {
	if override := strings.TrimSpace(os.Getenv("BROKER_HOME")); override != "" {
		return filepath.Abs(override)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home: %w", err)
	}
	return filepath.Join(home, ".subagent-broker"), nil
}

func NewLayout(home string) (Layout, error) {
	if strings.TrimSpace(home) == "" {
		return Layout{}, fmt.Errorf("broker home is required")
	}
	abs, err := filepath.Abs(home)
	if err != nil {
		return Layout{}, err
	}
	return Layout{Home: filepath.Clean(abs)}, nil
}

func (l Layout) ProjectDir(projectKey string) (string, error) {
	if err := validateSegment(projectKey); err != nil {
		return "", err
	}
	return filepath.Join(l.Home, "projects", projectKey), nil
}

func (l Layout) RunDir(projectKey, runID string) (string, error) {
	projectDir, err := l.ProjectDir(projectKey)
	if err != nil {
		return "", err
	}
	if err := validateSegment(runID); err != nil {
		return "", err
	}
	return filepath.Join(projectDir, "runs", runID), nil
}

func (l Layout) WaveDir(projectKey, runID, waveID string) (string, error) {
	runDir, err := l.RunDir(projectKey, runID)
	if err != nil {
		return "", err
	}
	if err := validateSegment(waveID); err != nil {
		return "", err
	}
	return filepath.Join(runDir, "waves", waveID), nil
}

func (l Layout) TaskDir(projectKey, runID, taskID string) (string, error) {
	runDir, err := l.RunDir(projectKey, runID)
	if err != nil {
		return "", err
	}
	if err := validateSegment(taskID); err != nil {
		return "", err
	}
	return filepath.Join(runDir, "tasks", taskID), nil
}

func (l Layout) EnsureRun(projectKey, runID string) (string, error) {
	home := l.HomePaths()
	projectPaths, err := l.ProjectPaths(projectKey)
	if err != nil {
		return "", err
	}
	runPaths, err := l.RunPaths(projectKey, runID)
	if err != nil {
		return "", err
	}
	for _, dir := range []string{l.Home, home.Projects, home.Index, projectPaths.Root, projectPaths.Runs, runPaths.Root, runPaths.Control, runPaths.Waves, runPaths.Tasks} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", fmt.Errorf("create %s: %w", dir, err)
		}
	}
	return runPaths.Root, nil
}

func validateSegment(value string) error {
	if strings.TrimSpace(value) == "" || value == "." || value == ".." || strings.ContainsAny(value, `/\\`) {
		return fmt.Errorf("invalid layout segment %q", value)
	}
	return nil
}
