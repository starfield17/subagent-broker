package storage

import "path/filepath"

type HomePaths struct {
	Config     string
	Projects   string
	Index      string
	ActiveRuns string
	RecentRuns string
}

type ProjectPaths struct {
	Root      string
	Project   string
	ActiveRun string
	Runs      string
}

type RunPaths struct {
	Root         string
	Run          string
	State        string
	Status       string
	Events       string
	RunSummary   string
	Supervisor   string
	Control      string
	Waves        string
	Tasks        string
	Plan         string
	Baseline     string
	Messages     string
	Verification string
}

type WavePaths struct {
	Root         string
	Wave         string
	Barrier      string
	Verification string
	Preflight    string
	Baseline     string
}

type TaskPaths struct {
	Root          string
	Task          string
	Contract      string
	ContractMeta  string
	Events        string
	Stdout        string
	Stderr        string
	Question      string
	QuestionMeta  string
	Report        string
	ReportMeta    string
	ValidationDir string
	QuestionsDir  string
}

func (l Layout) HomePaths() HomePaths {
	index := filepath.Join(l.Home, "index")
	return HomePaths{
		Config: filepath.Join(l.Home, "config.toml"), Projects: filepath.Join(l.Home, "projects"), Index: index,
		ActiveRuns: filepath.Join(index, "active-runs.json"), RecentRuns: filepath.Join(index, "recent-runs.json"),
	}
}

func (l Layout) ProjectPaths(projectKey string) (ProjectPaths, error) {
	root, err := l.ProjectDir(projectKey)
	if err != nil {
		return ProjectPaths{}, err
	}
	return ProjectPaths{Root: root, Project: filepath.Join(root, "project.json"), ActiveRun: filepath.Join(root, "active-run.json"), Runs: filepath.Join(root, "runs")}, nil
}

func (l Layout) RunPaths(projectKey, runID string) (RunPaths, error) {
	root, err := l.RunDir(projectKey, runID)
	if err != nil {
		return RunPaths{}, err
	}
	return RunPaths{
		Root: root, Run: filepath.Join(root, "run.json"), State: filepath.Join(root, "state.json"), Status: filepath.Join(root, "status.md"),
		Events: filepath.Join(root, "events.jsonl"), RunSummary: filepath.Join(root, "run-summary.md"), Supervisor: filepath.Join(root, "supervisor.json"),
		Control: filepath.Join(root, "control"), Waves: filepath.Join(root, "waves"), Tasks: filepath.Join(root, "tasks"),
		Plan: filepath.Join(root, "plan.json"), Baseline: filepath.Join(root, "baseline.json"), Messages: filepath.Join(root, "messages.jsonl"),
		Verification: filepath.Join(root, "verification.json"),
	}, nil
}

func (l Layout) WavePaths(projectKey, runID, waveID string) (WavePaths, error) {
	root, err := l.WaveDir(projectKey, runID, waveID)
	if err != nil {
		return WavePaths{}, err
	}
	return WavePaths{Root: root, Wave: filepath.Join(root, "wave.json"), Barrier: filepath.Join(root, "barrier.md"), Verification: filepath.Join(root, "verification.json"), Preflight: filepath.Join(root, "preflight.json"), Baseline: filepath.Join(root, "baseline.json")}, nil
}

func (l Layout) TaskPaths(projectKey, runID, taskID string) (TaskPaths, error) {
	root, err := l.TaskDir(projectKey, runID, taskID)
	if err != nil {
		return TaskPaths{}, err
	}
	return TaskPaths{
		Root: root, Task: filepath.Join(root, "task.json"), Contract: filepath.Join(root, "contract.md"), ContractMeta: filepath.Join(root, "contract.meta.json"),
		Events: filepath.Join(root, "events.jsonl"), Stdout: filepath.Join(root, "stdout.log"), Stderr: filepath.Join(root, "stderr.log"),
		Question: filepath.Join(root, "question.md"), QuestionMeta: filepath.Join(root, "question.meta.json"), Report: filepath.Join(root, "report.md"),
		ReportMeta: filepath.Join(root, "report.meta.json"), ValidationDir: filepath.Join(root, "validation"), QuestionsDir: filepath.Join(root, "questions"),
	}, nil
}
