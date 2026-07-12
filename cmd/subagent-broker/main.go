package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/claude"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/project"
	"github.com/vnai/subagent-broker/internal/run"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
	"github.com/vnai/subagent-broker/internal/supervisor"
	"github.com/vnai/subagent-broker/internal/task"
	"github.com/vnai/subagent-broker/internal/wave"
)

func main() {
	if err := execute(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func execute(args []string) error {
	if len(args) == 0 {
		return usageError()
	}
	switch args[0] {
	case "dispatch":
		return dispatch(args[1:])
	case "supervisor":
		return runSupervisor(args[1:])
	case "status":
		return statusCommand(args[1:])
	case "events":
		return eventsCommand(args[1:])
	case "wait":
		return waitCommand(args[1:])
	case "collect":
		return collectCommand(args[1:])
	case "cancel":
		return cancelCommand(args[1:])
	case "recover":
		return recoverCommand(args[1:])
	case "doctor":
		return doctorCommand(args[1:])
	case "help", "-h", "--help":
		return usageError()
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func usageError() error {
	return fmt.Errorf("usage: subagent-broker <dispatch|status|events|wait|collect|cancel|recover|doctor>")
}

func registry(executable string) *adapter.Registry {
	result := adapter.NewRegistry()
	_ = result.Register(claude.New(executable))
	return result
}

func dispatch(args []string) error {
	flags := flag.NewFlagSet("dispatch", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root; defaults to the current directory")
	goal := flags.String("goal", "", "run goal")
	tasksFile := flags.String("tasks", "", "JSON task array or object containing a tasks array")
	harness := flags.String("harness", "claude-code", "harness name")
	executable := flags.String("executable", "", "harness executable override")
	model := flags.String("model", "", "model override")
	safeMode := flags.Bool("safe-mode", false, "disable user Claude customizations")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	permissionMode := flags.String("permission-mode", "default", "Claude permission mode")
	maxTurns := flags.Int("max-turns", 8, "maximum model turns")
	quietAfter := flags.Duration("quiet-after", 30*time.Second, "quiet progress threshold")
	stallAfter := flags.Duration("stall-after", 2*time.Minute, "suspected stall threshold")
	hardTimeout := flags.Duration("hard-timeout", 30*time.Minute, "hard run timeout")
	validationTimeout := flags.Duration("validation-timeout", 5*time.Minute, "validation command timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*goal) == "" || strings.TrimSpace(*tasksFile) == "" {
		return fmt.Errorf("dispatch requires --goal and --tasks")
	}
	if *harness != string(adapter.HarnessClaudeCode) {
		return fmt.Errorf("Phase 1 only implements harness %q", adapter.HarnessClaudeCode)
	}
	home, err := resolveBrokerHome(*brokerHome)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	projectValue, err := project.Resolve(context.Background(), *projectPath, now)
	if err != nil {
		return err
	}
	tasks, err := readTasks(*tasksFile)
	if err != nil {
		return err
	}
	if len(tasks) != 1 {
		return fmt.Errorf("Phase 1 dispatch requires exactly one task")
	}
	for i := range tasks {
		if tasks[i].ProjectRoot == "" {
			tasks[i].ProjectRoot = projectValue.CanonicalPath
		}
		if tasks[i].WaveID == "" {
			tasks[i].WaveID = "wave-1"
		}
		if tasks[i].HarnessPreference == "" {
			tasks[i].HarnessPreference = *harness
		}
		if tasks[i].Status == "" {
			tasks[i].Status = state.TaskPlanned
		}
		if err := task.ValidateContract(tasks[i]); err != nil {
			return fmt.Errorf("task %s: %w", tasks[i].TaskID, err)
		}
	}
	preflight := wave.Preflight(tasks)
	if !preflight.Allowed {
		return fmt.Errorf("preflight rejected: %s", formatIssues(preflight.Issues))
	}
	config := supervisor.Config{BrokerHome: home, Harness: *harness, Executable: *executable, Model: *model, SafeMode: *safeMode, PermissionMode: *permissionMode, MaxTurns: *maxTurns, QuietAfter: *quietAfter, StallAfter: *stallAfter, HardTimeout: *hardTimeout, ValidationTimeout: *validationTimeout}
	config.Normalize()
	runID, err := project.NewRunID(now)
	if err != nil {
		return err
	}
	taskIDs := []domain.TaskID{tasks[0].TaskID}
	runValue, err := run.New(runID, projectValue.ProjectID, *goal, taskIDs, config, now)
	if err != nil {
		return err
	}
	runValue.CurrentWave = domain.WaveID("wave-1")
	runValue.BaseRevision = gitRevision(projectValue.CanonicalPath)
	runValue.BaseWorktreeSnapshot = domain.WorktreeSnapshot{Revision: runValue.BaseRevision}
	layout, err := storage.NewLayout(home)
	if err != nil {
		return err
	}
	runDir, err := layout.EnsureRun(string(projectValue.ProjectID), string(runID))
	if err != nil {
		return err
	}
	projectPaths, err := layout.ProjectPaths(string(projectValue.ProjectID))
	if err != nil {
		return err
	}
	wavePaths, err := layout.WavePaths(string(projectValue.ProjectID), string(runID), "wave-1")
	if err != nil {
		return err
	}
	taskPaths, err := layout.TaskPaths(string(projectValue.ProjectID), string(runID), string(tasks[0].TaskID))
	if err != nil {
		return err
	}
	for _, dir := range []string{wavePaths.Root, taskPaths.Root, taskPaths.ValidationDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	waveValue := domain.Wave{WaveID: "wave-1", Ordinal: 1, TaskIDs: taskIDs, Status: domain.WavePlanned}
	if err := storage.AtomicWriteJSON(projectPaths.Project, projectValue, 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteJSON(projectPaths.ActiveRun, map[string]string{"run_id": string(runID)}, 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteJSON(filepath.Join(runDir, "run.json"), runValue, 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteJSON(filepath.Join(wavePaths.Root, "wave.json"), waveValue, 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteJSON(taskPaths.Task, tasks[0], 0o600); err != nil {
		return err
	}
	contract, err := task.RenderContract(tasks[0], runID)
	if err != nil {
		return err
	}
	if err := storage.AtomicWriteFile(taskPaths.Contract, []byte(contract), 0o600); err != nil {
		return err
	}
	service, err := supervisor.Load(runDir, registry(*executable), false)
	if err != nil {
		return err
	}
	if err := service.Initialize(); err != nil {
		return err
	}
	if err := spawnSupervisor(runDir, false); err != nil {
		return err
	}
	if err := waitForSocket(runDir, 10*time.Second); err != nil {
		return err
	}
	fmt.Printf("run_id=%s\nrun_dir=%s\nstatus=%s\n", runID, runDir, filepath.Join(runDir, "status.md"))
	return nil
}

func runSupervisor(args []string) error {
	flags := flag.NewFlagSet("supervisor", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	runDir := flags.String("run-dir", "", "run directory")
	recoverRun := flags.Bool("recover", false, "recover an interrupted run")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *runDir == "" {
		return fmt.Errorf("supervisor requires --run-dir")
	}
	executable, err := configuredExecutable(*runDir)
	if err != nil {
		return err
	}
	service, err := supervisor.Load(*runDir, registry(executable), *recoverRun)
	if err != nil {
		return err
	}
	return service.Start(context.Background())
}

func statusCommand(args []string) error {
	flags, runDir, err := resolveFlags("status", args)
	if err != nil {
		return err
	}
	_ = flags
	data, err := os.ReadFile(filepath.Join(runDir, "status.md"))
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(data)
	return err
}

func eventsCommand(args []string) error {
	flags := flag.NewFlagSet("events", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	runID := flags.String("run", "", "Run ID")
	since := flags.Uint64("since-seq", 0, "return events after this sequence")
	if err := flags.Parse(args); err != nil {
		return err
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	if err != nil {
		return err
	}
	layout, err := storage.NewLayout(mustHome(*brokerHome))
	if err != nil {
		return err
	}
	var runValue domain.Run
	data, err := os.ReadFile(filepath.Join(runDir, "run.json"))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &runValue); err != nil {
		return err
	}
	paths, err := layout.RunPaths(string(runValue.ProjectID), string(runValue.RunID))
	if err != nil {
		return err
	}
	replay, err := event.Replay(paths.Events)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	for _, item := range replay.Events {
		if item.Seq > *since {
			if err := encoder.Encode(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func waitCommand(args []string) error {
	flags := flag.NewFlagSet("wait", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	runID := flags.String("run", "", "Run ID")
	timeout := flags.Duration("timeout", 0, "timeout; zero waits without a deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	if err != nil {
		return err
	}
	deadline := time.Time{}
	if *timeout > 0 {
		deadline = time.Now().Add(*timeout)
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, readErr := readSnapshot(runDir)
		if readErr != nil {
			return readErr
		}
		if isTerminal(snapshot.Run.Status) {
			fmt.Printf("%s\n", snapshot.Run.Status)
			if snapshot.Run.Status != domain.RunCompleted {
				return fmt.Errorf("run ended with status %s", snapshot.Run.Status)
			}
			return nil
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return fmt.Errorf("wait timed out")
		}
		<-ticker.C
	}
}

func collectCommand(args []string) error {
	flags := flag.NewFlagSet("collect", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	runID := flags.String("run", "", "Run ID")
	if err := flags.Parse(args); err != nil {
		return err
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(runDir, "run-summary.md"))
	if err != nil {
		return err
	}
	fmt.Print(string(data))
	snapshot, err := readSnapshot(runDir)
	if err != nil {
		return err
	}
	for _, runtime := range snapshot.Tasks {
		if runtime.ReportPath == "" {
			continue
		}
		reportData, readErr := os.ReadFile(runtime.ReportPath)
		if readErr != nil {
			return readErr
		}
		fmt.Printf("\n--- report: %s ---\n%s", runtime.Task.TaskID, reportData)
	}
	return nil
}

func cancelCommand(args []string) error {
	flags := flag.NewFlagSet("cancel", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	runID := flags.String("run", "", "Run ID")
	if err := flags.Parse(args); err != nil {
		return err
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	if err != nil {
		return err
	}
	response, err := callSupervisor(runDir, "cancel", nil)
	if err != nil {
		return err
	}
	if !response.OK {
		return errors.New(response.Error)
	}
	fmt.Println("cancel requested")
	return nil
}

func recoverCommand(args []string) error {
	flags := flag.NewFlagSet("recover", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	runID := flags.String("run", "", "Run ID")
	if err := flags.Parse(args); err != nil {
		return err
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	if err != nil {
		return err
	}
	snapshot, err := readSnapshot(runDir)
	if err != nil {
		return err
	}
	if isTerminal(snapshot.Run.Status) {
		return fmt.Errorf("run is already terminal: %s", snapshot.Run.Status)
	}
	if err := spawnSupervisor(runDir, true); err != nil {
		return err
	}
	return waitForSocket(runDir, 10*time.Second)
}

func doctorCommand(args []string) error {
	flags := flag.NewFlagSet("doctor", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	harness := flags.String("harness", string(adapter.HarnessClaudeCode), "harness name")
	executable := flags.String("executable", "", "harness executable override")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *harness != string(adapter.HarnessClaudeCode) {
		return fmt.Errorf("Phase 1 doctor only probes harness %q", adapter.HarnessClaudeCode)
	}
	result, err := claude.New(*executable).Probe(context.Background(), adapter.ProbeRequest{Executable: *executable})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return err
	}
	if !result.Installed {
		return fmt.Errorf("Claude Code is not installed")
	}
	return nil
}

func readTasks(path string) ([]domain.Task, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var tasks []domain.Task
	if len(strings.TrimSpace(string(data))) > 0 && strings.TrimSpace(string(data))[0] == '[' {
		if err := json.Unmarshal(data, &tasks); err != nil {
			return nil, err
		}
		return tasks, nil
	}
	var wrapper struct {
		Tasks []domain.Task `json:"tasks"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return nil, err
	}
	return wrapper.Tasks, nil
}

func resolveBrokerHome(override string) (string, error) {
	if strings.TrimSpace(override) != "" {
		return filepath.Abs(override)
	}
	return storage.BrokerHome()
}

func mustHome(override string) string {
	home, err := resolveBrokerHome(override)
	if err != nil {
		return override
	}
	return home
}

func resolveFlags(name string, args []string) (*flag.FlagSet, string, error) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	runID := flags.String("run", "", "Run ID")
	if err := flags.Parse(args); err != nil {
		return flags, "", err
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	return flags, runDir, err
}

func resolveRunDir(projectPath, brokerHome, runID string) (string, error) {
	home, err := resolveBrokerHome(brokerHome)
	if err != nil {
		return "", err
	}
	if runID == "" {
		projectValue, resolveErr := project.Resolve(context.Background(), projectPath, time.Now().UTC())
		if resolveErr != nil {
			return "", resolveErr
		}
		layout, layoutErr := storage.NewLayout(home)
		if layoutErr != nil {
			return "", layoutErr
		}
		projectPaths, pathErr := layout.ProjectPaths(string(projectValue.ProjectID))
		if pathErr != nil {
			return "", pathErr
		}
		data, readErr := os.ReadFile(projectPaths.ActiveRun)
		if readErr != nil {
			return "", fmt.Errorf("no active Run: %w", readErr)
		}
		var active struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(data, &active); err != nil || active.RunID == "" {
			return "", fmt.Errorf("invalid active-run.json")
		}
		runID = active.RunID
		return layout.RunDir(string(projectValue.ProjectID), runID)
	}
	if projectPath == "" {
		projectPath = "."
	}
	projectValue, err := project.Resolve(context.Background(), projectPath, time.Now().UTC())
	if err != nil {
		return "", err
	}
	layout, err := storage.NewLayout(home)
	if err != nil {
		return "", err
	}
	return layout.RunDir(string(projectValue.ProjectID), runID)
}

func readSnapshot(runDir string) (supervisor.Snapshot, error) {
	data, err := os.ReadFile(filepath.Join(runDir, "state.json"))
	if err != nil {
		return supervisor.Snapshot{}, err
	}
	var snapshot supervisor.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return supervisor.Snapshot{}, err
	}
	return snapshot, nil
}

func spawnSupervisor(runDir string, recoverRun bool) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	logPath := filepath.Join(runDir, "control", "supervisor.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	args := []string{"supervisor", "--run-dir", runDir}
	if recoverRun {
		args = append(args, "--recover")
	}
	command := exec.Command(executable, args...)
	process.ConfigureDetachedCommand(command)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return err
	}
	_ = logFile.Close()
	return nil
}

func waitForSocket(runDir string, timeout time.Duration) error {
	socketPath := supervisor.SocketPath(runDir)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(socketPath); err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("Supervisor socket did not become ready: %s", socketPath)
}

func callSupervisor(runDir, method string, params any) (supervisor.Response, error) {
	path := supervisor.SocketPath(runDir)
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		return supervisor.Response{}, err
	}
	defer conn.Close()
	runValue, err := readRun(runDir)
	if err != nil {
		return supervisor.Response{}, err
	}
	request := supervisor.Request{SchemaVersion: supervisor.SchemaVersion, RequestID: fmt.Sprintf("request-%d", time.Now().UnixNano()), RunID: string(runValue.RunID), Method: method}
	if params != nil {
		request.Params, err = json.Marshal(params)
		if err != nil {
			return supervisor.Response{}, err
		}
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return supervisor.Response{}, err
	}
	var response supervisor.Response
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return supervisor.Response{}, err
	}
	return response, nil
}

func readRun(runDir string) (domain.Run, error) {
	data, err := os.ReadFile(filepath.Join(runDir, "run.json"))
	if err != nil {
		return domain.Run{}, err
	}
	var value domain.Run
	if err := json.Unmarshal(data, &value); err != nil {
		return domain.Run{}, err
	}
	return value, nil
}

func configuredExecutable(runDir string) (string, error) {
	runValue, err := readRun(runDir)
	if err != nil {
		return "", err
	}
	var config supervisor.Config
	if err := json.Unmarshal(runValue.ConfigSnapshot, &config); err != nil {
		return "", err
	}
	return config.Executable, nil
}

func gitRevision(root string) string {
	output, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

func formatIssues(issues []wave.Issue) string {
	parts := make([]string, 0, len(issues))
	for _, issue := range issues {
		parts = append(parts, string(issue.Kind)+": "+issue.Details)
	}
	return strings.Join(parts, "; ")
}

func isTerminal(status domain.RunStatus) bool {
	switch status {
	case domain.RunCompleted, domain.RunFailed, domain.RunCancelled, domain.RunDegraded:
		return true
	default:
		return false
	}
}
