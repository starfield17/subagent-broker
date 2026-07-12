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
	"github.com/vnai/subagent-broker/internal/interaction"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/project"
	"github.com/vnai/subagent-broker/internal/run"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
	"github.com/vnai/subagent-broker/internal/supervisor"
	"github.com/vnai/subagent-broker/internal/task"
	"github.com/vnai/subagent-broker/internal/verify"
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
	case "mcp-worker":
		return runMCPWorker(args[1:])
	case "claude-hook":
		return runClaudeHook(args[1:])
	case "status":
		return statusCommand(args[1:])
	case "events":
		return eventsCommand(args[1:])
	case "wait":
		return waitCommand(args[1:])
	case "collect":
		return collectCommand(args[1:])
	case "inbox":
		return inboxCommand(args[1:])
	case "send":
		return sendCommand(args[1:])
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
	return fmt.Errorf("usage: subagent-broker <dispatch|status|events|wait|collect|inbox|send|cancel|recover|doctor>")
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
	maxConcurrency := flags.Int("max-concurrency", 4, "maximum concurrent Workers in a Wave")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*goal) == "" || strings.TrimSpace(*tasksFile) == "" {
		return fmt.Errorf("dispatch requires --goal and --tasks")
	}
	if *harness != string(adapter.HarnessClaudeCode) {
		return fmt.Errorf("Phase 3 only implements harness %q", adapter.HarnessClaudeCode)
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
	planData, err := os.ReadFile(*tasksFile)
	if err != nil {
		return err
	}
	plan, err := run.DecodePlan(planData)
	if err != nil {
		return err
	}
	var tasks []domain.Task
	for waveIndex := range plan.Waves {
		planned := &plan.Waves[waveIndex]
		for taskIndex := range planned.Tasks {
			item := &planned.Tasks[taskIndex]
			if item.ProjectRoot == "" {
				item.ProjectRoot = projectValue.CanonicalPath
			} else if filepath.Clean(item.ProjectRoot) != filepath.Clean(projectValue.CanonicalPath) {
				return fmt.Errorf("task %s project_root does not match the resolved project", item.TaskID)
			}
			if item.WaveID == "" {
				item.WaveID = planned.WaveID
			} else if item.WaveID != planned.WaveID {
				return fmt.Errorf("task %s wave_id does not match its containing Wave", item.TaskID)
			}
			if item.HarnessPreference == "" {
				item.HarnessPreference = *harness
			} else if item.HarnessPreference != *harness {
				return fmt.Errorf("task %s requests unsupported harness %q", item.TaskID, item.HarnessPreference)
			}
			if item.Status == "" {
				item.Status = state.TaskPlanned
			}
			if err := task.ValidateContract(*item); err != nil {
				return fmt.Errorf("task %s: %w", item.TaskID, err)
			}
			tasks = append(tasks, *item)
		}
	}
	if err := run.ValidatePlan(plan); err != nil {
		return err
	}
	config := supervisor.Config{BrokerHome: home, Harness: *harness, Executable: *executable, Model: *model, SafeMode: *safeMode, PermissionMode: *permissionMode, MaxTurns: *maxTurns, QuietAfter: *quietAfter, StallAfter: *stallAfter, HardTimeout: *hardTimeout, ValidationTimeout: *validationTimeout, MaxConcurrency: *maxConcurrency}
	config.Normalize()
	runID, err := project.NewRunID(now)
	if err != nil {
		return err
	}
	taskIDs := make([]domain.TaskID, 0, len(tasks))
	for _, item := range tasks {
		taskIDs = append(taskIDs, item.TaskID)
	}
	runValue, err := run.New(runID, projectValue.ProjectID, *goal, taskIDs, config, now)
	if err != nil {
		return err
	}
	runValue.CurrentWave = plan.Waves[0].WaveID
	for _, planned := range plan.Waves {
		runValue.WaveIDs = append(runValue.WaveIDs, planned.WaveID)
	}
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
	runPaths, err := layout.RunPaths(string(projectValue.ProjectID), string(runID))
	if err != nil {
		return err
	}
	baseline, err := verify.CaptureWorkspace(projectValue.CanonicalPath, home)
	if err != nil {
		return err
	}
	if err := storage.AtomicWriteJSON(projectPaths.Project, projectValue, 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteJSON(projectPaths.ActiveRun, map[string]string{"run_id": string(runID)}, 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteJSON(runPaths.Run, runValue, 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteJSON(runPaths.Plan, plan, 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteJSON(runPaths.Baseline, baseline, 0o600); err != nil {
		return err
	}
	for ordinal, planned := range plan.Waves {
		wavePaths, pathErr := layout.WavePaths(string(projectValue.ProjectID), string(runID), string(planned.WaveID))
		if pathErr != nil {
			return pathErr
		}
		if err := os.MkdirAll(wavePaths.Root, 0o700); err != nil {
			return err
		}
		ids := make([]domain.TaskID, 0, len(planned.Tasks))
		for _, item := range planned.Tasks {
			ids = append(ids, item.TaskID)
		}
		waveValue := domain.Wave{WaveID: planned.WaveID, Ordinal: ordinal + 1, TaskIDs: ids, Status: domain.WavePlanned, IntegrationChecks: planned.IntegrationChecks}
		if err := storage.AtomicWriteJSON(wavePaths.Wave, waveValue, 0o600); err != nil {
			return err
		}
		if err := storage.AtomicWriteJSON(wavePaths.Preflight, wave.Preflight(planned.Tasks), 0o600); err != nil {
			return err
		}
	}
	for _, item := range tasks {
		taskPaths, pathErr := layout.TaskPaths(string(projectValue.ProjectID), string(runID), string(item.TaskID))
		if pathErr != nil {
			return pathErr
		}
		for _, dir := range []string{taskPaths.Root, taskPaths.ValidationDir, taskPaths.QuestionsDir} {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				return err
			}
		}
		if err := storage.AtomicWriteJSON(taskPaths.Task, item, 0o600); err != nil {
			return err
		}
		contract, renderErr := task.RenderContract(item, runID)
		if renderErr != nil {
			return renderErr
		}
		if err := storage.AtomicWriteFile(taskPaths.Contract, []byte(contract), 0o600); err != nil {
			return err
		}
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

func runMCPWorker(args []string) error {
	flags := flag.NewFlagSet("mcp-worker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	runDir := flags.String("run-dir", "", "run directory")
	taskID := flags.String("task", "", "Task ID")
	workerID := flags.String("worker", "", "Worker ID")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *runDir == "" || *taskID == "" || *workerID == "" {
		return fmt.Errorf("mcp-worker requires --run-dir, --task, and --worker")
	}
	runID, err := interaction.LoadRunID(*runDir)
	if err != nil {
		return err
	}
	return (interaction.WorkerServer{RunDir: *runDir, RunID: runID, TaskID: *taskID, WorkerID: *workerID}).Run(context.Background())
}

func runClaudeHook(args []string) error {
	flags := flag.NewFlagSet("claude-hook", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	runDir := flags.String("run-dir", "", "run directory")
	taskID := flags.String("task", "", "Task ID")
	workerID := flags.String("worker", "", "Worker ID")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *runDir == "" || *taskID == "" || *workerID == "" {
		return fmt.Errorf("claude-hook requires --run-dir, --task, and --worker")
	}
	runID, err := interaction.LoadRunID(*runDir)
	if err != nil {
		return err
	}
	return interaction.RunPermissionHook(context.Background(), *runDir, runID, *taskID, *workerID, os.Stdin, os.Stdout)
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
	waitFor := flags.String("for", "run", "condition: run, wave, task, inbox, or event")
	taskID := flags.String("task", "", "Task ID for --for task")
	waveID := flags.String("wave", "", "Wave ID for --for wave")
	sinceSeq := flags.Uint64("since-seq", 0, "event cursor for --for event")
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
		matched, matchErr := waitCondition(runDir, snapshot, *waitFor, *taskID, *waveID, *sinceSeq)
		if matchErr != nil {
			return matchErr
		}
		if matched {
			if *waitFor != "run" {
				fmt.Println(*waitFor)
				return nil
			}
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

func waitCondition(runDir string, snapshot supervisor.Snapshot, condition, taskID, waveID string, sinceSeq uint64) (bool, error) {
	switch condition {
	case "run":
		return isTerminal(snapshot.Run.Status), nil
	case "wave":
		if waveID == "" {
			waveID = string(snapshot.Run.CurrentWave)
		}
		for _, item := range snapshot.Waves {
			if string(item.WaveID) == waveID {
				return item.Status == domain.WaveVerified || item.Status == domain.WaveBlocked || item.Status == domain.WaveFailed || item.Status == domain.WaveCancelled, nil
			}
		}
		return false, fmt.Errorf("Wave %q was not found", waveID)
	case "task":
		if taskID == "" {
			return false, fmt.Errorf("--for task requires --task")
		}
		for _, runtime := range snapshot.Tasks {
			if string(runtime.Task.TaskID) == taskID {
				switch runtime.Task.Status {
				case state.TaskVerifiedSuccess, state.TaskVerifiedPartial, state.TaskVerificationFailed, state.TaskFailed, state.TaskCancelled:
					return true, nil
				}
				return false, nil
			}
		}
		return false, fmt.Errorf("task %q was not found", taskID)
	case "inbox":
		index, err := message.Replay(filepath.Join(runDir, "messages.jsonl"))
		if err != nil {
			return false, err
		}
		return len(message.Sorted(index, false)) > 0, nil
	case "event":
		replayed, err := event.Replay(filepath.Join(runDir, "events.jsonl"))
		if err != nil {
			return false, err
		}
		for _, item := range replayed.Events {
			if item.Seq > sinceSeq {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("unsupported wait condition %q", condition)
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
	runValue, err := readRun(runDir)
	if err != nil {
		return err
	}
	layout, err := storage.NewLayout(mustHome(*brokerHome))
	if err != nil {
		return err
	}
	runPaths, err := layout.RunPaths(string(runValue.ProjectID), string(runValue.RunID))
	if err != nil {
		return err
	}
	index, err := message.Replay(runPaths.Messages)
	if err != nil {
		return err
	}
	for _, item := range message.Sorted(index, false) {
		questionPath := filepath.Join(runPaths.Tasks, item.TaskID, "questions", item.MessageID, "question.md")
		question, readErr := os.ReadFile(questionPath)
		if readErr == nil {
			fmt.Printf("\n--- pending: %s ---\n%s", item.MessageID, question)
		}
	}
	return nil
}

func inboxCommand(args []string) error {
	flags := flag.NewFlagSet("inbox", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	runID := flags.String("run", "", "Run ID")
	taskID := flags.String("task", "", "Task ID filter")
	includeResolved := flags.Bool("all", false, "include resolved messages")
	jsonOutput := flags.Bool("json", false, "print JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	if err != nil {
		return err
	}
	runValue, err := readRun(runDir)
	if err != nil {
		return err
	}
	paths, err := storage.NewLayout(mustHome(*brokerHome))
	if err != nil {
		return err
	}
	runPaths, err := paths.RunPaths(string(runValue.ProjectID), string(runValue.RunID))
	if err != nil {
		return err
	}
	index, err := message.Replay(runPaths.Messages)
	if err != nil {
		return err
	}
	items := message.Sorted(index, *includeResolved)
	filtered := items[:0]
	for _, item := range items {
		if *taskID == "" || item.TaskID == *taskID {
			filtered = append(filtered, item)
		}
	}
	if *jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(filtered)
	}
	if len(filtered) == 0 {
		fmt.Println("Inbox is empty.")
		return nil
	}
	for _, item := range filtered {
		fmt.Printf("%s / task=%s / type=%s / category=%s / status=%s\n", item.MessageID, item.TaskID, item.Type, item.Category, item.Status)
	}
	return nil
}

func sendCommand(args []string) error {
	flags := flag.NewFlagSet("send", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	runID := flags.String("run", "", "Run ID")
	taskID := flags.String("task", "", "Task ID")
	messageID := flags.String("message", "", "Message ID")
	textValue := flags.String("text", "", "instruction text")
	answer := flags.String("answer", "", "question answer")
	approve := flags.Bool("approve", false, "approve a scope or permission request")
	deny := flags.Bool("deny", false, "deny a scope or permission request")
	reason := flags.String("reason", "", "decision reason")
	allowInterface := flags.Bool("allow-public-interface-change", false, "approve a requested public-interface change")
	if err := flags.Parse(args); err != nil {
		return err
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	if err != nil {
		return err
	}
	if *taskID != "" && *textValue != "" && *messageID == "" {
		response, err := callSupervisor(runDir, "send", map[string]any{"task_id": *taskID, "text": *textValue})
		if err != nil {
			return err
		}
		if !response.OK {
			return errors.New(response.Error)
		}
		data, _ := json.Marshal(response.Result)
		fmt.Println(string(data))
		return nil
	}
	if *messageID == "" {
		return fmt.Errorf("send requires either --task with --text or --message with a resolution")
	}
	actions := 0
	if *answer != "" {
		actions++
	}
	if *approve {
		actions++
	}
	if *deny {
		actions++
	}
	if actions != 1 {
		return fmt.Errorf("message resolution requires exactly one of --answer, --approve, or --deny")
	}
	resolution := message.Resolution{Answer: *answer}
	if *approve || *deny {
		resolution.Decision = message.DecisionPayload{Allowed: *approve, Reason: *reason, AllowPublicInterfaceChange: *allowInterface}
		if *deny && strings.TrimSpace(*reason) == "" {
			return fmt.Errorf("--deny requires --reason")
		}
	}
	response, err := callSupervisor(runDir, "resolve_message", map[string]any{"message_id": *messageID, "resolution": resolution})
	if err != nil {
		return err
	}
	if !response.OK {
		return errors.New(response.Error)
	}
	fmt.Println("message resolved")
	return nil
}

func cancelCommand(args []string) error {
	flags := flag.NewFlagSet("cancel", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	runID := flags.String("run", "", "Run ID")
	taskID := flags.String("task", "", "cancel one Task")
	workerID := flags.String("worker", "", "cancel one Worker")
	waveID := flags.String("wave", "", "cancel one Wave")
	if err := flags.Parse(args); err != nil {
		return err
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	if err != nil {
		return err
	}
	targets := 0
	for _, value := range []string{*taskID, *workerID, *waveID} {
		if value != "" {
			targets++
		}
	}
	if targets > 1 {
		return fmt.Errorf("cancel accepts at most one of --task, --worker, or --wave")
	}
	response, err := callSupervisor(runDir, "cancel", map[string]string{"task_id": *taskID, "worker_id": *workerID, "wave_id": *waveID})
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
		return fmt.Errorf("Phase 3 doctor only probes harness %q", adapter.HarnessClaudeCode)
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

func isTerminal(status domain.RunStatus) bool {
	switch status {
	case domain.RunCompleted, domain.RunFailed, domain.RunCancelled, domain.RunDegraded:
		return true
	default:
		return false
	}
}
