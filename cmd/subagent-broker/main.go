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
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/claude"
	"github.com/vnai/subagent-broker/internal/clioutcome"
	"github.com/vnai/subagent-broker/internal/cliread"
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
	os.Exit(runMain(os.Args[1:], os.Stdout, os.Stderr))
}

// runMain executes a CLI invocation and returns a stable process exit code.
// Tests call this instead of os.Exit.
func runMain(args []string, stdout, stderr io.Writer) int {
	_ = stdout // command bodies still write to os.Stdout; error path is testable.
	err := execute(args)
	if err != nil {
		fmt.Fprintln(stderr, err)
	}
	return int(clioutcome.CodeOf(err))
}

func execute(args []string) error {
	if len(args) == 0 {
		return usageError()
	}
	var err error
	switch args[0] {
	case "dispatch":
		err = dispatch(args[1:])
	case "supervisor":
		err = runSupervisor(args[1:])
	case "mcp-worker":
		err = runMCPWorker(args[1:])
	case "claude-hook":
		err = runClaudeHook(args[1:])
	case "status":
		err = statusCommand(args[1:])
	case "events":
		err = eventsCommand(args[1:])
	case "wait":
		err = waitCommand(args[1:])
	case "barrier":
		err = barrierCommand(args[1:])
	case "collect":
		err = collectCommand(args[1:])
	case "inbox":
		err = inboxCommand(args[1:])
	case "send":
		err = sendCommand(args[1:])
	case "cancel":
		err = cancelCommand(args[1:])
	case "recover":
		err = recoverCommand(args[1:])
	case "doctor":
		err = doctorCommand(args[1:])
	case "help", "-h", "--help":
		return usageError()
	default:
		return clioutcome.New(clioutcome.ExitUsage, "execute", fmt.Sprintf("unknown command %q", args[0]), nil)
	}
	return normalizeCommandError(args[0], err)
}

func usageError() error {
	return clioutcome.New(clioutcome.ExitUsage, "usage", "usage: subagent-broker <dispatch|status|events|wait|barrier|collect|inbox|send|cancel|recover|doctor>", nil)
}

func usageWrap(op string, err error) error {
	if err == nil {
		return nil
	}
	var typed *clioutcome.Error
	if errors.As(err, &typed) {
		return err
	}
	return clioutcome.New(clioutcome.ExitUsage, op, err.Error(), err)
}

func normalizeCommandError(command string, err error) error {
	if err == nil {
		return nil
	}
	var typed *clioutcome.Error
	if errors.As(err, &typed) {
		return err
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "requires") || strings.Contains(lower, "required") || strings.Contains(lower, "flag provided") || strings.Contains(lower, "invalid value") {
		return clioutcome.New(clioutcome.ExitUsage, command, err.Error(), err)
	}
	if errors.Is(err, os.ErrNotExist) || strings.Contains(lower, "not found") || strings.Contains(lower, "no active run") {
		return clioutcome.New(clioutcome.ExitNotFound, command, err.Error(), err)
	}
	if strings.Contains(lower, "socket") || strings.Contains(lower, "supervisor") || strings.Contains(lower, "connection refused") || strings.Contains(lower, "broken pipe") {
		return clioutcome.New(clioutcome.ExitCommunication, command, err.Error(), err)
	}
	return clioutcome.New(clioutcome.ExitInternal, command, err.Error(), err)
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
		return usageWrap("dispatch", err)
	}
	if strings.TrimSpace(*goal) == "" || strings.TrimSpace(*tasksFile) == "" {
		return clioutcome.New(clioutcome.ExitUsage, "dispatch", "dispatch requires --goal and --tasks", nil)
	}
	if *harness != string(adapter.HarnessClaudeCode) {
		return clioutcome.New(clioutcome.ExitCompatibility, "dispatch", fmt.Sprintf("Phase 3 only implements harness %q", adapter.HarnessClaudeCode), nil)
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
		if strings.Contains(err.Error(), "preflight") {
			return clioutcome.New(clioutcome.ExitPreflight, "dispatch", err.Error(), err)
		}
		return clioutcome.New(clioutcome.ExitUsage, "dispatch", err.Error(), err)
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
		return clioutcome.New(clioutcome.ExitCommunication, "dispatch", err.Error(), err)
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
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	runID := flags.String("run", "", "Run ID")
	jsonOutput := flags.Bool("json", false, "print JSON")
	if err := flags.Parse(args); err != nil {
		return usageWrap("status", err)
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	if err != nil {
		return notFoundWrap("status", err)
	}
	view, err := cliread.LoadSnapshot(runDir)
	if err != nil {
		return notFoundWrap("status", err)
	}
	outcome := clioutcome.FromRunDetailed(view.Snapshot.Run.Status, view.Snapshot.LastError)
	outcome.ID = string(view.Snapshot.Run.RunID)
	output := clioutcome.OutputFor(outcome, string(view.Meta.Source), view.Meta.Reason, view.Meta.Degraded, view.Meta.SupervisorAlive, string(view.Meta.SupervisorIdentity), view.Meta.SnapshotTime)
	if *jsonOutput {
		return json.NewEncoder(os.Stdout).Encode(struct {
			clioutcome.CLIOutput
			Snapshot supervisor.Snapshot  `json:"snapshot"`
			Meta     cliread.ReadMetadata `json:"meta"`
		}{CLIOutput: output, Snapshot: view.Snapshot, Meta: view.Meta})
	}
	printReadMetadata(view.Meta)
	_, _ = os.Stdout.Write([]byte(supervisor.RenderStatus(view.Snapshot)))
	if outcome.Terminal && outcome.Code != clioutcome.ExitOK {
		return outcome.Err("status")
	}
	return nil
}

func eventsCommand(args []string) error {
	flags := flag.NewFlagSet("events", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	runID := flags.String("run", "", "Run ID")
	since := flags.Uint64("since-seq", 0, "return events after this sequence")
	jsonOutput := flags.Bool("json", false, "print JSON")
	if err := flags.Parse(args); err != nil {
		return usageWrap("events", err)
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	if err != nil {
		return notFoundWrap("events", err)
	}
	view, err := cliread.LoadEvents(runDir, *since)
	if err != nil {
		return clioutcome.New(clioutcome.ExitCommunication, "events", "read event stream failed", err)
	}
	outcome := clioutcome.Outcome{Kind: clioutcome.KindEvent, Status: "read", Terminal: true, Successful: true, Code: clioutcome.ExitOK}
	output := clioutcome.OutputFor(outcome, string(view.Meta.Source), view.Meta.Reason, view.Meta.Degraded, view.Meta.SupervisorAlive, string(view.Meta.SupervisorIdentity), view.Meta.SnapshotTime)
	if *jsonOutput {
		return json.NewEncoder(os.Stdout).Encode(struct {
			clioutcome.CLIOutput
			Events []event.Event        `json:"events"`
			Meta   cliread.ReadMetadata `json:"meta"`
		}{CLIOutput: output, Events: view.Events, Meta: view.Meta})
	}
	printReadMetadata(view.Meta)
	encoder := json.NewEncoder(os.Stdout)
	for _, item := range view.Events {
		if err := encoder.Encode(item); err != nil {
			return clioutcome.New(clioutcome.ExitInternal, "events", "encode event", err)
		}
	}
	return nil
}

func printReadMetadata(meta cliread.ReadMetadata) {
	fmt.Printf("Data source: %s\nMode: %s\nReason: %s\nSnapshot/checkpoint time: %s\nSupervisor identity: %s\n", meta.Source, meta.Mode, meta.Reason, formatTime(meta.SnapshotTime), meta.SupervisorIdentity)
	if meta.TailRepaired {
		fmt.Printf("Journal tail: repaired\nQuarantine: %s\n", meta.QuarantinePath)
	}
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "unknown"
	}
	return value.UTC().Format(time.RFC3339Nano)
}

// WaitMatch is the result of evaluating one wait poll against a Snapshot.
type WaitMatch struct {
	Matched bool
	Outcome clioutcome.Outcome
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
	jsonOutput := flags.Bool("json", false, "print JSON")
	returnOnBlocked := flags.Bool("return-on-blocked", false, "return 21 for a blocked task instead of waiting")
	if err := flags.Parse(args); err != nil {
		return usageWrap("wait", err)
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	if err != nil {
		return notFoundWrap("wait", err)
	}
	return waitOnRunDirOptions(runDir, *timeout, *waitFor, *taskID, *waveID, *sinceSeq, *returnOnBlocked, *jsonOutput)
}

func waitOnRunDir(runDir string, timeout time.Duration, waitFor, taskID, waveID string, sinceSeq uint64) error {
	return waitOnRunDirOptions(runDir, timeout, waitFor, taskID, waveID, sinceSeq, false, false)
}

func waitOnRunDirOptions(runDir string, timeout time.Duration, waitFor, taskID, waveID string, sinceSeq uint64, returnOnBlocked, jsonOutput bool) error {
	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	params := supervisor.WaitParams{For: waitFor, TaskID: taskID, WaveID: waveID, SinceSeq: sinceSeq, ReturnOnBlocked: returnOnBlocked}
	reconnectUntil := time.Now().Add(2 * time.Second)
	for {
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return emitWaitFailure(clioutcome.Outcome{Kind: waitKind(waitFor), ID: waitID(taskID, waveID), Status: "timeout", Code: clioutcome.ExitTimeout, Reason: "wait timed out"}, cliread.ReadMetadata{Source: cliread.ReadSourceDisk, Mode: "degraded", Degraded: true, Reason: "wait timed out"}, jsonOutput)
			}
			params.Timeout = remaining
		} else {
			params.Timeout = 0
		}

		view, readErr := cliread.Wait(runDir, params)
		if readErr == nil && view.Matched && (waitFor == "event" || waitFor == "inbox") {
			status := "event"
			kind := clioutcome.KindEvent
			if waitFor == "inbox" {
				status = "pending"
				kind = clioutcome.KindInbox
			}
			return emitWaitOutcome(clioutcome.Outcome{Kind: kind, ID: waitID(taskID, waveID), Status: status, Terminal: true, Successful: true, Code: clioutcome.ExitOK}, view.Meta, jsonOutput)
		}
		match, matchErr := waitMatchWithBlocked(runDir, view.Snapshot, waitFor, taskID, waveID, sinceSeq, returnOnBlocked)
		if matchErr != nil {
			return matchErr
		}
		if match.Matched {
			return emitWaitOutcome(match.Outcome, view.Meta, jsonOutput)
		}
		if errors.Is(readErr, cliread.ErrWaitTimeout) {
			return emitWaitFailure(clioutcome.Outcome{Kind: waitKind(waitFor), ID: waitID(taskID, waveID), Status: "timeout", Code: clioutcome.ExitTimeout, Reason: "wait timed out"}, view.Meta, jsonOutput)
		}
		if errors.Is(readErr, cliread.ErrSupervisorUnavailable) {
			if !cliread.HasRunIdentity(runDir) {
				if !deadline.IsZero() && time.Now().After(deadline) {
					return emitWaitFailure(clioutcome.Outcome{Kind: waitKind(waitFor), ID: waitID(taskID, waveID), Status: "timeout", Code: clioutcome.ExitTimeout, Reason: "wait timed out"}, view.Meta, jsonOutput)
				}
				time.Sleep(50 * time.Millisecond)
				continue
			}
			if view.Meta.SupervisorAlive && time.Now().Before(reconnectUntil) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			outcome := clioutcome.Outcome{Kind: waitKind(waitFor), ID: waitID(taskID, waveID), Status: string(view.Snapshot.Run.Status), Code: clioutcome.ExitCommunication, Reason: view.Meta.Reason}
			if waitFor == "task" {
				for _, runtime := range view.Snapshot.Tasks {
					if string(runtime.Task.TaskID) == taskID {
						outcome.Status = string(runtime.Task.Status)
						break
					}
				}
			}
			return emitWaitFailure(outcome, view.Meta, jsonOutput)
		}
		if readErr != nil {
			return classifyReadError("wait", readErr)
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return emitWaitFailure(clioutcome.Outcome{Kind: waitKind(waitFor), ID: waitID(taskID, waveID), Status: "timeout", Code: clioutcome.ExitTimeout, Reason: "wait timed out"}, view.Meta, jsonOutput)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitMatch(runDir string, snapshot supervisor.Snapshot, condition, taskID, waveID string, sinceSeq uint64) (WaitMatch, error) {
	return waitMatchWithBlocked(runDir, snapshot, condition, taskID, waveID, sinceSeq, false)
}

func waitMatchWithBlocked(runDir string, snapshot supervisor.Snapshot, condition, taskID, waveID string, sinceSeq uint64, returnOnBlocked bool) (WaitMatch, error) {
	switch condition {
	case "run":
		outcome := clioutcome.FromRunDetailed(snapshot.Run.Status, snapshot.LastError)
		outcome.ID = string(snapshot.Run.RunID)
		return WaitMatch{Matched: outcome.Terminal, Outcome: outcome}, nil
	case "wave":
		if waveID == "" {
			waveID = string(snapshot.Run.CurrentWave)
		}
		for _, item := range snapshot.Waves {
			if string(item.WaveID) == waveID {
				outcome := clioutcome.FromWave(item)
				return WaitMatch{Matched: outcome.Terminal, Outcome: outcome}, nil
			}
		}
		if string(snapshot.Wave.WaveID) == waveID {
			outcome := clioutcome.FromWave(snapshot.Wave)
			return WaitMatch{Matched: outcome.Terminal, Outcome: outcome}, nil
		}
		return WaitMatch{}, clioutcome.New(clioutcome.ExitNotFound, "wait", fmt.Sprintf("Wave %q was not found", waveID), nil)
	case "task":
		if taskID == "" {
			return WaitMatch{}, clioutcome.New(clioutcome.ExitUsage, "wait", "--for task requires --task", nil)
		}
		for _, runtime := range snapshot.Tasks {
			if string(runtime.Task.TaskID) == taskID {
				outcome := clioutcome.FromTask(taskID, runtime.Task.Status, returnOnBlocked || runtime.BlockKind == supervisor.BlockKindFinal)
				return WaitMatch{Matched: outcome.Terminal, Outcome: outcome}, nil
			}
		}
		return WaitMatch{}, clioutcome.New(clioutcome.ExitNotFound, "wait", fmt.Sprintf("task %q was not found", taskID), nil)
	case "inbox":
		index, err := message.Replay(filepath.Join(runDir, "messages.jsonl"))
		if err != nil {
			return WaitMatch{}, err
		}
		if len(message.Sorted(index, false)) == 0 {
			return WaitMatch{}, nil
		}
		return WaitMatch{Matched: true, Outcome: clioutcome.Outcome{Kind: clioutcome.KindInbox, Status: "pending", Terminal: true, Successful: true, Code: clioutcome.ExitOK}}, nil
	case "event":
		replayed, err := event.Replay(filepath.Join(runDir, "events.jsonl"))
		if err != nil {
			return WaitMatch{}, err
		}
		for _, item := range replayed.Events {
			if item.Seq > sinceSeq {
				return WaitMatch{Matched: true, Outcome: clioutcome.Outcome{Kind: clioutcome.KindEvent, Status: "event", Terminal: true, Successful: true, Code: clioutcome.ExitOK, ID: fmt.Sprintf("%d", item.Seq)}}, nil
			}
		}
		return WaitMatch{}, nil
	default:
		return WaitMatch{}, clioutcome.New(clioutcome.ExitUsage, "wait", fmt.Sprintf("unsupported wait condition %q", condition), nil)
	}
}

func waitKind(condition string) clioutcome.Kind {
	switch condition {
	case "task":
		return clioutcome.KindTask
	case "wave":
		return clioutcome.KindWave
	case "event":
		return clioutcome.KindEvent
	case "inbox":
		return clioutcome.KindInbox
	default:
		return clioutcome.KindRun
	}
}

func waitID(taskID, waveID string) string {
	if taskID != "" {
		return taskID
	}
	return waveID
}

func emitWaitOutcome(outcome clioutcome.Outcome, meta cliread.ReadMetadata, jsonOutput bool) error {
	output := clioutcome.OutputFor(outcome, string(meta.Source), meta.Reason, meta.Degraded, meta.SupervisorAlive, string(meta.SupervisorIdentity), meta.SnapshotTime)
	if jsonOutput {
		if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
			return clioutcome.New(clioutcome.ExitInternal, "wait", "encode wait result", err)
		}
	} else {
		fmt.Printf("outcome: %s\nexit_code: %d\ntarget_type: %s\ntarget_id: %s\nterminal: %t\nstatus: %s\nreason: %s\ndata_source: %s\nmode: %s\ndegraded: %t\nsnapshot_time: %s\nsupervisor_alive: %t\nsupervisor_identity: %s\n", output.Outcome, output.ExitCode, output.TargetType, output.TargetID, output.Terminal, output.Status, output.Reason, output.DataSource, output.Mode, output.Degraded, formatTime(output.SnapshotTime), output.SupervisorAlive, output.SupervisorIdentity)
	}
	return outcome.Err("wait")
}

func emitWaitFailure(outcome clioutcome.Outcome, meta cliread.ReadMetadata, jsonOutput bool) error {
	return emitWaitOutcome(outcome, meta, jsonOutput)
}

func classifyReadError(op string, err error) error {
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "not found") || strings.Contains(message, "requires --") {
		code := clioutcome.ExitNotFound
		if strings.Contains(message, "requires --") {
			code = clioutcome.ExitUsage
		}
		return clioutcome.New(code, op, err.Error(), err)
	}
	return clioutcome.New(clioutcome.ExitCommunication, op, err.Error(), err)
}

func barrierCommand(args []string) error {
	if len(args) == 0 {
		return clioutcome.New(clioutcome.ExitUsage, "barrier", "usage: subagent-broker barrier <show|accept|reject>", nil)
	}
	switch args[0] {
	case "show":
		return barrierShow(args[1:])
	case "accept":
		return barrierDecision(args[1:], true)
	case "reject":
		return barrierDecision(args[1:], false)
	default:
		return clioutcome.New(clioutcome.ExitUsage, "barrier", fmt.Sprintf("unsupported barrier operation %q", args[0]), nil)
	}
}

func barrierShow(args []string) error {
	flags := flag.NewFlagSet("barrier show", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	runID := flags.String("run", "", "Run ID")
	waveID := flags.String("wave", "", "Wave ID")
	jsonOutput := flags.Bool("json", false, "print JSON")
	if err := flags.Parse(args); err != nil {
		return usageWrap("barrier show", err)
	}
	if strings.TrimSpace(*waveID) == "" {
		return clioutcome.New(clioutcome.ExitUsage, "barrier show", "barrier show requires --wave", nil)
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	if err != nil {
		return notFoundWrap("barrier show", err)
	}
	view, err := cliread.LoadSnapshot(runDir)
	if err != nil {
		return notFoundWrap("barrier show", err)
	}
	value, found := findWave(view.Snapshot, *waveID)
	if !found {
		return clioutcome.New(clioutcome.ExitNotFound, "barrier show", fmt.Sprintf("Wave %q was not found", *waveID), nil)
	}
	verification, verificationErr := readWaveVerification(runDir, *brokerHome, view.Snapshot.Run, *waveID)
	outcome := clioutcome.FromWave(value)
	outcome.ID = *waveID
	output := clioutcome.OutputFor(outcome, string(view.Meta.Source), view.Meta.Reason, view.Meta.Degraded, view.Meta.SupervisorAlive, string(view.Meta.SupervisorIdentity), view.Meta.SnapshotTime)
	if *jsonOutput {
		return json.NewEncoder(os.Stdout).Encode(struct {
			clioutcome.CLIOutput
			Wave         domain.Wave          `json:"wave"`
			Verification *wave.Verification   `json:"verification,omitempty"`
			Meta         cliread.ReadMetadata `json:"meta"`
		}{CLIOutput: output, Wave: value, Verification: verification, Meta: view.Meta})
	}
	printReadMetadata(view.Meta)
	fmt.Printf("Wave: %s\nStatus: %s\nBarrier result: %s\nAccepted: %t\n", value.WaveID, value.Status, value.BarrierResult, value.BarrierAccepted)
	if verificationErr == nil && verification != nil {
		fmt.Printf("Input hash: %s\nWarnings: %s\nErrors: %s\n", verification.InputHash, strings.Join(verification.Warnings, "; "), strings.Join(verification.Errors, "; "))
	} else if verificationErr != nil {
		fmt.Printf("Verification: unavailable (%v)\n", verificationErr)
	}
	if outcome.Terminal && outcome.Code != clioutcome.ExitOK {
		return outcome.Err("barrier show")
	}
	return nil
}

func barrierDecision(args []string, accept bool) error {
	name := "barrier reject"
	method := "barrier.reject"
	if accept {
		name = "barrier accept"
		method = "barrier.accept"
	}
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	projectPath := flags.String("project", "", "project root")
	brokerHome := flags.String("broker-home", "", "Broker Home override")
	runID := flags.String("run", "", "Run ID")
	waveID := flags.String("wave", "", "Wave ID")
	actor := flags.String("actor", "", "decision actor; defaults to the current CLI identity")
	reason := flags.String("reason", "", "non-empty decision reason")
	jsonOutput := flags.Bool("json", false, "print JSON")
	if err := flags.Parse(args); err != nil {
		return usageWrap(name, err)
	}
	if strings.TrimSpace(*waveID) == "" || strings.TrimSpace(*reason) == "" {
		return clioutcome.New(clioutcome.ExitUsage, name, "barrier decision requires --wave and non-empty --reason", nil)
	}
	if strings.TrimSpace(*actor) == "" {
		*actor = currentCLIActor()
	}
	if strings.TrimSpace(*actor) == "" {
		return clioutcome.New(clioutcome.ExitUsage, name, "barrier decision requires a non-empty actor", nil)
	}
	runDir, err := resolveRunDir(*projectPath, *brokerHome, *runID)
	if err != nil {
		return notFoundWrap(name, err)
	}
	response, err := cliread.CallIPC(runDir, method, map[string]string{"wave_id": *waveID, "actor": *actor, "reason": *reason})
	if err != nil {
		return clioutcome.New(clioutcome.ExitCommunication, name, "Supervisor IPC unavailable; Barrier decision was not written by the CLI", err)
	}
	if !response.OK {
		code := clioutcome.ExitFailed
		lower := strings.ToLower(response.Error)
		if strings.Contains(lower, "required") || strings.Contains(lower, "requires") {
			code = clioutcome.ExitUsage
		}
		return clioutcome.New(code, name, response.Error, nil)
	}
	var snapshot supervisor.Snapshot
	raw, marshalErr := json.Marshal(response.Result)
	if marshalErr != nil || json.Unmarshal(raw, &snapshot) != nil {
		if marshalErr == nil {
			marshalErr = fmt.Errorf("decode Supervisor snapshot")
		}
		return clioutcome.New(clioutcome.ExitInternal, name, "invalid Barrier IPC response", marshalErr)
	}
	value, found := findWave(snapshot, *waveID)
	if !found {
		return clioutcome.New(clioutcome.ExitInternal, name, fmt.Sprintf("Wave %q missing from Barrier IPC response", *waveID), nil)
	}
	outcome := clioutcome.FromWave(value)
	outcome.ID = *waveID
	output := clioutcome.OutputFor(outcome, string(cliread.ReadSourceIPC), "Supervisor accepted Barrier operation", false, true, "valid", snapshot.UpdatedAt)
	if *jsonOutput {
		if err := json.NewEncoder(os.Stdout).Encode(struct {
			clioutcome.CLIOutput
			Snapshot supervisor.Snapshot `json:"snapshot"`
		}{CLIOutput: output, Snapshot: snapshot}); err != nil {
			return clioutcome.New(clioutcome.ExitInternal, name, "encode Barrier result", err)
		}
	} else {
		fmt.Printf("outcome: %s\nexit_code: %d\ntarget_type: wave\ntarget_id: %s\nstatus: %s\ndata_source: ipc\ndegraded: false\n", output.Outcome, output.ExitCode, *waveID, value.Status)
	}
	return outcome.Err(name)
}

func currentCLIActor() string {
	if value, err := user.Current(); err == nil && strings.TrimSpace(value.Username) != "" {
		return "user:" + value.Username
	}
	if value := strings.TrimSpace(os.Getenv("USER")); value != "" {
		return "user:" + value
	}
	return "user:unknown"
}

func findWave(snapshot supervisor.Snapshot, waveID string) (domain.Wave, bool) {
	for _, value := range snapshot.Waves {
		if string(value.WaveID) == waveID {
			return value, true
		}
	}
	if string(snapshot.Wave.WaveID) == waveID {
		return snapshot.Wave, true
	}
	return domain.Wave{}, false
}

func readWaveVerification(runDir, brokerHome string, runValue domain.Run, waveID string) (*wave.Verification, error) {
	layout, err := storage.NewLayout(mustHome(brokerHome))
	if err != nil {
		return nil, err
	}
	paths, err := layout.WavePaths(string(runValue.ProjectID), string(runValue.RunID), waveID)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(paths.Verification)
	if err != nil {
		return nil, err
	}
	var verification wave.Verification
	if err := json.Unmarshal(data, &verification); err != nil {
		return nil, err
	}
	return &verification, nil
}

// waitCondition remains as a thin compatibility wrapper for older call sites/tests.
func waitCondition(runDir string, snapshot supervisor.Snapshot, condition, taskID, waveID string, sinceSeq uint64) (bool, error) {
	match, err := waitMatch(runDir, snapshot, condition, taskID, waveID, sinceSeq)
	return match.Matched, err
}

func notFoundWrap(op string, err error) error {
	if err == nil {
		return nil
	}
	var typed *clioutcome.Error
	if errors.As(err, &typed) {
		return err
	}
	return clioutcome.New(clioutcome.ExitNotFound, op, err.Error(), err)
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
			return clioutcome.New(clioutcome.ExitFailed, "send", response.Error, nil)
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
		return clioutcome.New(clioutcome.ExitFailed, "send", response.Error, nil)
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
		return clioutcome.New(clioutcome.ExitFailed, "cancel", response.Error, nil)
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
		return usageWrap("doctor", err)
	}
	if *harness != string(adapter.HarnessClaudeCode) {
		return clioutcome.New(clioutcome.ExitCompatibility, "doctor", fmt.Sprintf("Phase 3 doctor only probes harness %q", adapter.HarnessClaudeCode), nil)
	}
	result, err := claude.New(*executable).Probe(context.Background(), adapter.ProbeRequest{Executable: *executable})
	if err != nil {
		return clioutcome.New(clioutcome.ExitCompatibility, "doctor", "harness probe failed", err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return err
	}
	if !result.Installed {
		return clioutcome.New(clioutcome.ExitCompatibility, "doctor", "Claude Code is not installed", nil)
	}
	switch strings.ToLower(strings.TrimSpace(result.Compatibility)) {
	case "incompatible", "probe_failed", "unavailable":
		return clioutcome.New(clioutcome.ExitCompatibility, "doctor", fmt.Sprintf("harness compatibility %q", result.Compatibility), nil)
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
		return supervisor.Response{}, clioutcome.New(clioutcome.ExitCommunication, "ipc", "supervisor socket dial failed", err)
	}
	defer conn.Close()
	runValue, err := readRun(runDir)
	if err != nil {
		return supervisor.Response{}, clioutcome.New(clioutcome.ExitNotFound, "ipc", "run metadata not found", err)
	}
	request := supervisor.Request{SchemaVersion: supervisor.SchemaVersion, RequestID: fmt.Sprintf("request-%d", time.Now().UnixNano()), RunID: string(runValue.RunID), Method: method}
	if params != nil {
		request.Params, err = json.Marshal(params)
		if err != nil {
			return supervisor.Response{}, err
		}
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return supervisor.Response{}, clioutcome.New(clioutcome.ExitCommunication, "ipc", "supervisor request encode failed", err)
	}
	var response supervisor.Response
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return supervisor.Response{}, clioutcome.New(clioutcome.ExitCommunication, "ipc", "supervisor response decode failed", err)
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
