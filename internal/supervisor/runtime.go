package supervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
	"github.com/vnai/subagent-broker/internal/task"
	"github.com/vnai/subagent-broker/internal/wave"
)

const SchemaVersion = "v1alpha1"

type Config struct {
	BrokerHome        string        `json:"broker_home"`
	Harness           string        `json:"harness"`
	Executable        string        `json:"executable,omitempty"`
	Scenario          string        `json:"scenario,omitempty"`
	Model             string        `json:"model,omitempty"`
	SafeMode          bool          `json:"safe_mode,omitempty"`
	PermissionMode    string        `json:"permission_mode,omitempty"`
	MaxTurns          int           `json:"max_turns,omitempty"`
	QuietAfter        time.Duration `json:"quiet_after"`
	StallAfter        time.Duration `json:"stall_after"`
	HardTimeout       time.Duration `json:"hard_timeout"`
	CancelGrace       time.Duration `json:"cancel_grace"`
	ValidationTimeout time.Duration `json:"validation_timeout"`
}

func DefaultConfig() Config {
	return Config{
		Harness: "claude-code", PermissionMode: "default", MaxTurns: 8,
		QuietAfter: 30 * time.Second, StallAfter: 2 * time.Minute,
		HardTimeout: 30 * time.Minute, CancelGrace: 1500 * time.Millisecond,
		ValidationTimeout: 5 * time.Minute,
	}
}

func (c *Config) Normalize() {
	defaults := DefaultConfig()
	if c.Harness == "" {
		c.Harness = defaults.Harness
	}
	if c.PermissionMode == "" {
		c.PermissionMode = defaults.PermissionMode
	}
	if c.MaxTurns == 0 {
		c.MaxTurns = defaults.MaxTurns
	}
	if c.QuietAfter == 0 {
		c.QuietAfter = defaults.QuietAfter
	}
	if c.StallAfter == 0 {
		c.StallAfter = defaults.StallAfter
	}
	if c.HardTimeout == 0 {
		c.HardTimeout = defaults.HardTimeout
	}
	if c.CancelGrace == 0 {
		c.CancelGrace = defaults.CancelGrace
	}
	if c.ValidationTimeout == 0 {
		c.ValidationTimeout = defaults.ValidationTimeout
	}
}

type ValidationResult struct {
	Command string `json:"command"`
	Passed  bool   `json:"passed"`
	Details string `json:"details,omitempty"`
}

type TaskState struct {
	Task         domain.Task           `json:"task"`
	Worker       *domain.WorkerSession `json:"worker,omitempty"`
	Dimensions   state.Dimensions      `json:"status_dimensions"`
	ReportPath   string                `json:"report_path,omitempty"`
	Validation   []ValidationResult    `json:"validation,omitempty"`
	LastError    string                `json:"last_error,omitempty"`
	LastProgress time.Time             `json:"last_progress_at,omitempty"`
}

type Snapshot struct {
	SchemaVersion string      `json:"schema_version"`
	Run           domain.Run  `json:"run"`
	Wave          domain.Wave `json:"wave"`
	Tasks         []TaskState `json:"tasks"`
	UpdatedAt     time.Time   `json:"updated_at"`
	LastError     string      `json:"last_error,omitempty"`
}

type Service struct {
	mu           sync.Mutex
	runDir       string
	paths        storage.RunPaths
	registry     *adapter.Registry
	config       Config
	snapshot     Snapshot
	events       *event.Store
	listener     net.Listener
	terminal     chan struct{}
	current      adapter.Adapter
	currentID    string
	workerCancel context.CancelFunc
	cancelled    bool
	recovering   bool
}

func Load(runDir string, registry *adapter.Registry, recovering bool) (*Service, error) {
	if registry == nil {
		return nil, fmt.Errorf("adapter registry is required")
	}
	runData, err := os.ReadFile(filepath.Join(runDir, "run.json"))
	if err != nil {
		return nil, fmt.Errorf("read run.json: %w", err)
	}
	var run domain.Run
	if err := json.Unmarshal(runData, &run); err != nil {
		return nil, fmt.Errorf("decode run.json: %w", err)
	}
	if err := validateRun(run); err != nil {
		return nil, err
	}
	var config Config
	if err := json.Unmarshal(run.ConfigSnapshot, &config); err != nil {
		return nil, fmt.Errorf("decode run config: %w", err)
	}
	config.Normalize()
	if config.BrokerHome == "" {
		config.BrokerHome = filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(runDir))))
	}
	layout, err := storage.NewLayout(config.BrokerHome)
	if err != nil {
		return nil, err
	}
	paths, err := layout.RunPaths(string(run.ProjectID), string(run.RunID))
	if err != nil {
		return nil, err
	}
	if filepath.Clean(paths.Root) != filepath.Clean(runDir) {
		return nil, fmt.Errorf("run directory does not match run identity: %s != %s", runDir, paths.Root)
	}
	waveValue := domain.Wave{WaveID: run.CurrentWave, Ordinal: 1, Status: domain.WavePlanned}
	if data, readErr := os.ReadFile(filepath.Join(paths.Waves, string(run.CurrentWave), "wave.json")); readErr == nil {
		_ = json.Unmarshal(data, &waveValue)
	}
	tasks := make([]TaskState, 0, len(run.TaskIDs))
	for _, taskID := range run.TaskIDs {
		taskPaths, pathErr := layout.TaskPaths(string(run.ProjectID), string(run.RunID), string(taskID))
		if pathErr != nil {
			return nil, pathErr
		}
		data, readErr := os.ReadFile(taskPaths.Task)
		if readErr != nil {
			return nil, fmt.Errorf("read task %s: %w", taskID, readErr)
		}
		var item domain.Task
		if err := json.Unmarshal(data, &item); err != nil {
			return nil, fmt.Errorf("decode task %s: %w", taskID, err)
		}
		tasks = append(tasks, TaskState{Task: item, Dimensions: state.Dimensions{Process: state.ProcessQueued, Protocol: state.ProtocolInitializing, Progress: state.ProgressUnknown, Task: item.Status}})
	}
	snapshot := Snapshot{SchemaVersion: SchemaVersion, Run: run, Wave: waveValue, Tasks: tasks, UpdatedAt: time.Now().UTC()}
	if data, readErr := os.ReadFile(paths.State); readErr == nil {
		var persisted Snapshot
		if json.Unmarshal(data, &persisted) == nil && persisted.Run.RunID == run.RunID && len(persisted.Tasks) == len(tasks) {
			snapshot = persisted
		}
	}
	replay, err := event.Replay(paths.Events)
	if err != nil {
		return nil, fmt.Errorf("replay run events: %w", err)
	}
	return &Service{
		runDir: runDir, paths: paths, registry: registry, config: config,
		snapshot: snapshot, events: event.NewStore(paths.Events, string(run.RunID), lastSequence(replay)),
		terminal: make(chan struct{}), recovering: recovering,
	}, nil
}

func (s *Service) Start(ctx context.Context) error {
	if err := s.prepareIPC(); err != nil {
		return err
	}
	if err := s.writeSupervisorIdentity(""); err != nil {
		return err
	}
	go s.heartbeat(ctx)
	if s.recovering {
		if err := s.reconcileRecovery(ctx); err != nil {
			return err
		}
	} else {
		s.setRunStatus(domain.RunStarting, "")
		s.append(event.Input{Source: "supervisor", Type: event.RunStarted, Severity: "info"})
	}
	go s.serveIPC(ctx)
	err := s.execute(ctx)
	s.mu.Lock()
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	_ = os.Remove(SocketPath(s.runDir))
	s.mu.Unlock()
	_ = s.writeSupervisorIdentity(shutdownReason(err, s.snapshot.Run.Status))
	close(s.terminal)
	return err
}

func (s *Service) Terminal() <-chan struct{} { return s.terminal }

func (s *Service) Initialize() error {
	s.append(event.Input{Source: "dispatch", Type: event.RunCreated, Severity: "info"})
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

func (s *Service) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneSnapshot(s.snapshot)
}

func (s *Service) RequestCancel(ctx context.Context) error {
	s.mu.Lock()
	if s.cancelled {
		s.mu.Unlock()
		return nil
	}
	s.cancelled = true
	current := s.current
	currentID := s.currentID
	workerCancel := s.workerCancel
	s.mu.Unlock()
	if current != nil && currentID != "" {
		_ = current.InterruptTurn(ctx, currentID)
		grace := s.config.CancelGrace
		if grace <= 0 {
			grace = 1500 * time.Millisecond
		}
		timer := time.NewTimer(grace)
		go func() {
			<-timer.C
			_ = current.TerminateSession(context.Background(), currentID)
			if workerCancel != nil {
				workerCancel()
			}
		}()
	} else if workerCancel != nil {
		workerCancel()
	}
	s.append(event.Input{Source: "supervisor", Type: "run.cancel_requested", Severity: "warning"})
	return nil
}

func (s *Service) execute(ctx context.Context) error {
	if len(s.snapshot.Tasks) == 0 {
		return s.finishRun(domain.RunFailed, "run has no tasks")
	}
	if s.snapshot.Run.Status == domain.RunCompleted || s.snapshot.Run.Status == domain.RunFailed || s.snapshot.Run.Status == domain.RunCancelled || s.snapshot.Run.Status == domain.RunDegraded {
		return nil
	}
	if err := s.preflight(); err != nil {
		return s.finishRun(domain.RunFailed, err.Error())
	}
	s.setWaveStatus(domain.WaveRunning)
	s.append(event.Input{Source: "supervisor", Type: event.WaveStarted, Severity: "info"})
	for i := range s.snapshot.Tasks {
		if s.snapshot.Tasks[i].Task.Status == state.TaskVerifiedSuccess || s.snapshot.Tasks[i].Task.Status == state.TaskVerifiedPartial {
			continue
		}
		if err := s.executeTask(ctx, &s.snapshot.Tasks[i]); err != nil {
			return s.finishRun(runStatusForTask(s.snapshot.Tasks[i].Task.Status), err.Error())
		}
		if s.snapshot.Tasks[i].Task.Status != state.TaskVerifiedSuccess && s.snapshot.Tasks[i].Task.Status != state.TaskVerifiedPartial {
			return s.finishRun(runStatusForTask(s.snapshot.Tasks[i].Task.Status), s.snapshot.Tasks[i].LastError)
		}
	}
	if s.snapshot.Run.Status != domain.RunCancelled {
		s.setWaveStatus(domain.WaveVerified)
		s.append(event.Input{Source: "supervisor", Type: event.WaveVerified, Severity: "info"})
		return s.finishRun(domain.RunCompleted, "")
	}
	return s.finishRun(domain.RunCancelled, "run cancelled")
}

func (s *Service) executeTask(parent context.Context, runtime *TaskState) error {
	if err := task.ValidateContract(runtime.Task); err != nil {
		return err
	}
	harness, ok := s.registry.Get(adapter.HarnessName(s.config.Harness))
	if !ok {
		return fmt.Errorf("adapter %q is not registered", s.config.Harness)
	}
	workerID := fmt.Sprintf("worker-%d", time.Now().UTC().UnixNano())
	worker := &domain.WorkerSession{WorkerID: domain.WorkerID(workerID), TaskID: runtime.Task.TaskID, Harness: s.config.Harness, AdapterVersion: harness.Descriptor().AdapterVersion, StartedAt: time.Now().UTC(), LastEventAt: time.Now().UTC(), LastProgressAt: time.Now().UTC(), Attempt: 1, StatusDimensions: state.Dimensions{Process: state.ProcessStarting, Protocol: state.ProtocolInitializing, Progress: state.ProgressActive, Task: state.TaskRunning}}
	resumeSessionID := ""
	if s.recovering && runtime.Worker != nil {
		resumeSessionID = runtime.Worker.NativeSessionID
		worker.Attempt = runtime.Worker.Attempt + 1
	}
	runtime.Worker = worker
	runtime.Task.Status = state.TaskRunning
	runtime.Dimensions = worker.StatusDimensions
	runtime.LastProgress = worker.LastProgressAt
	s.setRunStatus(domain.RunRunning, "")
	s.save()
	s.append(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor", Type: event.SessionStarting, Severity: "info"})

	options := map[string]string{"permission_mode": s.config.PermissionMode, "max_turns": fmt.Sprintf("%d", s.config.MaxTurns), "scenario": s.config.Scenario}
	if s.config.SafeMode {
		options["safe_mode"] = "true"
	}
	workerCtx, cancel := context.WithCancel(parent)
	if s.config.HardTimeout > 0 {
		workerCtx, cancel = context.WithTimeout(parent, s.config.HardTimeout)
	}
	s.mu.Lock()
	s.current = harness
	s.currentID = ""
	s.workerCancel = cancel
	s.mu.Unlock()
	request := adapter.StartRequest{RunID: string(s.snapshot.Run.RunID), TaskID: string(runtime.Task.TaskID), WorkerID: workerID, ProjectRoot: runtime.Task.ProjectRoot, Contract: buildWorkerPrompt(runtime.Task, s.snapshot.Run.RunID, workerID), Model: s.config.Model, Scenario: s.config.Scenario, Options: options}
	var session adapter.Session
	var err error
	if resumeSessionID != "" {
		worker.NativeSessionID = resumeSessionID
		session, err = harness.ResumeSession(workerCtx, adapter.ResumeRequest{NativeSessionID: resumeSessionID, RunID: request.RunID, TaskID: request.TaskID, WorkerID: request.WorkerID, ProjectRoot: request.ProjectRoot, Contract: request.Contract, Model: request.Model, Options: options})
	} else {
		session, err = harness.StartSession(workerCtx, request)
	}
	if err != nil {
		cancel()
		runtime.LastError = err.Error()
		if s.isCancelled() {
			return s.cancelTask(runtime, "cancelled before the Worker session started")
		}
		return s.failTask(runtime, "start_session", err)
	}
	s.mu.Lock()
	s.currentID = session.NativeSessionID
	s.mu.Unlock()
	worker.NativeSessionID = session.NativeSessionID
	worker.NativeTurnID = session.NativeTurnID
	worker.PID = session.PID
	worker.ProcessStartToken = session.ProcessStartToken
	worker.StatusDimensions.Process = state.ProcessAlive
	runtime.Dimensions = worker.StatusDimensions
	s.save()
	s.append(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor", Type: event.ProcessSpawned, Severity: "info", Payload: map[string]any{"pid": session.PID}})
	s.append(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor", Type: event.SessionStarted, Severity: "info"})

	events := session.Events
	stderr := session.Stderr
	resultSeen := false
	contextDone := workerCtx.Done()
	progressTicker := time.NewTicker(250 * time.Millisecond)
	defer progressTicker.Stop()
eventLoop:
	for events != nil || stderr != nil {
		select {
		case <-contextDone:
			contextDone = nil
			if !resultSeen {
				_ = harness.TerminateSession(context.Background(), session.NativeSessionID)
			}
		case now := <-progressTicker.C:
			s.updateProgress(runtime, now.UTC())
		case native, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			s.handleNative(runtime, harness, native, workerID)
			if native.Kind == event.ResultSubmitted || native.Kind == event.TurnFailed {
				resultSeen = native.Kind == event.ResultSubmitted
				_ = harness.TerminateSession(context.Background(), session.NativeSessionID)
				break eventLoop
			}
		case chunk, ok := <-stderr:
			if !ok {
				stderr = nil
				continue
			}
			_ = appendFile(filepath.Join(s.taskDir(runtime.Task), "stderr.log"), chunk.Data)
		}
	}
	timedOut := errors.Is(workerCtx.Err(), context.DeadlineExceeded)
	exit := adapter.ExitStatus{Code: -1}
	if session.Exited != nil {
		if value, ok := <-session.Exited; ok {
			exit = value
		}
	}
	worker.ExitCode = &exit.Code
	worker.EndedAt = timePtr(time.Now().UTC())
	worker.StatusDimensions.Process = state.ProcessExited
	runtime.Dimensions = worker.StatusDimensions
	s.append(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor", Type: event.ProcessExited, Severity: severityForExit(exit.Code), Payload: exit})
	s.save()
	cancel()
	s.mu.Lock()
	s.current = nil
	s.currentID = ""
	s.workerCancel = nil
	s.mu.Unlock()

	s.mu.Lock()
	cancelled := s.cancelled
	s.mu.Unlock()
	if cancelled {
		return s.cancelTask(runtime, "cancelled by main agent")
	}
	if timedOut {
		return s.failTask(runtime, "hard_timeout", fmt.Errorf("Worker exceeded the hard timeout"))
	}
	if exit.Code != 0 && !resultSeen {
		return s.failTask(runtime, "process", fmt.Errorf("Claude exited with code %d: %s", exit.Code, exit.Error))
	}
	result, err := harness.CollectFinalResult(context.Background(), session.NativeSessionID)
	if err != nil {
		return s.failTask(runtime, "result", err)
	}
	if result.TaskID != string(runtime.Task.TaskID) || result.WorkerID != workerID {
		return s.failTask(runtime, "result", fmt.Errorf("result identity mismatch: task=%q worker=%q", result.TaskID, result.WorkerID))
	}
	taskDir := s.taskDir(runtime.Task)
	if err := report.Publish(taskDir, result, time.Now().UTC()); err != nil {
		return s.failTask(runtime, "result_validation", err)
	}
	runtime.ReportPath = filepath.Join(taskDir, "report.md")
	s.append(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor", Type: event.ReportPublished, Severity: "info"})
	if err := s.transitionTask(runtime, state.TaskReportedComplete); err != nil {
		return err
	}
	if result.Status != report.StatusSucceeded {
		return s.markPartial(runtime, result.Status)
	}
	if err := s.transitionTask(runtime, state.TaskVerifying); err != nil {
		return err
	}
	passed := s.runValidation(parent, runtime)
	if !passed {
		_ = s.transitionTask(runtime, state.TaskVerificationFailed)
		s.append(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "verifier", Type: event.TaskVerificationFailed, Severity: "error"})
		return fmt.Errorf("task validation failed")
	}
	if err := s.transitionTask(runtime, state.TaskVerifiedSuccess); err != nil {
		return err
	}
	s.append(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "verifier", Type: event.TaskVerifiedSuccess, Severity: "info"})
	return nil
}

func (s *Service) handleNative(runtime *TaskState, harness adapter.Adapter, native adapter.NativeEvent, workerID string) {
	input, err := harness.NormalizeEvent(native)
	if err != nil {
		input = event.Input{Timestamp: native.Timestamp, Source: "supervisor", Type: "protocol.error", Severity: "error", Payload: map[string]string{"error": err.Error()}}
	}
	input.TaskID = string(runtime.Task.TaskID)
	input.WorkerID = workerID
	s.append(input)
	_ = appendFile(filepath.Join(s.taskDir(runtime.Task), "stdout.log"), append(native.Payload, '\n'))
	worker := runtime.Worker
	if worker == nil {
		return
	}
	now := time.Now().UTC()
	worker.LastEventAt = now
	worker.LastProgressAt = now
	runtime.LastProgress = now
	worker.StatusDimensions.Progress = state.ProgressActive
	switch native.Kind {
	case event.TurnStarted:
		worker.StatusDimensions.Protocol = state.ProtocolThinking
	case event.ModelOutputDelta, event.ModelMessageCompleted:
		worker.StatusDimensions.Protocol = state.ProtocolStreaming
	case event.ToolStarted:
		worker.StatusDimensions.Protocol = state.ProtocolToolRunning
	case event.ToolCompleted, event.ToolOutput:
		worker.StatusDimensions.Protocol = state.ProtocolThinking
	case event.PermissionRequested:
		worker.StatusDimensions.Protocol = state.ProtocolWaitingPermission
		worker.StatusDimensions.Progress = state.ProgressQuiet
	case event.UserInputRequested:
		worker.StatusDimensions.Protocol = state.ProtocolWaitingUser
		worker.StatusDimensions.Progress = state.ProgressQuiet
	case event.ScopeExpansionRequested:
		worker.StatusDimensions.Protocol = state.ProtocolWaitingScope
		worker.StatusDimensions.Progress = state.ProgressQuiet
	case event.APIRetrying:
		worker.StatusDimensions.Protocol = state.ProtocolRetrying
	case event.ResultSubmitted:
		worker.StatusDimensions.Protocol = state.ProtocolIdleBetweenTurns
	case event.TurnFailed, "protocol.error":
		worker.StatusDimensions.Protocol = state.ProtocolError
	}
	runtime.Dimensions = worker.StatusDimensions
	s.save()
}

func (s *Service) updateProgress(runtime *TaskState, now time.Time) {
	worker := runtime.Worker
	if worker == nil || state.IsWaiting(worker.StatusDimensions.Protocol) || worker.LastProgressAt.IsZero() {
		return
	}
	elapsed := now.Sub(worker.LastProgressAt)
	desired := worker.StatusDimensions.Progress
	switch {
	case elapsed >= 2*s.config.StallAfter:
		desired = state.ProgressStalled
	case elapsed >= s.config.StallAfter:
		desired = state.ProgressSuspectedStall
	case elapsed >= s.config.QuietAfter:
		desired = state.ProgressQuiet
	default:
		return
	}
	if desired == worker.StatusDimensions.Progress {
		return
	}
	if err := state.ValidateProgressTransition(worker.StatusDimensions.Progress, desired); err != nil {
		return
	}
	worker.StatusDimensions.Progress = desired
	runtime.Dimensions = worker.StatusDimensions
	s.save()
	s.append(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: string(worker.WorkerID), Source: "supervisor", Type: "progress." + string(desired), Severity: "warning", Payload: map[string]any{"quiet_for": elapsed.String()}})
}

func (s *Service) runValidation(ctx context.Context, runtime *TaskState) bool {
	allPassed := true
	for index, validation := range runtime.Task.ValidationCommands {
		validationCtx, cancel := context.WithTimeout(ctx, s.config.ValidationTimeout)
		command := exec.CommandContext(validationCtx, "sh", "-c", validation.Command)
		command.Dir = runtime.Task.ProjectRoot
		output, err := command.CombinedOutput()
		cancel()
		passed := err == nil
		details := strings.TrimSpace(string(output))
		if err != nil && details == "" {
			details = err.Error()
		}
		runtime.Validation = append(runtime.Validation, ValidationResult{Command: validation.Command, Passed: passed, Details: details})
		validationPath := filepath.Join(s.taskDir(runtime.Task), "validation", fmt.Sprintf("%03d.log", index+1))
		_ = appendFile(validationPath, output)
		if !passed {
			allPassed = false
		}
	}
	s.save()
	return allPassed
}

func (s *Service) reconcileRecovery(ctx context.Context) error {
	s.setRunStatus(domain.RunRecovering, "")
	s.append(event.Input{Source: "recovery", Type: "run.recovering", Severity: "warning"})
	for i := range s.snapshot.Tasks {
		runtime := &s.snapshot.Tasks[i]
		if runtime.Worker == nil || runtime.Task.Status != state.TaskRunning {
			continue
		}
		if runtime.Worker.PID > 0 && runtime.Worker.ProcessStartToken != "" {
			identity, err := process.Inspect(ctx, runtime.Worker.PID)
			if err == nil && runtime.Worker.ProcessStartToken == identity.StartToken {
				runtime.Dimensions.Process = state.ProcessOrphaned
				runtime.Worker.StatusDimensions.Process = state.ProcessOrphaned
				s.snapshot.Run.Status = domain.RunDegraded
				s.snapshot.LastError = "worker process is alive but cannot be reattached safely"
				s.save()
				s.append(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: string(runtime.Worker.WorkerID), Source: "recovery", Type: event.ProcessOrphaned, Severity: "error"})
				return nil
			}
		}
	}
	return nil
}

func (s *Service) preflight() error {
	tasks := make([]domain.Task, 0, len(s.snapshot.Tasks))
	for _, runtime := range s.snapshot.Tasks {
		tasks = append(tasks, runtime.Task)
	}
	result := wave.Preflight(tasks)
	if !result.Allowed {
		return fmt.Errorf("preflight rejected: %s", formatPreflightIssues(result.Issues))
	}
	return nil
}

func (s *Service) transitionTask(runtime *TaskState, next state.Task) error {
	if err := state.ValidateTaskTransition(runtime.Task.Status, next); err != nil {
		return err
	}
	runtime.Task.Status = next
	if runtime.Worker != nil {
		runtime.Worker.StatusDimensions.Task = next
		runtime.Dimensions.Task = next
	}
	s.save()
	if next == state.TaskReportedComplete {
		s.append(event.Input{TaskID: string(runtime.Task.TaskID), Source: "supervisor", Type: event.TaskReportedComplete, Severity: "info"})
	}
	return nil
}

func (s *Service) markPartial(runtime *TaskState, status report.Status) error {
	if err := s.transitionTask(runtime, state.TaskVerifying); err != nil {
		return err
	}
	if err := s.transitionTask(runtime, state.TaskVerifiedPartial); err != nil {
		return err
	}
	runtime.LastError = fmt.Sprintf("worker reported %s", status)
	s.save()
	return nil
}

func (s *Service) failTask(runtime *TaskState, stage string, err error) error {
	runtime.LastError = err.Error()
	if runtime.Worker != nil {
		runtime.Worker.StatusDimensions.Protocol = state.ProtocolError
		runtime.Worker.StatusDimensions.Task = state.TaskFailed
		runtime.Dimensions = runtime.Worker.StatusDimensions
	}
	if runtime.Task.Status != state.TaskFailed && runtime.Task.Status != state.TaskCancelled {
		if transitionErr := s.transitionTask(runtime, state.TaskFailed); transitionErr != nil {
			return transitionErr
		}
	}
	if stage != "result" && stage != "result_validation" && runtime.ReportPath == "" {
		failed := report.Envelope{SchemaVersion: report.SchemaVersion, TaskID: string(runtime.Task.TaskID), WorkerID: workerID(runtime), Status: report.StatusFailed, Summary: "Worker execution failed", NoFilesChangedReason: "No verified file list was available after the failure", ValidationNotRunReason: "validation was not reached", FailureStage: stage, ErrorSummary: err.Error(), WorkspaceState: "Workspace was left in place; no rollback was performed", HandoffNotes: []string{"Main Agent should inspect the workspace before retrying."}}
		if publishErr := report.Publish(s.taskDir(runtime.Task), failed, time.Now().UTC()); publishErr == nil {
			runtime.ReportPath = filepath.Join(s.taskDir(runtime.Task), "report.md")
			s.append(event.Input{TaskID: string(runtime.Task.TaskID), Source: "supervisor", Type: event.ReportPublished, Severity: "error"})
		}
	}
	s.append(event.Input{TaskID: string(runtime.Task.TaskID), Source: "supervisor", Type: event.TurnFailed, Severity: "error", Payload: map[string]string{"stage": stage, "error": err.Error()}})
	return err
}

func (s *Service) cancelTask(runtime *TaskState, reason string) error {
	runtime.LastError = reason
	if runtime.Task.Status != state.TaskCancelled {
		if err := s.transitionTask(runtime, state.TaskCancelled); err != nil {
			return err
		}
	}
	result := report.Envelope{SchemaVersion: report.SchemaVersion, TaskID: string(runtime.Task.TaskID), WorkerID: workerID(runtime), Status: report.StatusCancelled, Summary: "Task was cancelled", NoFilesChangedReason: "cancellation state was collected before verification", ValidationNotRunReason: "run cancelled", StopReason: reason, WorkspaceState: "workspace was left in place; no rollback was performed", HandoffNotes: []string{"Main Agent must inspect the current workspace before retrying."}}
	if err := report.Publish(s.taskDir(runtime.Task), result, time.Now().UTC()); err == nil {
		runtime.ReportPath = filepath.Join(s.taskDir(runtime.Task), "report.md")
		s.append(event.Input{TaskID: string(runtime.Task.TaskID), Source: "supervisor", Type: event.ReportPublished, Severity: "warning"})
	}
	s.save()
	return nil
}

func (s *Service) finishRun(status domain.RunStatus, reason string) error {
	if status == domain.RunFailed || status == domain.RunDegraded {
		s.setWaveStatus(domain.WaveFailed)
	} else if status == domain.RunCancelled {
		s.setWaveStatus(domain.WaveCancelled)
	}
	s.setRunStatus(status, reason)
	typeName := event.RunCompleted
	severity := "info"
	if status == domain.RunFailed || status == domain.RunDegraded {
		typeName = event.RunFailed
		severity = "error"
	} else if status == domain.RunCancelled {
		typeName = "run.cancelled"
		severity = "warning"
	}
	s.append(event.Input{Source: "supervisor", Type: typeName, Severity: severity, Payload: map[string]string{"reason": reason}})
	s.save()
	return nil
}

func (s *Service) isCancelled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelled
}

func (s *Service) setRunStatus(status domain.RunStatus, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	s.snapshot.Run.Status = status
	if reason != "" {
		s.snapshot.LastError = reason
	}
	if s.snapshot.Run.StartedAt == nil && status != domain.RunPlanned {
		s.snapshot.Run.StartedAt = &now
	}
	if status == domain.RunCompleted || status == domain.RunFailed || status == domain.RunCancelled || status == domain.RunDegraded {
		s.snapshot.Run.EndedAt = &now
	}
	s.snapshot.UpdatedAt = now
	_ = s.saveLocked()
}

func (s *Service) setWaveStatus(status domain.WaveStatus) {
	s.mu.Lock()
	s.snapshot.Wave.Status = status
	now := time.Now().UTC()
	if status == domain.WaveRunning {
		s.snapshot.Wave.StartedAt = &now
	}
	if status == domain.WaveVerified || status == domain.WaveFailed || status == domain.WaveCancelled {
		s.snapshot.Wave.EndedAt = &now
	}
	s.snapshot.UpdatedAt = now
	_ = s.saveLocked()
	s.mu.Unlock()
}

func (s *Service) append(input event.Input) {
	_, _ = s.events.Append(input)
}

func (s *Service) save() {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.saveLocked()
}

func (s *Service) saveLocked() error {
	s.snapshot.UpdatedAt = time.Now().UTC()
	if err := storage.AtomicWriteJSON(s.paths.State, s.snapshot, 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteJSON(s.paths.Run, s.snapshot.Run, 0o600); err != nil {
		return err
	}
	for _, runtime := range s.snapshot.Tasks {
		taskPaths, err := storage.NewLayout(s.config.BrokerHome)
		if err != nil {
			return err
		}
		paths, err := taskPaths.TaskPaths(string(s.snapshot.Run.ProjectID), string(s.snapshot.Run.RunID), string(runtime.Task.TaskID))
		if err != nil {
			return err
		}
		if err := storage.AtomicWriteJSON(paths.Task, runtime.Task, 0o600); err != nil {
			return err
		}
	}
	status := renderStatus(s.snapshot)
	if err := storage.AtomicWriteFile(s.paths.Status, []byte(status), 0o600); err != nil {
		return err
	}
	return storage.AtomicWriteFile(s.paths.RunSummary, []byte(renderRunSummary(s.snapshot)), 0o600)
}

func (s *Service) taskDir(item domain.Task) string {
	return filepath.Join(s.paths.Tasks, string(item.TaskID))
}

func (s *Service) prepareIPC() error {
	if err := os.MkdirAll(filepath.Join(s.runDir, "control"), 0o700); err != nil {
		return err
	}
	path := SocketPath(s.runDir)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	_ = os.Remove(path)
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return err
	}
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	return nil
}

func (s *Service) writeSupervisorIdentity(reason string) error {
	identity, err := process.Inspect(context.Background(), os.Getpid())
	if err != nil {
		return err
	}
	value := domain.SupervisorIdentity{PID: identity.PID, ProcessStartToken: identity.StartToken, Executable: os.Args[0], ExecutableVersion: "phase1", IPCEndpoint: SocketPath(s.runDir), HeartbeatAt: time.Now().UTC(), ShutdownReason: reason}
	s.mu.Lock()
	s.snapshot.Run.SupervisorIdentity = &value
	s.snapshot.UpdatedAt = time.Now().UTC()
	err = s.saveLocked()
	s.mu.Unlock()
	return err
}

func (s *Service) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.terminal:
			return
		case now := <-ticker.C:
			s.mu.Lock()
			if s.snapshot.Run.SupervisorIdentity != nil {
				s.snapshot.Run.SupervisorIdentity.HeartbeatAt = now.UTC()
			}
			_ = s.saveLocked()
			s.mu.Unlock()
		}
	}
}

func (s *Service) serveIPC(ctx context.Context) {
	for {
		s.mu.Lock()
		listener := s.listener
		s.mu.Unlock()
		if listener == nil {
			return
		}
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			return
		}
		go s.handleConnection(ctx, conn)
	}
}

func validateRun(run domain.Run) error {
	if run.SchemaVersion == "" || run.RunID == "" || run.ProjectID == "" || len(run.TaskIDs) == 0 {
		return fmt.Errorf("run metadata is incomplete")
	}
	return nil
}

func lastSequence(result event.ReplayResult) uint64 {
	if len(result.Events) == 0 {
		return 0
	}
	return result.Events[len(result.Events)-1].Seq
}

func timePtr(value time.Time) *time.Time { return &value }

func severityForExit(code int) string {
	if code == 0 {
		return "info"
	}
	return "error"
}

func workerID(runtime *TaskState) string {
	if runtime.Worker == nil {
		return "worker-unknown"
	}
	return string(runtime.Worker.WorkerID)
}

func runStatusForTask(status state.Task) domain.RunStatus {
	if status == state.TaskCancelled {
		return domain.RunCancelled
	}
	return domain.RunFailed
}

func shutdownReason(err error, status domain.RunStatus) string {
	if err != nil {
		return err.Error()
	}
	return string(status)
}

func formatPreflightIssues(issues []wave.Issue) string {
	parts := make([]string, 0, len(issues))
	for _, issue := range issues {
		parts = append(parts, string(issue.Kind)+": "+issue.Details)
	}
	return strings.Join(parts, "; ")
}

func buildWorkerPrompt(item domain.Task, runID domain.RunID, workerID string) string {
	contract, err := task.RenderContract(item, runID)
	if err != nil {
		return item.Objective
	}
	return contract + fmt.Sprintf("\n\nSupervisor identities: task_id=%s, worker_id=%s. Return the final Result Envelope as one JSON object only. Do not wrap it in Markdown fences or add commentary. The JSON must use schema_version v1alpha1 and these exact identities.", item.TaskID, workerID)
}

func appendFile(path string, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(data)
	return err
}

func cloneSnapshot(source Snapshot) Snapshot {
	data, err := json.Marshal(source)
	if err != nil {
		return source
	}
	var copy Snapshot
	if json.Unmarshal(data, &copy) != nil {
		return source
	}
	return copy
}

func SocketPath(runDir string) string {
	path := filepath.Join(runDir, "control", "supervisor.sock")
	if len(path) < 100 {
		return path
	}
	sum := sha256.Sum256([]byte(path))
	return filepath.Join(os.TempDir(), "subagent-broker-"+hex.EncodeToString(sum[:8])+".sock")
}
