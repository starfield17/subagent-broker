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
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
	"github.com/vnai/subagent-broker/internal/task"
	"github.com/vnai/subagent-broker/internal/verify"
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
	MaxConcurrency    int           `json:"max_concurrency"`
}

func DefaultConfig() Config {
	return Config{
		Harness: "claude-code", PermissionMode: "default", MaxTurns: 8,
		QuietAfter: 30 * time.Second, StallAfter: 2 * time.Minute,
		HardTimeout: 30 * time.Minute, CancelGrace: 1500 * time.Millisecond,
		ValidationTimeout: 5 * time.Minute, MaxConcurrency: 4,
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
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = defaults.MaxConcurrency
	}
}

type ValidationResult struct {
	Command string `json:"command"`
	Passed  bool   `json:"passed"`
	Details string `json:"details,omitempty"`
}

type TaskState struct {
	Task               domain.Task           `json:"task"`
	Worker             *domain.WorkerSession `json:"worker,omitempty"`
	Dimensions         state.Dimensions      `json:"status_dimensions"`
	ReportPath         string                `json:"report_path,omitempty"`
	Validation         []ValidationResult    `json:"validation,omitempty"`
	LastError          string                `json:"last_error,omitempty"`
	LastProgress       time.Time             `json:"last_progress_at,omitempty"`
	PendingInstruction string                `json:"pending_instruction,omitempty"`
}

type Snapshot struct {
	SchemaVersion string            `json:"schema_version"`
	Run           domain.Run        `json:"run"`
	Wave          domain.Wave       `json:"wave"`
	Waves         []domain.Wave     `json:"waves,omitempty"`
	Tasks         []TaskState       `json:"tasks"`
	Messages      []message.Message `json:"messages,omitempty"`
	UpdatedAt     time.Time         `json:"updated_at"`
	LastError     string            `json:"last_error,omitempty"`
}

type Service struct {
	mu             sync.Mutex
	runDir         string
	paths          storage.RunPaths
	registry       *adapter.Registry
	config         Config
	snapshot       Snapshot
	events         *event.Store
	listener       net.Listener
	terminal       chan struct{}
	cancelled      bool
	cancelledTasks map[string]bool
	recovering     bool
	plan           domain.RunPlan
	messages       *message.Store
	messageIndex   map[string]message.Message
	pending        map[string]chan message.Resolution
	active         map[string]activeWorker
	runBaseline    verify.WorkspaceSnapshot
}

type activeWorker struct {
	adapter   adapter.Adapter
	sessionID string
	cancel    context.CancelFunc
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
	var plan domain.RunPlan
	if data, readErr := os.ReadFile(paths.Plan); readErr == nil {
		_ = json.Unmarshal(data, &plan)
	}
	if len(plan.Waves) == 0 {
		plan = domain.RunPlan{SchemaVersion: run.SchemaVersion, Waves: []domain.WavePlan{{WaveID: run.CurrentWave}}}
	}
	waves := make([]domain.Wave, 0, len(plan.Waves))
	for ordinal, planned := range plan.Waves {
		value := domain.Wave{WaveID: planned.WaveID, Ordinal: ordinal + 1, Status: domain.WavePlanned, IntegrationChecks: planned.IntegrationChecks}
		if data, readErr := os.ReadFile(filepath.Join(paths.Waves, string(planned.WaveID), "wave.json")); readErr == nil {
			_ = json.Unmarshal(data, &value)
		}
		waves = append(waves, value)
	}
	waveValue := waves[0]
	for _, value := range waves {
		if value.WaveID == run.CurrentWave {
			waveValue = value
			break
		}
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
	for waveIndex := range plan.Waves {
		if len(plan.Waves[waveIndex].Tasks) > 0 {
			continue
		}
		for _, runtime := range tasks {
			if runtime.Task.WaveID == plan.Waves[waveIndex].WaveID {
				plan.Waves[waveIndex].Tasks = append(plan.Waves[waveIndex].Tasks, runtime.Task)
			}
		}
	}
	snapshot := Snapshot{SchemaVersion: SchemaVersion, Run: run, Wave: waveValue, Waves: waves, Tasks: tasks, UpdatedAt: time.Now().UTC()}
	if data, readErr := os.ReadFile(paths.State); readErr == nil {
		var persisted Snapshot
		if json.Unmarshal(data, &persisted) == nil && persisted.Run.RunID == run.RunID && len(persisted.Tasks) == len(tasks) {
			snapshot = persisted
		}
	}
	if len(snapshot.Waves) == 0 {
		snapshot.Waves = waves
	}
	replay, err := event.Replay(paths.Events)
	if err != nil {
		return nil, fmt.Errorf("replay run events: %w", err)
	}
	messageIndex, err := message.Replay(paths.Messages)
	if err != nil {
		return nil, fmt.Errorf("replay messages: %w", err)
	}
	snapshot.Messages = message.Sorted(messageIndex, false)
	var runBaseline verify.WorkspaceSnapshot
	if data, readErr := os.ReadFile(paths.Baseline); readErr == nil {
		_ = json.Unmarshal(data, &runBaseline)
	}
	return &Service{
		runDir: runDir, paths: paths, registry: registry, config: config,
		snapshot: snapshot, events: event.NewStore(paths.Events, string(run.RunID), lastSequence(replay)),
		terminal: make(chan struct{}), recovering: recovering, plan: plan,
		messages: message.NewStore(paths.Messages), messageIndex: messageIndex, pending: map[string]chan message.Resolution{}, active: map[string]activeWorker{}, cancelledTasks: map[string]bool{}, runBaseline: runBaseline,
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
	active := make([]activeWorker, 0, len(s.active))
	for _, worker := range s.active {
		active = append(active, worker)
	}
	s.mu.Unlock()
	for _, worker := range active {
		_ = worker.adapter.InterruptTurn(ctx, worker.sessionID)
		worker := worker
		time.AfterFunc(s.config.CancelGrace, func() {
			_ = worker.adapter.TerminateSession(context.Background(), worker.sessionID)
			worker.cancel()
		})
	}
	s.append(event.Input{Source: "supervisor", Type: "run.cancel_requested", Severity: "warning"})
	return nil
}

func (s *Service) RequestCancelTask(ctx context.Context, taskID string) error {
	s.mu.Lock()
	worker, ok := s.active[taskID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("task %q has no active Worker", taskID)
	}
	s.cancelledTasks[taskID] = true
	s.mu.Unlock()
	_ = worker.adapter.InterruptTurn(ctx, worker.sessionID)
	time.AfterFunc(s.config.CancelGrace, func() {
		_ = worker.adapter.TerminateSession(context.Background(), worker.sessionID)
		worker.cancel()
	})
	s.append(event.Input{TaskID: taskID, Source: "supervisor", Type: "task.cancel_requested", Severity: "warning"})
	return nil
}

func (s *Service) execute(ctx context.Context) error {
	if len(s.snapshot.Tasks) == 0 {
		return s.finishRun(domain.RunFailed, "run has no tasks")
	}
	if s.snapshot.Run.Status == domain.RunCompleted || s.snapshot.Run.Status == domain.RunFailed || s.snapshot.Run.Status == domain.RunCancelled || s.snapshot.Run.Status == domain.RunDegraded {
		return nil
	}
	for ordinal, planned := range s.plan.Waves {
		if s.isCancelled() {
			return s.finishRun(domain.RunCancelled, "run cancelled")
		}
		if s.waveAlreadyVerified(planned.WaveID) {
			continue
		}
		s.selectWave(planned.WaveID)
		if err := s.preflightWave(planned); err != nil {
			return s.finishRun(domain.RunFailed, err.Error())
		}
		baseline, err := verify.CaptureWorkspace(s.projectRoot(), s.config.BrokerHome)
		if err != nil {
			return s.finishRun(domain.RunFailed, err.Error())
		}
		wavePaths := s.wavePaths(planned.WaveID)
		if err := storage.AtomicWriteJSON(wavePaths.Baseline, baseline, 0o600); err != nil {
			return s.finishRun(domain.RunFailed, err.Error())
		}
		s.setWaveStatus(domain.WaveRunning)
		s.append(event.Input{Source: "supervisor", Type: event.WaveStarted, Severity: "info", Payload: map[string]any{"wave_id": planned.WaveID, "ordinal": ordinal + 1}})
		s.executeWave(ctx, planned)
		if s.isCancelled() {
			return s.finishRun(domain.RunCancelled, "run cancelled")
		}
		verification, err := s.runBarrier(ctx, planned, baseline)
		if err != nil {
			return s.finishRun(domain.RunFailed, err.Error())
		}
		if verification.Result == domain.BarrierCancelled {
			return s.finishRun(domain.RunCancelled, "Wave cancelled")
		}
		if verification.Result != domain.BarrierPassed {
			return s.finishRun(domain.RunFailed, "Wave barrier ended with "+string(verification.Result))
		}
		s.setWaveStatus(domain.WaveVerified)
		s.append(event.Input{Source: "supervisor", Type: event.WaveVerified, Severity: "info", Payload: map[string]any{"wave_id": planned.WaveID}})
	}
	final, err := s.runFinalVerification(ctx)
	if err != nil {
		return s.finishRun(domain.RunFailed, err.Error())
	}
	if final.Result != domain.BarrierPassed {
		return s.finishRun(domain.RunFailed, "final verification ended with "+string(final.Result))
	}
	return s.finishRun(domain.RunCompleted, "")
}

func (s *Service) executeWave(ctx context.Context, planned domain.WavePlan) {
	sem := make(chan struct{}, s.config.MaxConcurrency)
	var wg sync.WaitGroup
	for _, taskID := range taskIDs(planned.Tasks) {
		runtime, ok := s.taskState(taskID)
		if !ok || runtime.Task.Status == state.TaskVerifiedSuccess {
			continue
		}
		wg.Add(1)
		go func(runtime TaskState) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			_ = s.executeTask(ctx, &runtime)
			s.saveRuntime(runtime)
		}(runtime)
	}
	wg.Wait()
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
	capabilities := harness.Descriptor().Capabilities
	if s.config.SafeMode {
		capabilities.PermissionEvents = false
		capabilities.Hooks = false
	}
	worker := &domain.WorkerSession{WorkerID: domain.WorkerID(workerID), TaskID: runtime.Task.TaskID, Harness: s.config.Harness, AdapterVersion: harness.Descriptor().AdapterVersion, StartedAt: time.Now().UTC(), LastEventAt: time.Now().UTC(), LastProgressAt: time.Now().UTC(), Capabilities: capabilityMap(capabilities), Attempt: 1, StatusDimensions: state.Dimensions{Process: state.ProcessStarting, Protocol: state.ProtocolInitializing, Progress: state.ProgressActive, Task: state.TaskRunning}}
	resumeSessionID := ""
	if runtime.Worker != nil && runtime.Worker.NativeSessionID != "" && runtime.Dimensions.Process == state.ProcessExited {
		resumeSessionID = runtime.Worker.NativeSessionID
		worker.Attempt = runtime.Worker.Attempt + 1
	}
	runtime.Worker = worker
	runtime.Task.Status = state.TaskRunning
	runtime.Dimensions = worker.StatusDimensions
	runtime.LastProgress = worker.LastProgressAt
	s.setRunStatus(domain.RunRunning, "")
	s.saveRuntime(*runtime)
	s.append(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: workerID, Source: "supervisor", Type: event.SessionStarting, Severity: "info"})

	options := map[string]string{"permission_mode": s.config.PermissionMode, "max_turns": fmt.Sprintf("%d", s.config.MaxTurns), "scenario": s.config.Scenario}
	if s.config.SafeMode {
		options["safe_mode"] = "true"
	}
	workerCtx, cancel := context.WithCancel(parent)
	if s.config.HardTimeout > 0 {
		workerCtx, cancel = context.WithTimeout(parent, s.config.HardTimeout)
	}
	model := s.config.Model
	if runtime.Task.ModelPreference != "" {
		model = runtime.Task.ModelPreference
	}
	prompt := buildWorkerPrompt(runtime.Task, s.snapshot.Run.RunID, workerID)
	if runtime.PendingInstruction != "" {
		prompt += "\n\nMain Agent follow-up instruction:\n" + runtime.PendingInstruction
		runtime.PendingInstruction = ""
	}
	executable, _ := os.Executable()
	interaction := adapter.InteractionConfig{Enabled: !s.config.SafeMode && s.config.Harness == string(adapter.HarnessClaudeCode), BrokerExecutable: executable, RunDir: s.runDir}
	request := adapter.StartRequest{RunID: string(s.snapshot.Run.RunID), TaskID: string(runtime.Task.TaskID), WorkerID: workerID, ProjectRoot: runtime.Task.ProjectRoot, Contract: prompt, Model: model, Scenario: s.config.Scenario, Options: options, Interaction: interaction}
	var session adapter.Session
	var err error
	if resumeSessionID != "" {
		worker.NativeSessionID = resumeSessionID
		session, err = harness.ResumeSession(workerCtx, adapter.ResumeRequest{NativeSessionID: resumeSessionID, RunID: request.RunID, TaskID: request.TaskID, WorkerID: request.WorkerID, ProjectRoot: request.ProjectRoot, Contract: request.Contract, Model: request.Model, Options: options, Interaction: interaction})
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
	s.active[string(runtime.Task.TaskID)] = activeWorker{adapter: harness, sessionID: session.NativeSessionID, cancel: cancel}
	s.mu.Unlock()
	worker.NativeSessionID = session.NativeSessionID
	worker.NativeTurnID = session.NativeTurnID
	worker.PID = session.PID
	worker.ProcessStartToken = session.ProcessStartToken
	worker.StatusDimensions.Process = state.ProcessAlive
	runtime.Dimensions = worker.StatusDimensions
	s.saveRuntime(*runtime)
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
	s.saveRuntime(*runtime)
	cancel()
	s.mu.Lock()
	delete(s.active, string(runtime.Task.TaskID))
	s.mu.Unlock()

	s.mu.Lock()
	cancelled := s.cancelled
	s.mu.Unlock()
	if cancelled {
		return s.cancelTask(runtime, "cancelled by main agent")
	}
	if s.isTaskCancelled(string(runtime.Task.TaskID)) {
		return s.cancelTask(runtime, "Task cancelled by main agent")
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
	if latest, ok := s.taskState(runtime.Task.TaskID); ok {
		runtime.Task.WriteScope = append([]string(nil), latest.Task.WriteScope...)
		runtime.Task.AllowPublicInterfaceChange = latest.Task.AllowPublicInterfaceChange
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
	line := append(append([]byte(nil), native.Payload...), '\n')
	_ = appendFile(filepath.Join(s.taskDir(runtime.Task), "stdout.log"), line)
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
	s.saveRuntime(*runtime)
}

func (s *Service) updateProgress(runtime *TaskState, now time.Time) {
	worker := runtime.Worker
	if worker == nil || s.taskHasPendingMessage(string(runtime.Task.TaskID)) || state.IsWaiting(worker.StatusDimensions.Protocol) || worker.LastProgressAt.IsZero() {
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
	s.saveRuntime(*runtime)
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
	s.saveRuntime(*runtime)
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

func (s *Service) preflightWave(planned domain.WavePlan) error {
	result := wave.Preflight(planned.Tasks)
	paths := s.wavePaths(planned.WaveID)
	if err := storage.AtomicWriteJSON(paths.Preflight, result, 0o600); err != nil {
		return err
	}
	if !result.Allowed {
		return fmt.Errorf("Wave %q preflight rejected: %s", planned.WaveID, formatPreflightIssues(result.Issues))
	}
	return nil
}

func (s *Service) runBarrier(ctx context.Context, planned domain.WavePlan, baseline verify.WorkspaceSnapshot) (wave.Verification, error) {
	started := time.Now().UTC()
	s.setRunStatus(domain.RunBarrier, "")
	s.setWaveStatus(domain.WaveBarrier)
	s.append(event.Input{Source: "supervisor", Type: event.WaveBarrierStarted, Severity: "info", Payload: map[string]any{"wave_id": planned.WaveID}})
	verification := wave.Verification{SchemaVersion: SchemaVersion, WaveID: planned.WaveID, StartedAt: started}
	cancelled := false
	for _, item := range planned.Tasks {
		runtime, ok := s.taskState(item.TaskID)
		if !ok {
			verification.Errors = append(verification.Errors, "missing task state: "+string(item.TaskID))
			continue
		}
		switch runtime.Task.Status {
		case state.TaskVerifiedSuccess:
		case state.TaskCancelled:
			cancelled = true
		case state.TaskVerifiedPartial, state.TaskBlocked, state.TaskReportedComplete:
			verification.Errors = append(verification.Errors, fmt.Sprintf("task %s is %s", item.TaskID, runtime.Task.Status))
		default:
			verification.Errors = append(verification.Errors, fmt.Sprintf("task %s failed to verify: %s", item.TaskID, runtime.Task.Status))
		}
	}
	for index, check := range planned.IntegrationChecks {
		result := s.runCheck(ctx, check, filepath.Join(s.wavePaths(planned.WaveID).Root, fmt.Sprintf("check-%03d.log", index+1)))
		verification.Checks = append(verification.Checks, result)
		if !result.Passed {
			verification.Errors = append(verification.Errors, "integration check failed: "+check.Command)
		}
	}
	after, err := verify.CaptureWorkspace(s.projectRoot(), s.config.BrokerHome)
	if err != nil {
		return verification, err
	}
	verification.ChangedFiles = verify.ChangedFiles(baseline, after)
	leases := map[string][]string{}
	for _, item := range planned.Tasks {
		runtime, ok := s.taskState(item.TaskID)
		if ok {
			leases[string(item.TaskID)] = append([]string(nil), runtime.Task.WriteScope...)
		}
	}
	verification.ScopeAudit, err = verify.AuditScopes(verification.ChangedFiles, leases)
	if err != nil {
		return verification, err
	}
	for _, path := range verification.ScopeAudit.Unauthorized {
		verification.Errors = append(verification.Errors, "unauthorized file: "+path)
	}
	verification.Result = domain.BarrierPassed
	if cancelled {
		verification.Result = domain.BarrierCancelled
	} else if len(verification.Errors) > 0 {
		verification.Result = domain.BarrierFailed
	}
	if len(verification.Warnings) > 0 && verification.Result == domain.BarrierPassed {
		verification.Result = domain.BarrierPassedWithWarnings
	}
	verification.EndedAt = time.Now().UTC()
	paths := s.wavePaths(planned.WaveID)
	if err := storage.AtomicWriteJSON(paths.Verification, verification, 0o600); err != nil {
		return verification, err
	}
	if err := storage.AtomicWriteFile(paths.Barrier, []byte(wave.RenderBarrier(verification)), 0o600); err != nil {
		return verification, err
	}
	s.mu.Lock()
	for index := range s.snapshot.Waves {
		if s.snapshot.Waves[index].WaveID == planned.WaveID {
			s.snapshot.Waves[index].BarrierResult = verification.Result
			s.snapshot.Wave = s.snapshot.Waves[index]
			break
		}
	}
	_ = s.saveLocked()
	s.mu.Unlock()
	return verification, nil
}

func (s *Service) runCheck(ctx context.Context, check domain.ValidationCommand, logPath string) wave.CheckResult {
	checkCtx, cancel := context.WithTimeout(ctx, s.config.ValidationTimeout)
	defer cancel()
	command := exec.CommandContext(checkCtx, "sh", "-c", check.Command)
	command.Dir = s.projectRoot()
	output, err := command.CombinedOutput()
	_ = appendFile(logPath, output)
	details := strings.TrimSpace(string(output))
	if err != nil && details == "" {
		details = err.Error()
	}
	return wave.CheckResult{Command: check.Command, Passed: err == nil, Details: details}
}

func (s *Service) runFinalVerification(ctx context.Context) (wave.Verification, error) {
	value := wave.Verification{SchemaVersion: SchemaVersion, WaveID: "run-final", StartedAt: time.Now().UTC(), Result: domain.BarrierPassed}
	for index, check := range s.plan.FinalChecks {
		result := s.runCheck(ctx, check, filepath.Join(s.runDir, fmt.Sprintf("final-check-%03d.log", index+1)))
		value.Checks = append(value.Checks, result)
		if !result.Passed {
			value.Errors = append(value.Errors, "final check failed: "+check.Command)
		}
	}
	after, err := verify.CaptureWorkspace(s.projectRoot(), s.config.BrokerHome)
	if err != nil {
		return value, err
	}
	value.ChangedFiles = verify.ChangedFiles(s.runBaseline, after)
	leases := map[string][]string{}
	s.mu.Lock()
	for _, runtime := range s.snapshot.Tasks {
		leases[string(runtime.Task.TaskID)] = append([]string(nil), runtime.Task.WriteScope...)
	}
	s.mu.Unlock()
	value.ScopeAudit, err = verify.AuditScopes(value.ChangedFiles, leases)
	if err != nil {
		return value, err
	}
	for _, path := range value.ScopeAudit.Unauthorized {
		value.Errors = append(value.Errors, "unauthorized file: "+path)
	}
	if len(value.Errors) > 0 {
		value.Result = domain.BarrierFailed
	}
	value.EndedAt = time.Now().UTC()
	if err := storage.AtomicWriteJSON(s.paths.Verification, value, 0o600); err != nil {
		return value, err
	}
	return value, nil
}

func (s *Service) taskState(taskID domain.TaskID) (TaskState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, runtime := range s.snapshot.Tasks {
		if runtime.Task.TaskID == taskID {
			return cloneTaskState(runtime), true
		}
	}
	return TaskState{}, false
}

func (s *Service) saveRuntime(runtime TaskState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index := range s.snapshot.Tasks {
		if s.snapshot.Tasks[index].Task.TaskID == runtime.Task.TaskID {
			runtime.Task.WriteScope = append([]string(nil), s.snapshot.Tasks[index].Task.WriteScope...)
			runtime.Task.AllowPublicInterfaceChange = s.snapshot.Tasks[index].Task.AllowPublicInterfaceChange
			for _, pending := range s.messageIndex {
				if pending.TaskID == string(runtime.Task.TaskID) && pending.Status != message.Answered && pending.Status != message.Failed && pending.Status != message.Expired {
					runtime.Task.Status = state.TaskBlocked
					runtime.Dimensions.Task = state.TaskBlocked
					runtime.Dimensions.Progress = state.ProgressQuiet
					switch pending.Type {
					case message.ScopeExpansionRequest:
						runtime.Dimensions.Protocol = state.ProtocolWaitingScope
					case message.PermissionRequest:
						runtime.Dimensions.Protocol = state.ProtocolWaitingPermission
					default:
						runtime.Dimensions.Protocol = state.ProtocolWaitingUser
					}
					break
				}
			}
			s.snapshot.Tasks[index] = cloneTaskState(runtime)
			break
		}
	}
	_ = s.saveLocked()
}

func (s *Service) taskHasPendingMessage(taskID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, pending := range s.messageIndex {
		if pending.TaskID == taskID && pending.Status != message.Answered && pending.Status != message.Failed && pending.Status != message.Expired {
			return true
		}
	}
	return false
}

func cloneTaskState(source TaskState) TaskState {
	data, err := json.Marshal(source)
	if err != nil {
		return source
	}
	var result TaskState
	if json.Unmarshal(data, &result) != nil {
		return source
	}
	return result
}

func (s *Service) selectWave(id domain.WaveID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Run.CurrentWave = id
	for _, value := range s.snapshot.Waves {
		if value.WaveID == id {
			s.snapshot.Wave = value
			break
		}
	}
	_ = s.saveLocked()
}

func (s *Service) waveAlreadyVerified(id domain.WaveID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, value := range s.snapshot.Waves {
		if value.WaveID == id {
			return value.Status == domain.WaveVerified
		}
	}
	return false
}

func (s *Service) projectRoot() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.snapshot.Tasks) == 0 {
		return ""
	}
	return s.snapshot.Tasks[0].Task.ProjectRoot
}

func (s *Service) wavePaths(id domain.WaveID) storage.WavePaths {
	layout, _ := storage.NewLayout(s.config.BrokerHome)
	paths, _ := layout.WavePaths(string(s.snapshot.Run.ProjectID), string(s.snapshot.Run.RunID), string(id))
	return paths
}

func taskIDs(tasks []domain.Task) []domain.TaskID {
	result := make([]domain.TaskID, 0, len(tasks))
	for _, item := range tasks {
		result = append(result, item.TaskID)
	}
	return result
}

func capabilityMap(value adapter.Capabilities) map[string]bool {
	return map[string]bool{
		"structured_stream": value.StructuredStream, "bidirectional_stream": value.BidirectionalStream,
		"resume_session": value.ResumeSession, "steer_active_turn": value.SteerActiveTurn,
		"interrupt_turn": value.InterruptTurn, "structured_final_output": value.StructuredFinalOutput,
		"permission_events": value.PermissionEvents, "diff_events": value.DiffEvents,
		"usage_events": value.UsageEvents, "hooks": value.Hooks, "session_history": value.SessionHistory,
	}
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
	s.saveRuntime(*runtime)
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
	s.saveRuntime(*runtime)
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
	s.saveRuntime(*runtime)
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

func (s *Service) isTaskCancelled(taskID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelledTasks[taskID]
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
	for index := range s.snapshot.Waves {
		if s.snapshot.Waves[index].WaveID == s.snapshot.Wave.WaveID {
			s.snapshot.Waves[index] = s.snapshot.Wave
			break
		}
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
	for _, value := range s.snapshot.Waves {
		paths := s.wavePaths(value.WaveID)
		if err := storage.AtomicWriteJSON(paths.Wave, value, 0o600); err != nil {
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
	return contract + fmt.Sprintf("\n\nSupervisor identities: task_id=%s, worker_id=%s. Use the subagent-broker MCP tool ask_main_agent for a blocking question. Use request_scope_expansion before every out-of-scope edit and wait for its decision. Do not use the built-in AskUserQuestion tool. Return the final Result Envelope as one JSON object only. Do not wrap it in Markdown fences or add commentary. The JSON must use schema_version v1alpha1 and these exact identities.", item.TaskID, workerID)
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
