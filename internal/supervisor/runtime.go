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
	workerpkg "github.com/vnai/subagent-broker/internal/worker"
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

// BlockKind distinguishes temporary waiting from a final blocked result.
type BlockKind string

const (
	BlockKindNone           BlockKind = ""
	BlockKindWaitingMessage BlockKind = "waiting_message"
	BlockKindFinal          BlockKind = "final"
)

type TaskState struct {
	Task          domain.Task           `json:"task"`
	Worker        *domain.WorkerSession `json:"worker,omitempty"` // current/active attempt projection (compat)
	Attempts      []workerpkg.Attempt   `json:"attempts,omitempty"`
	ActiveAttempt int                   `json:"active_attempt,omitempty"` // attempt number; 0 = none
	BlockKind     BlockKind             `json:"block_kind,omitempty"`
	Dimensions    state.Dimensions      `json:"status_dimensions"`
	ReportPath    string                `json:"report_path,omitempty"`
	Validation    []ValidationResult    `json:"validation,omitempty"`
	LastError     string                `json:"last_error,omitempty"`
	LastProgress  time.Time             `json:"last_progress_at,omitempty"`
	// Deprecated: PendingInstruction is retained only for JSON compatibility with
	// older snapshots. It is not an outbox; durable instructions use message.Router.
	PendingInstruction string `json:"pending_instruction,omitempty"`
}

type Snapshot struct {
	SchemaVersion   string            `json:"schema_version"`
	Run             domain.Run        `json:"run"`
	Wave            domain.Wave       `json:"wave"`
	Waves           []domain.Wave     `json:"waves,omitempty"`
	Tasks           []TaskState       `json:"tasks"`
	Messages        []message.Message `json:"messages,omitempty"`
	UpdatedAt       time.Time         `json:"updated_at"`
	LastError       string            `json:"last_error,omitempty"`
	AppliedEventSeq uint64            `json:"applied_event_seq"`
}

// eventAppender is the internal write boundary for run events. Tests may inject
// a fake implementation; production uses *event.Store.
type eventAppender interface {
	Append(event.Input) (event.Event, error)
}

type Service struct {
	mu             sync.Mutex
	runDir         string
	paths          storage.RunPaths
	registry       *adapter.Registry
	config         Config
	snapshot       Snapshot
	events         eventAppender
	listener       net.Listener
	terminal       chan struct{}
	cancelled      bool
	cancelledTasks map[string]bool
	recovering     bool
	plan           domain.RunPlan
	messages       *message.Store
	messageIndex   map[string]message.Message
	router         *message.Router
	pending        map[string]chan message.Resolution
	active         map[string]activeWorker
	runBaseline    verify.WorkspaceSnapshot

	// Commit fail-closed control plane.
	fatalPersistence chan error
	acceptingWork    bool

	// persistSnapshotFn overrides disk persistence for tests. When nil,
	// persistSnapshotLocked writes the multi-file Snapshot projection.
	persistSnapshotFn func(value Snapshot) error
}

type activeWorker struct {
	adapter   adapter.Adapter
	sessionID string
	cancel    context.CancelFunc
	identity  process.Identity
	taskID    string
	workerID  string
	attempt   int
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
	// 1) Build base snapshot from run+plan (+task files).
	base := Snapshot{SchemaVersion: SchemaVersion, Run: run, Wave: waveValue, Waves: waves, Tasks: tasks, UpdatedAt: time.Now().UTC()}
	// 2) Load checkpoint if present.
	checkpoint := base
	hasCheckpoint := false
	if data, readErr := os.ReadFile(paths.State); readErr == nil {
		var persisted Snapshot
		if err := json.Unmarshal(data, &persisted); err != nil {
			return nil, fmt.Errorf("decode state.json: %w", err)
		}
		if persisted.Run.RunID != "" && persisted.Run.RunID != run.RunID {
			return nil, fmt.Errorf("state.json run_id %q does not match run.json %q", persisted.Run.RunID, run.RunID)
		}
		if len(persisted.Tasks) > 0 {
			checkpoint = persisted
			hasCheckpoint = true
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return nil, fmt.Errorf("read state.json: %w", readErr)
	}
	if len(checkpoint.Waves) == 0 {
		checkpoint.Waves = waves
	}
	// 3) Replay repairable event journal.
	replay, err := event.Replay(paths.Events)
	if err != nil {
		return nil, fmt.Errorf("replay run events: %w", err)
	}
	lastSeq := lastSequence(replay)
	if hasCheckpoint && checkpoint.AppliedEventSeq > lastSeq {
		return nil, fmt.Errorf("snapshot applied_event_seq %d is ahead of event log last seq %d", checkpoint.AppliedEventSeq, lastSeq)
	}
	// 4) Apply events after checkpoint.
	snapshot, err := ReplayEvents(checkpoint, replay.Events)
	if err != nil {
		return nil, fmt.Errorf("reduce events onto snapshot: %w", err)
	}
	// Migrate legacy single-Worker history into Attempts.
	for i := range snapshot.Tasks {
		migrateAttempts(&snapshot.Tasks[i])
	}
	// 5) Persist caught-up checkpoint when events advanced the state.
	if snapshot.AppliedEventSeq != checkpoint.AppliedEventSeq || !hasCheckpoint {
		if err := storage.AtomicWriteJSON(paths.State, snapshot, 0o600); err != nil {
			return nil, fmt.Errorf("write recovered state.json: %w", err)
		}
	}
	messageIndex, err := message.Replay(paths.Messages)
	if err != nil {
		return nil, fmt.Errorf("replay messages: %w", err)
	}
	snapshot.Messages = message.Sorted(messageIndex, false)
	messageStore := message.NewStore(paths.Messages)
	router, err := message.NewRouter(message.NewRouterOptions{
		RunID:   string(run.RunID),
		Store:   messageStore,
		Initial: messageIndex,
	})
	if err != nil {
		return nil, fmt.Errorf("init message router: %w", err)
	}
	var runBaseline verify.WorkspaceSnapshot
	if data, readErr := os.ReadFile(paths.Baseline); readErr == nil {
		if err := json.Unmarshal(data, &runBaseline); err != nil {
			return nil, fmt.Errorf("decode baseline: %w", err)
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return nil, fmt.Errorf("read baseline: %w", readErr)
	}
	return &Service{
		runDir: runDir, paths: paths, registry: registry, config: config,
		snapshot: snapshot, events: event.NewStore(paths.Events, string(run.RunID), lastSeq),
		terminal: make(chan struct{}), recovering: recovering, plan: plan,
		messages: messageStore, messageIndex: messageIndex, router: router, pending: map[string]chan message.Resolution{}, active: map[string]activeWorker{}, cancelledTasks: map[string]bool{}, runBaseline: runBaseline,
		fatalPersistence: make(chan error, 1), acceptingWork: true,
	}, nil
}

func (s *Service) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := s.prepareIPC(); err != nil {
		return err
	}
	if err := s.writeSupervisorIdentity(""); err != nil {
		return err
	}
	go s.watchPersistenceFailures(cancel)
	go s.heartbeat(ctx)
	if s.recovering {
		if err := s.reconcileRecovery(ctx); err != nil {
			return err
		}
	} else {
		if err := s.setRunStatus(domain.RunStarting, ""); err != nil {
			return err
		}
		if err := s.appendEvent(event.Input{Source: "supervisor", Type: event.RunStarted, Severity: "info"}); err != nil {
			return err
		}
	}
	go s.serveIPC(ctx)
	err := s.execute(ctx)
	if err == nil && !s.AcceptingWork() {
		err = fmt.Errorf("supervisor stopped after a fatal persistence failure")
	}
	s.mu.Lock()
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	_ = os.Remove(SocketPath(s.runDir))
	status := s.snapshot.Run.Status
	s.mu.Unlock()
	// Best-effort final identity write; do not fail-close again if already closed.
	_ = s.writeSupervisorIdentity(shutdownReason(err, status))
	close(s.terminal)
	return err
}

func (s *Service) watchPersistenceFailures(cancel context.CancelFunc) {
	err, ok := <-s.FatalPersistenceErrors()
	if !ok || err == nil {
		return
	}
	s.cancelAllActiveWorkers()
	cancel()
}

func (s *Service) Terminal() <-chan struct{} { return s.terminal }

func (s *Service) Initialize() error {
	return s.commitMutate(context.Background(), event.Input{
		Source: "dispatch", Type: event.RunCreated, Severity: "info",
		Payload: map[string]any{"reason": "initialize"},
	}, func(candidate *Snapshot) error {
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
}

func (s *Service) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy, err := cloneSnapshot(s.snapshot)
	if err != nil {
		// Snapshot is always JSON-serializable in normal operation; return a
		// zero value rather than a shared mutable alias on rare marshal failure.
		return Snapshot{}
	}
	return copy
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
	_ = s.appendEvent(event.Input{Source: "supervisor", Type: event.CancelTreeRequested, Severity: "warning", Payload: map[string]any{"scope": "run"}})
	var firstErr error
	degraded := false
	for _, worker := range active {
		result, err := s.terminateActiveWorker(ctx, worker)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if !result.TreeExited && !result.PIDReused {
			degraded = true
		}
	}
	_ = s.appendEvent(event.Input{Source: "supervisor", Type: event.CancelTreeCompleted, Severity: "warning", Payload: map[string]any{"scope": "run", "degraded": degraded}})
	if degraded {
		_ = s.setRunStatus(domain.RunDegraded, "cancel completed with residual worker processes")
	}
	return firstErr
}

func (s *Service) RequestCancelTask(ctx context.Context, taskID string) error {
	s.mu.Lock()
	if s.cancelledTasks[taskID] {
		worker, ok := s.active[taskID]
		s.mu.Unlock()
		if !ok {
			return nil // idempotent: already cancelled and inactive
		}
		_, err := s.terminateActiveWorker(ctx, worker)
		return err
	}
	worker, ok := s.active[taskID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("task %q has no active Worker", taskID)
	}
	s.cancelledTasks[taskID] = true
	s.mu.Unlock()
	_ = s.appendEvent(event.Input{TaskID: taskID, Source: "supervisor", Type: event.CancelTreeRequested, Severity: "warning", Payload: map[string]any{"scope": "task"}})
	result, err := s.terminateActiveWorker(ctx, worker)
	_ = s.appendEvent(event.Input{TaskID: taskID, Source: "supervisor", Type: event.CancelTreeCompleted, Severity: "warning", Payload: map[string]any{
		"tree_exited": result.TreeExited, "pid_reused": result.PIDReused, "remaining": result.RemainingPIDs,
	}})
	if err == nil && !result.TreeExited && !result.PIDReused {
		_ = s.setRunStatus(domain.RunDegraded, "task cancel left residual processes")
	}
	return err
}

func (s *Service) terminateActiveWorker(ctx context.Context, worker activeWorker) (process.TerminationResult, error) {
	// Protocol interrupt first (best effort).
	_ = worker.adapter.InterruptTurn(ctx, worker.sessionID)
	policy := defaultCancelPolicy(s.config.CancelGrace)
	var result process.TerminationResult
	var err error
	if worker.identity.Complete() {
		result, err = process.Controller{Manager: process.PlatformManager{}}.TerminateTree(ctx, worker.identity, policy)
	} else {
		// Fall back to adapter terminate when OS identity is incomplete.
		_ = worker.adapter.TerminateSession(context.Background(), worker.sessionID)
		result.TermSent = true
		result.TreeExited = true
	}
	if worker.cancel != nil {
		worker.cancel()
	}
	return result, err
}

func (s *Service) execute(ctx context.Context) error {
	if len(s.Snapshot().Tasks) == 0 {
		return s.finishRun(domain.RunFailed, "run has no tasks")
	}
	status := s.Snapshot().Run.Status
	if status == domain.RunCompleted || status == domain.RunFailed || status == domain.RunCancelled || status == domain.RunDegraded {
		return nil
	}
	for ordinal, planned := range s.plan.Waves {
		if !s.AcceptingWork() {
			return fmt.Errorf("supervisor is not accepting work after a fatal persistence failure")
		}
		if s.isCancelled() || ctx.Err() != nil {
			return s.finishRun(domain.RunCancelled, "run cancelled")
		}
		if s.waveAlreadyVerified(planned.WaveID) {
			continue
		}
		if err := s.selectWave(planned.WaveID); err != nil {
			return err
		}
		if err := s.preflightWave(ctx, planned); err != nil {
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
		if err := s.setWaveStatus(domain.WaveRunning); err != nil {
			return err
		}
		if err := s.appendEvent(event.Input{Source: "supervisor", Type: event.WaveStarted, Severity: "info", Payload: map[string]any{"wave_id": planned.WaveID, "ordinal": ordinal + 1}}); err != nil {
			return err
		}
		if err := s.executeWave(ctx, planned); err != nil {
			return err
		}
		if s.isCancelled() || ctx.Err() != nil {
			return s.finishRun(domain.RunCancelled, "run cancelled")
		}
		if !s.AcceptingWork() {
			return fmt.Errorf("supervisor is not accepting work after a fatal persistence failure")
		}
		verification, err := s.runBarrier(ctx, planned, baseline)
		if err != nil {
			return s.finishRun(domain.RunFailed, err.Error())
		}
		switch verification.Result {
		case domain.BarrierCancelled:
			return s.finishRun(domain.RunCancelled, "Wave cancelled")
		case domain.BarrierFailed:
			return s.finishRun(domain.RunFailed, "Wave barrier failed")
		case domain.BarrierBlocked:
			// Blocked is not a hard Run failure; stop progression until Main Agent unblocks.
			return s.finishRun(domain.RunDegraded, "Wave barrier blocked: pending decisions or final-blocked tasks")
		case domain.BarrierPassedWithWarnings:
			// Do not auto-continue; require AcceptBarrierWarnings before next Wave.
			return s.finishRun(domain.RunDegraded, "Wave barrier passed_with_warnings; acceptance required before continuing")
		case domain.BarrierPassed:
			// Wave already marked verified inside runBarrier.
			if err := s.appendEvent(event.Input{Source: "supervisor", Type: event.WaveVerified, Severity: "info", Payload: map[string]any{"wave_id": planned.WaveID}}); err != nil {
				return err
			}
		default:
			return s.finishRun(domain.RunFailed, "Wave barrier ended with "+string(verification.Result))
		}
	}
	final, err := s.runFinalVerification(ctx)
	if err != nil {
		return s.finishRun(domain.RunFailed, err.Error())
	}
	if final.Result != domain.BarrierPassed && final.Result != domain.BarrierPassedWithWarnings {
		return s.finishRun(domain.RunFailed, "final verification ended with "+string(final.Result))
	}
	if final.Result == domain.BarrierPassedWithWarnings {
		return s.finishRun(domain.RunDegraded, "final verification has warnings requiring acceptance")
	}
	// Ensure every wave is verified (or accepted) before completed.
	for _, w := range s.Snapshot().Waves {
		if w.Status != domain.WaveVerified {
			return s.finishRun(domain.RunDegraded, fmt.Sprintf("wave %s is %s, not verified", w.WaveID, w.Status))
		}
		if w.BarrierResult == domain.BarrierPassedWithWarnings && !w.BarrierAccepted {
			return s.finishRun(domain.RunDegraded, fmt.Sprintf("wave %s warnings not accepted", w.WaveID))
		}
	}
	return s.finishRun(domain.RunCompleted, "")
}

func (s *Service) executeWave(ctx context.Context, planned domain.WavePlan) error {
	if !s.AcceptingWork() {
		return fmt.Errorf("supervisor is not accepting work after a fatal persistence failure")
	}
	sem := make(chan struct{}, s.config.MaxConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, taskID := range taskIDs(planned.Tasks) {
		runtime, ok := s.taskState(taskID)
		// Never re-run terminal tasks or orphaned workers (no second concurrent Worker).
		if !ok || recoveryTaskTerminal(runtime) || runtime.Dimensions.Process == state.ProcessOrphaned {
			continue
		}
		if !s.AcceptingWork() {
			break
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
			if err := s.executeTask(ctx, &runtime); err != nil {
				mu.Lock()
				if firstErr == nil && !s.AcceptingWork() {
					firstErr = err
				}
				mu.Unlock()
			}
			_ = s.saveRuntime(runtime)
		}(runtime)
	}
	wg.Wait()
	if !s.AcceptingWork() {
		if firstErr != nil {
			return firstErr
		}
		return fmt.Errorf("supervisor is not accepting work after a fatal persistence failure")
	}
	return nil
}

func (s *Service) handleNative(runtime *TaskState, harness adapter.Adapter, native adapter.NativeEvent, workerID string) {
	input, err := harness.NormalizeEvent(native)
	if err != nil {
		input = event.Input{Timestamp: native.Timestamp, Source: "supervisor", Type: "protocol.error", Severity: "error", Payload: map[string]string{"error": err.Error()}}
	}
	input.TaskID = string(runtime.Task.TaskID)
	input.WorkerID = workerID
	_ = s.appendEvent(input)
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
	case event.TurnCompleted:
		worker.StatusDimensions.Protocol = state.ProtocolIdleBetweenTurns
	}
	runtime.Dimensions = worker.StatusDimensions
	_ = s.saveRuntime(*runtime)
	if native.Kind == event.TurnCompleted || native.Kind == event.ResultSubmitted ||
		worker.StatusDimensions.Protocol == state.ProtocolIdleBetweenTurns {
		_ = s.FlushInstructionOutbox(context.Background(), string(runtime.Task.TaskID), "turn_boundary")
	}
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
	from := worker.StatusDimensions.Progress
	if err := state.ValidateProgressTransition(from, desired); err != nil {
		return
	}
	worker.StatusDimensions.Progress = desired
	runtime.Dimensions = worker.StatusDimensions
	_ = s.saveRuntime(*runtime)
	_ = s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: string(worker.WorkerID), Source: "supervisor", Type: event.ProgressStateChanged, Severity: "warning", Payload: map[string]any{"from": string(from), "to": string(desired), "quiet_for": elapsed.String(), "reason": "stall_watch"}})
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
	_ = s.saveRuntime(*runtime)
	return allPassed
}

func (s *Service) preflightWave(ctx context.Context, planned domain.WavePlan) error {
	tasks := append([]domain.Task(nil), planned.Tasks...)
	for index := range tasks {
		if tasks[index].HarnessPreference == "" {
			tasks[index].HarnessPreference = s.config.Harness
		}
	}
	// Environment preflight probes each unique harness once via the registry.
	result := wave.EvaluatePreflight(ctx, tasks, wave.PreflightEnvironment{
		Registry:     s.registry,
		Executable:   s.config.Executable,
		ProbeTimeout: 10 * time.Second,
	})
	paths := s.wavePaths(planned.WaveID)
	if err := storage.AtomicWriteJSON(paths.Preflight, result, 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteFile(filepath.Join(paths.Root, "preflight.md"), []byte(wave.RenderPreflightMarkdown(result)), 0o600); err != nil {
		return err
	}
	if !result.Allowed {
		return fmt.Errorf("Wave %q preflight rejected: %s", planned.WaveID, formatPreflightIssues(result.Issues))
	}
	return nil
}

func (s *Service) runBarrier(ctx context.Context, planned domain.WavePlan, baseline verify.WorkspaceSnapshot) (wave.Verification, error) {
	started := time.Now().UTC()
	if err := s.setRunStatus(domain.RunBarrier, ""); err != nil {
		return wave.Verification{}, err
	}
	if err := s.setWaveStatus(domain.WaveBarrier); err != nil {
		return wave.Verification{}, err
	}
	if err := s.appendEvent(event.Input{Source: "supervisor", Type: event.WaveBarrierStarted, Severity: "info", Payload: map[string]any{"wave_id": planned.WaveID}}); err != nil {
		return wave.Verification{}, err
	}

	input, err := s.collectBarrierInputs(ctx, planned, baseline)
	if err != nil {
		return wave.Verification{}, err
	}
	input.ExistingErrors = append(input.ExistingErrors, s.collectQueuedInstructionErrors(planned)...)
	inputHash := hashBarrierInputs(input)
	if err := storage.AtomicWriteJSON(filepath.Join(s.wavePaths(planned.WaveID).Root, "barrier-input.json"), input, 0o600); err != nil {
		return wave.Verification{}, err
	}

	verification := wave.EvaluateBarrier(input, time.Now().UTC())
	verification.SchemaVersion = SchemaVersion
	verification.StartedAt = started
	verification.InputHash = inputHash

	paths := s.wavePaths(planned.WaveID)
	if err := storage.AtomicWriteJSON(paths.Verification, verification, 0o600); err != nil {
		return verification, err
	}
	if err := storage.AtomicWriteFile(paths.Barrier, []byte(wave.RenderBarrier(verification)), 0o600); err != nil {
		return verification, err
	}
	_ = storage.AtomicWriteFile(filepath.Join(paths.Root, "verification.md"), []byte(wave.RenderBarrier(verification)), 0o600)

	waveStatus := domain.WaveBarrier
	switch verification.Result {
	case domain.BarrierPassed:
		waveStatus = domain.WaveVerified
	case domain.BarrierPassedWithWarnings:
		waveStatus = domain.WaveWaiting
	case domain.BarrierBlocked:
		waveStatus = domain.WaveBlocked
	case domain.BarrierFailed:
		waveStatus = domain.WaveFailed
	case domain.BarrierCancelled:
		waveStatus = domain.WaveCancelled
	}
	if err := s.commitMutate(ctx, event.Input{
		Source: "supervisor", Type: event.WaveStateChanged, Severity: "info",
		Payload: map[string]any{
			"wave_id":        string(planned.WaveID),
			"from":           string(domain.WaveBarrier),
			"to":             string(waveStatus),
			"reason":         "barrier_result",
			"barrier_result": string(verification.Result),
			"input_hash":     inputHash,
		},
	}, func(candidate *Snapshot) error {
		for index := range candidate.Waves {
			if candidate.Waves[index].WaveID == planned.WaveID {
				candidate.Waves[index].BarrierResult = verification.Result
				candidate.Waves[index].Status = waveStatus
				if waveStatus == domain.WaveVerified || waveStatus == domain.WaveFailed || waveStatus == domain.WaveCancelled {
					now := time.Now().UTC()
					candidate.Waves[index].EndedAt = &now
				}
				candidate.Wave = candidate.Waves[index]
				break
			}
		}
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	}); err != nil {
		return verification, err
	}
	_ = s.appendEvent(event.Input{Source: "supervisor", Type: "barrier.evaluated", Severity: "info", Payload: map[string]any{
		"wave_id": string(planned.WaveID), "result": string(verification.Result), "input_hash": inputHash,
	}})
	return verification, nil
}

// AcceptBarrierWarnings records formal acceptance of passed_with_warnings for a Wave.
func (s *Service) AcceptBarrierWarnings(waveID, actor, reason string) error {
	if strings.TrimSpace(actor) == "" || strings.TrimSpace(reason) == "" {
		return fmt.Errorf("actor and reason are required to accept barrier warnings")
	}
	paths := s.wavePaths(domain.WaveID(waveID))
	data, err := os.ReadFile(paths.Verification)
	if err != nil {
		return fmt.Errorf("read verification for %s: %w", waveID, err)
	}
	var verification wave.Verification
	if err := json.Unmarshal(data, &verification); err != nil {
		return err
	}
	if verification.Result != domain.BarrierPassedWithWarnings {
		return fmt.Errorf("wave %s barrier result is %s, not passed_with_warnings", waveID, verification.Result)
	}
	// Recompute current input hash; acceptance binds to the stored verification input_hash.
	if verification.InputHash == "" {
		return fmt.Errorf("wave %s verification lacks input_hash; re-run barrier", waveID)
	}
	inputData, err := os.ReadFile(filepath.Join(paths.Root, "barrier-input.json"))
	if err != nil {
		return fmt.Errorf("barrier input missing for %s; re-run barrier", waveID)
	}
	var input wave.BarrierInputs
	if err := json.Unmarshal(inputData, &input); err != nil {
		return err
	}
	if hashBarrierInputs(input) != verification.InputHash {
		return fmt.Errorf("barrier input changed since evaluation; acceptance rejected (re-run barrier)")
	}
	now := time.Now().UTC()
	verification.Accepted = true
	verification.AcceptReason = reason
	verification.AcceptedBy = actor
	verification.AcceptedAt = &now
	if err := storage.AtomicWriteJSON(paths.Verification, verification, 0o600); err != nil {
		return err
	}
	return s.commitMutate(context.Background(), event.Input{
		Source: "supervisor", Type: "barrier.warnings_accepted", Severity: "info",
		Payload: map[string]any{
			"wave_id": waveID, "actor": actor, "reason": reason, "input_hash": verification.InputHash,
			"from": string(domain.WaveWaiting), "to": string(domain.WaveVerified),
		},
	}, func(candidate *Snapshot) error {
		for index := range candidate.Waves {
			if string(candidate.Waves[index].WaveID) == waveID {
				candidate.Waves[index].BarrierAccepted = true
				candidate.Waves[index].BarrierReason = reason
				candidate.Waves[index].Status = domain.WaveVerified
				candidate.Waves[index].EndedAt = &now
				if candidate.Wave.WaveID == candidate.Waves[index].WaveID {
					candidate.Wave = candidate.Waves[index]
				}
				break
			}
		}
		candidate.UpdatedAt = now
		return nil
	})
}

// RejectBarrierWarnings records rejection of warnings and fails the Wave.
func (s *Service) RejectBarrierWarnings(waveID, actor, reason string) error {
	if strings.TrimSpace(reason) == "" {
		return fmt.Errorf("reason is required to reject barrier warnings")
	}
	return s.commitMutate(context.Background(), event.Input{
		Source: "supervisor", Type: "barrier.warnings_rejected", Severity: "error",
		Payload: map[string]any{"wave_id": waveID, "actor": actor, "reason": reason, "from": string(domain.WaveWaiting), "to": string(domain.WaveFailed)},
	}, func(candidate *Snapshot) error {
		for index := range candidate.Waves {
			if string(candidate.Waves[index].WaveID) == waveID {
				candidate.Waves[index].Status = domain.WaveFailed
				candidate.Waves[index].BarrierAccepted = false
				candidate.Waves[index].BarrierReason = reason
				now := time.Now().UTC()
				candidate.Waves[index].EndedAt = &now
				if candidate.Wave.WaveID == candidate.Waves[index].WaveID {
					candidate.Wave = candidate.Waves[index]
				}
				break
			}
		}
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
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
	started := time.Now().UTC()
	checks := make([]wave.CheckResult, 0, len(s.plan.FinalChecks))
	for index, check := range s.plan.FinalChecks {
		checks = append(checks, s.runCheck(ctx, check, filepath.Join(s.runDir, fmt.Sprintf("final-check-%03d.log", index+1))))
	}
	after, err := verify.CaptureWorkspace(s.projectRoot(), s.config.BrokerHome)
	if err != nil {
		return wave.Verification{}, err
	}
	changed := verify.ChangedFiles(s.runBaseline, after)
	leases := map[string][]string{}
	s.mu.Lock()
	for _, runtime := range s.snapshot.Tasks {
		leases[string(runtime.Task.TaskID)] = append([]string(nil), runtime.Task.WriteScope...)
	}
	s.mu.Unlock()
	scopeAudit, err := verify.AuditScopes(changed, leases)
	if err != nil {
		return wave.Verification{}, err
	}
	value := wave.EvaluateBarrier(wave.BarrierInputs{
		WaveID:       "run-final",
		ChangedFiles: changed,
		ScopeAudit:   scopeAudit,
		Checks:       checks,
	}, time.Now().UTC())
	value.SchemaVersion = SchemaVersion
	value.StartedAt = started
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

func (s *Service) selectWave(id domain.WaveID) error {
	return s.commitMutate(context.Background(), event.Input{
		Source: "supervisor", Type: event.WaveStateChanged, Severity: "info",
		Payload: map[string]any{"wave_id": string(id), "from": string(s.Snapshot().Wave.WaveID), "to": string(id), "reason": "select_wave"},
	}, func(candidate *Snapshot) error {
		candidate.Run.CurrentWave = id
		for _, value := range candidate.Waves {
			if value.WaveID == id {
				candidate.Wave = value
				break
			}
		}
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
}

func (s *Service) waveAlreadyVerified(id domain.WaveID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, value := range s.snapshot.Waves {
		if value.WaveID == id {
			if value.Status == domain.WaveVerified {
				return true
			}
			// Accepted warnings also count as complete for progression after AcceptBarrierWarnings.
			if value.BarrierResult == domain.BarrierPassedWithWarnings && value.BarrierAccepted {
				return true
			}
			return false
		}
	}
	return false
}

func (s *Service) projectRoot() string {
	snap := s.Snapshot()
	if len(snap.Tasks) == 0 {
		return ""
	}
	return snap.Tasks[0].Task.ProjectRoot
}

func (s *Service) wavePaths(id domain.WaveID) storage.WavePaths {
	layout, _ := storage.NewLayout(s.config.BrokerHome)
	snap := s.Snapshot()
	paths, _ := layout.WavePaths(string(snap.Run.ProjectID), string(snap.Run.RunID), string(id))
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

func (s *Service) markPartial(runtime *TaskState, status report.Status) error {
	if err := s.transitionTask(runtime, state.TaskVerifying); err != nil {
		return err
	}
	if err := s.transitionTask(runtime, state.TaskVerifiedPartial); err != nil {
		return err
	}
	runtime.LastError = fmt.Sprintf("worker reported %s", status)
	return s.saveRuntime(*runtime)
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
	s.onTaskTerminalMessages(string(runtime.Task.TaskID), "task failed: "+stage)
	if stage != "result" && stage != "result_validation" && runtime.ReportPath == "" {
		failed := report.Envelope{SchemaVersion: report.SchemaVersion, TaskID: string(runtime.Task.TaskID), WorkerID: workerID(runtime), Status: report.StatusFailed, Summary: "Worker execution failed", NoFilesChangedReason: "No verified file list was available after the failure", ValidationNotRunReason: "validation was not reached", FailureStage: stage, ErrorSummary: err.Error(), WorkspaceState: "Workspace was left in place; no rollback was performed", HandoffNotes: []string{"Main Agent should inspect the workspace before retrying."}}
		if publishErr := report.Publish(s.taskDir(runtime.Task), failed, time.Now().UTC()); publishErr == nil {
			runtime.ReportPath = filepath.Join(s.taskDir(runtime.Task), "report.md")
			_ = s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), Source: "supervisor", Type: event.ReportPublished, Severity: "error"})
		}
	}
	if saveErr := s.saveRuntime(*runtime); saveErr != nil {
		return saveErr
	}
	_ = s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), Source: "supervisor", Type: event.TurnFailed, Severity: "error", Payload: map[string]string{"stage": stage, "error": err.Error()}})
	return err
}

func (s *Service) cancelTask(runtime *TaskState, reason string) error {
	runtime.LastError = reason
	if runtime.Task.Status != state.TaskCancelled {
		if err := s.transitionTask(runtime, state.TaskCancelled); err != nil {
			return err
		}
	}
	s.onTaskTerminalMessages(string(runtime.Task.TaskID), "task cancelled: "+reason)
	result := report.Envelope{SchemaVersion: report.SchemaVersion, TaskID: string(runtime.Task.TaskID), WorkerID: workerID(runtime), Status: report.StatusCancelled, Summary: "Task was cancelled", NoFilesChangedReason: "cancellation state was collected before verification", ValidationNotRunReason: "run cancelled", StopReason: reason, WorkspaceState: "workspace was left in place; no rollback was performed", HandoffNotes: []string{"Main Agent must inspect the current workspace before retrying."}}
	if err := report.Publish(s.taskDir(runtime.Task), result, time.Now().UTC()); err == nil {
		runtime.ReportPath = filepath.Join(s.taskDir(runtime.Task), "report.md")
		_ = s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), Source: "supervisor", Type: event.ReportPublished, Severity: "warning"})
	}
	return s.saveRuntime(*runtime)
}

func (s *Service) finishRun(status domain.RunStatus, reason string) error {
	// Expire all pending messages across tasks when the Run ends hard.
	if status == domain.RunFailed || status == domain.RunCancelled || status == domain.RunCompleted {
		for _, runtime := range s.Snapshot().Tasks {
			s.onTaskTerminalMessages(string(runtime.Task.TaskID), "run ended: "+string(status))
		}
	}
	if status == domain.RunFailed {
		if err := s.setWaveStatus(domain.WaveFailed); err != nil {
			return err
		}
	} else if status == domain.RunCancelled {
		if err := s.setWaveStatus(domain.WaveCancelled); err != nil {
			return err
		}
	}
	// Aggregate durable summary before terminal status.
	if summary, err := s.buildRunSummary(s.runBaseline); err == nil {
		summary.RunStatus = status
		if writeErr := s.writeRunSummary(summary); writeErr != nil && status == domain.RunCompleted {
			return writeErr
		}
	} else if status == domain.RunCompleted {
		return fmt.Errorf("build run summary: %w", err)
	}
	if err := s.setRunStatus(status, reason); err != nil {
		return err
	}
	typeName := event.RunCompleted
	severity := "info"
	if status == domain.RunFailed || status == domain.RunDegraded {
		typeName = event.RunFailed
		severity = "error"
	} else if status == domain.RunCancelled {
		typeName = "run.cancelled"
		severity = "warning"
	}
	return s.appendEvent(event.Input{Source: "supervisor", Type: typeName, Severity: severity, Payload: map[string]string{"reason": reason}})
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

// saveLocked is retained only for rare non-event projections (legacy tests).
// Production state changes must use Commit.
func (s *Service) saveLocked() error {
	if !s.acceptingWork {
		return fmt.Errorf("supervisor is not accepting work after a fatal persistence failure")
	}
	candidate := s.snapshot
	candidate.UpdatedAt = time.Now().UTC()
	if err := s.persistSnapshotLocked(candidate); err != nil {
		s.acceptingWork = false
		s.reportPersistenceFailure(err)
		return err
	}
	s.snapshot = candidate
	return nil
}

// persistSnapshotLocked writes the multi-file Snapshot projection using only
// the provided value. It must not read or mutate s.snapshot.
func (s *Service) persistSnapshotLocked(value Snapshot) error {
	if s.persistSnapshotFn != nil {
		return s.persistSnapshotFn(value)
	}
	if err := storage.AtomicWriteJSON(s.paths.State, value, 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteJSON(s.paths.Run, value.Run, 0o600); err != nil {
		return err
	}
	layout, err := storage.NewLayout(s.config.BrokerHome)
	if err != nil {
		return err
	}
	for _, runtime := range value.Tasks {
		paths, err := layout.TaskPaths(string(value.Run.ProjectID), string(value.Run.RunID), string(runtime.Task.TaskID))
		if err != nil {
			return err
		}
		if err := storage.AtomicWriteJSON(paths.Task, runtime.Task, 0o600); err != nil {
			return err
		}
	}
	for _, waveValue := range value.Waves {
		paths, err := layout.WavePaths(string(value.Run.ProjectID), string(value.Run.RunID), string(waveValue.WaveID))
		if err != nil {
			return err
		}
		if err := storage.AtomicWriteJSON(paths.Wave, waveValue, 0o600); err != nil {
			return err
		}
	}
	if err := storage.AtomicWriteFile(s.paths.Status, []byte(renderStatus(value)), 0o600); err != nil {
		return err
	}
	return storage.AtomicWriteFile(s.paths.RunSummary, []byte(renderRunSummary(value)), 0o600)
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
	return s.commitMutate(context.Background(), event.Input{
		Source: "supervisor", Type: event.SupervisorHeartbeat, Severity: "info",
		Payload: map[string]any{"reason": reason, "identity": value},
	}, func(candidate *Snapshot) error {
		candidate.Run.SupervisorIdentity = &value
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
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
			if !s.AcceptingWork() {
				return
			}
			_ = s.commitMutate(ctx, event.Input{
				Source: "supervisor", Type: event.SupervisorHeartbeat, Severity: "info",
				Payload: map[string]any{"reason": "heartbeat", "at": now.UTC()},
			}, func(candidate *Snapshot) error {
				if candidate.Run.SupervisorIdentity != nil {
					candidate.Run.SupervisorIdentity.HeartbeatAt = now.UTC()
				}
				candidate.UpdatedAt = now.UTC()
				return nil
			})
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

// cloneSnapshot returns a deep copy of a Snapshot via JSON round-trip.
// Snapshot is itself a JSON persistence structure, so this matches on-disk form.
func cloneSnapshot(value Snapshot) (Snapshot, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return Snapshot{}, fmt.Errorf("clone snapshot: %w", err)
	}
	var copy Snapshot
	if err := json.Unmarshal(data, &copy); err != nil {
		return Snapshot{}, fmt.Errorf("clone snapshot: %w", err)
	}
	return copy, nil
}

func SocketPath(runDir string) string {
	path := filepath.Join(runDir, "control", "supervisor.sock")
	if len(path) < 100 {
		return path
	}
	sum := sha256.Sum256([]byte(path))
	return filepath.Join(os.TempDir(), "subagent-broker-"+hex.EncodeToString(sum[:8])+".sock")
}
