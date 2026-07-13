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
	"syscall"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/stall"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
	"github.com/vnai/subagent-broker/internal/task"
	"github.com/vnai/subagent-broker/internal/verify"
	"github.com/vnai/subagent-broker/internal/wave"
	workerpkg "github.com/vnai/subagent-broker/internal/worker"
)

const SchemaVersion = "v1alpha1"

// errAwaitingDecision keeps the Supervisor alive and IPC-accessible while a
// Barrier or pending Main Agent decision pauses progression.
var errAwaitingDecision = errors.New("supervisor is awaiting a decision")

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

// ReportIdentity freezes the producing attempt identity at report publication time.
// Barrier validates disk meta against this snapshot, not a dynamic current/latest attempt.
type ReportIdentity struct {
	TaskID        string    `json:"task_id"`
	WorkerID      string    `json:"worker_id"`
	AttemptNumber int       `json:"attempt_number"`
	EnvelopeHash  string    `json:"envelope_hash"`
	MarkdownHash  string    `json:"markdown_hash"`
	PublishedAt   time.Time `json:"published_at"`
	// Stale is set when a newer attempt begins; old reports must not pass Barrier.
	Stale bool `json:"stale,omitempty"`
}

type TaskState struct {
	Task                domain.Task           `json:"task"`
	Worker              *domain.WorkerSession `json:"worker,omitempty"` // current/active attempt projection (compat)
	Attempts            []workerpkg.Attempt   `json:"attempts,omitempty"`
	ActiveAttempt       int                   `json:"active_attempt,omitempty"` // attempt number; 0 = none
	BlockKind           BlockKind             `json:"block_kind,omitempty"`
	Dimensions          state.Dimensions      `json:"status_dimensions"`
	ReportPath          string                `json:"report_path,omitempty"`
	ReportIdentity      *ReportIdentity       `json:"report_identity,omitempty"`
	Validation          []ValidationResult    `json:"validation,omitempty"`
	LastError           string                `json:"last_error,omitempty"`
	LastProgress        time.Time             `json:"last_progress_at,omitempty"`
	Stall               *stall.Assessment     `json:"stall_assessment,omitempty"`
	LastStdout          time.Time             `json:"last_stdout_at,omitempty"`
	LastStderr          time.Time             `json:"last_stderr_at,omitempty"`
	LastProtocolEventAt time.Time             `json:"last_protocol_event_at,omitempty"`
	LastToolStart       time.Time             `json:"last_tool_start_at,omitempty"`
	LastToolFinish      time.Time             `json:"last_tool_finish_at,omitempty"`
	TurnStartedAt       time.Time             `json:"turn_started_at,omitempty"`
	TurnEndedAt         time.Time             `json:"turn_ended_at,omitempty"`
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

// pendingWaiter is a buffered wake-up channel for a single decision message.
// The channel is buffered (size 1) so a resolver can notify without blocking.
// The durable Router message.Resolution is authoritative; the channel is only a
// wake-up optimization.
type pendingWaiter struct {
	ch chan struct{}
}

func (s *Service) registerDecisionWaiter(messageID string) *pendingWaiter {
	w := &pendingWaiter{ch: make(chan struct{}, 1)}
	s.mu.Lock()
	s.pending[messageID] = w
	s.mu.Unlock()
	return w
}

func (s *Service) unregisterDecisionWaiter(messageID string, waiter *pendingWaiter) {
	s.mu.Lock()
	if current, ok := s.pending[messageID]; ok && current == waiter {
		delete(s.pending, messageID)
	}
	s.mu.Unlock()
}

func (s *Service) notifyDecisionWaiter(messageID string) {
	s.mu.Lock()
	waiter := s.pending[messageID]
	if waiter != nil {
		delete(s.pending, messageID)
	}
	s.mu.Unlock()
	if waiter != nil {
		select {
		case waiter.ch <- struct{}{}:
		default:
			// Non-blocking: a missed notification is harmless because the
			// waiter re-reads durable state after any wake-up.
		}
	}
}

func (s *Service) loadAnsweredResolution(messageID string) (message.Resolution, bool, error) {
	if s.router == nil {
		return message.Resolution{}, false, fmt.Errorf("router is not initialized")
	}
	return s.router.GetAnsweredResolution(messageID)
}

type Service struct {
	mu             sync.Mutex
	runDir         string
	paths          storage.RunPaths
	registry       *adapter.Registry
	config         Config
	snapshot       Snapshot
	events         eventAppender
	listener       net.Listener // control-plane Unix socket
	workerListener net.Listener // worker-plane Unix socket (worker_request only)
	terminal       chan struct{}
	cancelled      bool
	cancelledTasks map[string]bool
	recovering     bool
	plan           domain.RunPlan
	messages       *message.Store
	messageIndex   map[string]message.Message
	router         *message.Router
	pending        map[string]*pendingWaiter
	active         map[string]activeWorker
	resumeInFlight map[string]bool
	runBaseline    verify.WorkspaceSnapshot

	// Commit fail-closed control plane.
	fatalPersistence chan error
	acceptingWork    bool
	advance          chan struct{}

	// persistSnapshotFn overrides disk persistence for tests. When nil,
	// persistSnapshotLocked writes the multi-file Snapshot projection.
	persistSnapshotFn func(value Snapshot) error

	// IPC authorities: control plane vs worker plane.
	auth *AuthState

	// Per-message native permission delivery serialization (no adapter I/O under Router lock).
	deliveryLocks sync.Map // messageID -> *sync.Mutex

	// Supervisor lease: kernel-backed exclusive fd for the run's mutable lifetime.
	// Acquired before any socket/token/state write; released on process exit.
	leaseFD int
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
	messageReplay, err := message.ReplayDetailed(paths.Messages)
	messageStore := message.NewStore(paths.Messages)
	messageIndex := messageReplay.Messages
	if messageIndex == nil {
		messageIndex = map[string]message.Message{}
	}
	acceptingWork := true
	if err != nil {
		var corrupt *message.ErrJournalCorrupt
		if errors.As(err, &corrupt) {
			// Complete lifecycle-illegal records: fail-closed. Do not truncate mid-journal.
			// Incomplete tails are repaired by ReplayDetailed before this path.
			disableReason := "message_journal_corrupt: " + corrupt.Error()
			messageStore.DisableAppend(disableReason)
			// Durable marker so a later NewStore(path).Append also refuses writes
			// even if this Load returns an error before a Service is retained.
			if markerErr := os.WriteFile(paths.Messages+".append-disabled", []byte(disableReason+"\n"), 0o600); markerErr != nil {
				return nil, errors.Join(
					fmt.Errorf("message journal corrupt: %w", corrupt),
					fmt.Errorf("write append-disabled marker: %w", markerErr),
				)
			}
			acceptingWork = false
			snapshot.Run.Status = domain.RunDegraded
			snapshot.LastError = disableReason
			// Empty projection — corrupt journal is not a trustworthy message source.
			messageIndex = map[string]message.Message{}
			snapshot.Messages = nil
			// Fail-closed status must be durable; write failures must surface.
			persistErr := errors.Join(
				storage.AtomicWriteJSON(paths.State, snapshot, 0o600),
				storage.AtomicWriteFile(paths.Status, []byte(
					"# Run Status\n\n- status: `degraded`\n- reason: `message_journal_corrupt`\n- error: `"+corrupt.Error()+"`\n",
				), 0o600),
			)
			if persistErr != nil {
				// Still construct a fail-closed Service so callers that retain it
				// cannot start workers or append; also return the persist error.
				svc := &Service{
					runDir: runDir, paths: paths, registry: registry, config: config,
					snapshot: snapshot, events: event.NewStore(paths.Events, string(run.RunID), lastSeq),
					terminal: make(chan struct{}), recovering: recovering, plan: plan,
					messages: messageStore, messageIndex: messageIndex,
					pending: map[string]*pendingWaiter{}, active: map[string]activeWorker{},
					cancelledTasks: map[string]bool{}, runBaseline: verify.WorkspaceSnapshot{},
					fatalPersistence: make(chan error, 1), acceptingWork: false, advance: make(chan struct{}, 1),
				}
				// Router still required for API surface; store is append-disabled.
				if r, rErr := message.NewRouter(message.NewRouterOptions{RunID: string(run.RunID), Store: messageStore, Initial: messageIndex}); rErr == nil {
					svc.router = r
				}
				return svc, errors.Join(
					fmt.Errorf("message journal corrupt: %w", corrupt),
					fmt.Errorf("persist fail-closed status: %w", persistErr),
				)
			}
		} else {
			return nil, fmt.Errorf("replay messages: %w", err)
		}
	} else {
		snapshot.Messages = message.Sorted(messageIndex, false)
		// Honor prior append-disabled marker from a previous corrupt Load.
		if _, markerErr := os.Stat(paths.Messages + ".append-disabled"); markerErr == nil {
			messageStore.DisableAppend("message journal append disabled by prior corruption marker")
			acceptingWork = false
			if snapshot.Run.Status != domain.RunDegraded && snapshot.Run.Status != domain.RunFailed {
				snapshot.Run.Status = domain.RunDegraded
			}
			if snapshot.LastError == "" {
				snapshot.LastError = "message_journal_corrupt"
			}
		}
	}
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
		messages: messageStore, messageIndex: messageIndex, router: router, pending: map[string]*pendingWaiter{}, active: map[string]activeWorker{}, cancelledTasks: map[string]bool{}, runBaseline: runBaseline,
		fatalPersistence: make(chan error, 1), acceptingWork: acceptingWork, advance: make(chan struct{}, 1),
	}, nil
}

func (s *Service) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Acquire run-scoped exclusive lease before any mutable operation.
	if err := s.acquireLease(); err != nil {
		return fmt.Errorf("supervisor_already_running: %w", err)
	}
	defer s.releaseLease()

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
	var err error
	for {
		err = s.execute(ctx)
		if !errors.Is(err, errAwaitingDecision) {
			break
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-s.advanceSignal():
		}
		if ctx.Err() != nil {
			err = ctx.Err()
			break
		}
	}
	if err == nil && !s.AcceptingWork() {
		err = fmt.Errorf("supervisor stopped after a fatal persistence failure")
	}
	s.mu.Lock()
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	if s.workerListener != nil {
		_ = s.workerListener.Close()
		s.workerListener = nil
	}
	_ = os.Remove(SocketPath(s.runDir))
	_ = os.Remove(WorkerSocketPath(s.runDir))
	status := s.snapshot.Run.Status
	s.mu.Unlock()
	// Best-effort final identity write; do not fail-close again if already closed.
	_ = s.writeSupervisorIdentity(shutdownReason(err, status))
	close(s.terminal)
	return err
}

func (s *Service) advanceSignal() <-chan struct{} {
	if s.advance == nil {
		return nil
	}
	return s.advance
}

func (s *Service) signalAdvance() {
	if s.advance == nil {
		return
	}
	select {
	case s.advance <- struct{}{}:
	default:
	}
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

	var errs []error
	if err := s.appendEvent(event.Input{Source: "supervisor", Type: event.CancelTreeRequested, Severity: "warning", Payload: map[string]any{"scope": "run"}}); err != nil {
		errs = append(errs, err)
	}
	degraded := false
	// One worker failure must not stop cancellation of others.
	for _, worker := range active {
		result, err := s.terminateActiveWorker(ctx, worker)
		if err != nil {
			errs = append(errs, err)
		}
		if !result.TreeExited && !result.PIDReused {
			degraded = true
		}
	}
	if err := s.appendEvent(event.Input{Source: "supervisor", Type: event.CancelTreeCompleted, Severity: "warning", Payload: map[string]any{"scope": "run", "degraded": degraded}}); err != nil {
		errs = append(errs, err)
	}
	if degraded {
		if err := s.setRunStatus(domain.RunDegraded, "cancel completed with residual worker processes"); err != nil {
			errs = append(errs, err)
		}
	}
	s.signalAdvance()
	return errors.Join(errs...)
}

func (s *Service) RequestCancelTask(ctx context.Context, taskID string) error {
	s.mu.Lock()
	if s.cancelledTasks[taskID] {
		worker, ok := s.active[taskID]
		s.mu.Unlock()
		if !ok {
			return nil // idempotent: already cancelled and inactive
		}
		result, err := s.terminateActiveWorker(ctx, worker)
		var errs []error
		if err != nil {
			errs = append(errs, err)
		}
		if !result.TreeExited && !result.PIDReused {
			if sErr := s.setRunStatus(domain.RunDegraded, "task cancel left residual processes"); sErr != nil {
				errs = append(errs, sErr)
			}
		}
		return errors.Join(errs...)
	}
	worker, ok := s.active[taskID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("task %q has no active Worker", taskID)
	}
	s.cancelledTasks[taskID] = true
	s.mu.Unlock()

	var errs []error
	if err := s.appendEvent(event.Input{TaskID: taskID, Source: "supervisor", Type: event.CancelTreeRequested, Severity: "warning", Payload: map[string]any{"scope": "task"}}); err != nil {
		errs = append(errs, err)
	}
	result, err := s.terminateActiveWorker(ctx, worker)
	if err != nil {
		errs = append(errs, err)
	}
	if err := s.appendEvent(event.Input{TaskID: taskID, Source: "supervisor", Type: event.CancelTreeCompleted, Severity: "warning", Payload: map[string]any{
		"tree_exited": result.TreeExited, "pid_reused": result.PIDReused, "remaining": result.RemainingPIDs,
		"orphan_risk": result.OrphanRisk,
	}}); err != nil {
		errs = append(errs, err)
	}
	if !result.TreeExited && !result.PIDReused {
		if err := s.setRunStatus(domain.RunDegraded, "task cancel left residual processes"); err != nil {
			errs = append(errs, err)
		}
	}
	s.signalAdvance()
	return errors.Join(errs...)
}

func (s *Service) terminateActiveWorker(ctx context.Context, worker activeWorker) (process.TerminationResult, error) {
	// Protocol interrupt first (best effort).
	if worker.adapter != nil {
		_ = worker.adapter.InterruptTurn(ctx, worker.sessionID)
	}
	policy := defaultCancelPolicy(s.config.CancelGrace)
	var result process.TerminationResult
	var err error
	if worker.identity.Complete() {
		result, err = process.Controller{Manager: process.PlatformManager{}}.TerminateTree(ctx, worker.identity, policy)
		if err == nil && !result.TreeExited && !result.PIDReused {
			result.OrphanRisk = true
		}
	} else {
		// Incomplete identity: Adapter terminate is best-effort only.
		// TerminateSession returning nil does NOT confirm full process-tree exit.
		result.TermSent = true
		result.TreeExited = false
		result.OrphanRisk = true
		if worker.adapter != nil {
			if termErr := worker.adapter.TerminateSession(context.Background(), worker.sessionID); termErr != nil {
				result.Errors = append(result.Errors, "adapter terminate: "+termErr.Error())
				err = termErr
			} else {
				result.Errors = append(result.Errors, "incomplete process identity: tree exit unconfirmed after adapter terminate")
			}
		} else {
			result.Errors = append(result.Errors, "incomplete process identity and no adapter for terminate fallback")
			err = fmt.Errorf("incomplete process identity; cannot confirm tree exit")
		}
	}
	if worker.cancel != nil {
		worker.cancel()
	}
	// Persist residual / orphan facts onto the Task when confirmation failed.
	if !result.TreeExited && !result.PIDReused {
		if persistErr := s.recordCancelOrphanRisk(worker, result); persistErr != nil {
			err = errors.Join(err, persistErr)
		}
	}
	return result, err
}

// recordCancelOrphanRisk marks process unknown/orphaned when cancel could not
// confirm tree exit. Remaining PIDs and reason are durable via Commit.
func (s *Service) recordCancelOrphanRisk(worker activeWorker, result process.TerminationResult) error {
	if worker.taskID == "" {
		return nil
	}
	processState := state.ProcessUnknown
	if len(result.RemainingPIDs) > 0 || result.OrphanRisk {
		if len(result.RemainingPIDs) > 0 {
			processState = state.ProcessOrphaned
		}
	}
	reason := "cancel could not confirm process tree exit"
	if len(result.Errors) > 0 {
		reason = reason + ": " + strings.Join(result.Errors, "; ")
	}
	return s.commitMutate(context.Background(), event.Input{
		TaskID: worker.taskID, WorkerID: worker.workerID, Source: "supervisor",
		Type: event.ProcessOrphaned, Severity: "error",
		Payload: map[string]any{
			"from": string(state.ProcessAlive), "to": string(processState),
			"reason": reason, "remaining": result.RemainingPIDs, "orphan_risk": result.OrphanRisk,
			"tree_exited": result.TreeExited,
		},
	}, func(candidate *Snapshot) error {
		index, err := findTaskIndex(candidate, domain.TaskID(worker.taskID))
		if err != nil {
			return err
		}
		candidate.Tasks[index].Dimensions.Process = processState
		if candidate.Tasks[index].Worker != nil {
			candidate.Tasks[index].Worker.StatusDimensions.Process = processState
		}
		candidate.LastError = reason
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	})
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
			// Keep the Supervisor alive so a pending decision can be answered
			// through IPC. The next execute pass re-runs this Barrier.
			if err := s.setRunStatus(domain.RunBarrier, "Wave barrier blocked: pending decisions or final-blocked tasks"); err != nil {
				return err
			}
			return errAwaitingDecision
		case domain.BarrierPassedWithWarnings:
			// Keep the Supervisor alive; barrier.accept will signal the next
			// execute pass and the following Wave will start automatically.
			if err := s.setRunStatus(domain.RunBarrier, "Wave barrier passed_with_warnings; acceptance required before continuing"); err != nil {
				return err
			}
			return errAwaitingDecision
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
		if final.Accepted {
			// Already accepted via AcceptFinalWarnings; fall through to complete.
		} else {
			// Non-terminal wait: keep Supervisor/IPC alive for accept/reject.
			if err := s.setRunStatus(domain.RunBarrier, "final verification has warnings; acceptance required before run completion"); err != nil {
				return err
			}
			_ = s.appendEvent(event.Input{Source: "supervisor", Type: "final_verification.awaiting_acceptance", Severity: "warning", Payload: map[string]any{
				"input_hash": final.InputHash, "result": string(final.Result),
			}})
			return errAwaitingDecision
		}
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
	var errs []error
	for _, taskID := range taskIDs(planned.Tasks) {
		runtime, ok := s.taskState(taskID)
		// Never re-run terminal, orphaned, or unknown-process tasks.
		if !ok || recoveryTaskTerminal(runtime) ||
			runtime.Dimensions.Process == state.ProcessOrphaned ||
			runtime.Dimensions.Process == state.ProcessUnknown {
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
			// executeTask owns final Task persistence; do not saveRuntime a stale local copy.
			if err := s.executeTask(ctx, &runtime); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("task %s: %w", runtime.Task.TaskID, err))
				mu.Unlock()
			}
		}(runtime)
	}
	wg.Wait()
	if !s.AcceptingWork() {
		joined := errors.Join(errs...)
		if joined != nil {
			return joined
		}
		return fmt.Errorf("supervisor is not accepting work after a fatal persistence failure")
	}
	return errors.Join(errs...)
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
	runtime.LastStdout = now
	runtime.LastProtocolEventAt = now
	if !native.Timestamp.IsZero() {
		runtime.LastProtocolEventAt = native.Timestamp
	}
	worker.StatusDimensions.Progress = state.ProgressActive
	switch native.Kind {
	case event.TurnStarted:
		runtime.TurnStartedAt = now
		runtime.TurnEndedAt = time.Time{}
		worker.StatusDimensions.Protocol = state.ProtocolThinking
	case event.ModelOutputDelta, event.ModelMessageCompleted:
		worker.StatusDimensions.Protocol = state.ProtocolStreaming
	case event.ToolStarted:
		runtime.LastToolStart = now
		worker.StatusDimensions.Protocol = state.ProtocolToolRunning
	case event.ToolCompleted, event.ToolOutput:
		runtime.LastToolFinish = now
		worker.StatusDimensions.Protocol = state.ProtocolThinking
	case event.PermissionRequested:
		worker.StatusDimensions.Protocol = state.ProtocolWaitingPermission
		worker.StatusDimensions.Progress = state.ProgressQuiet
		// Protocol-native permission events become durable Broker messages so the
		// Main Agent can answer and the adapter receives RespondPermission.
		s.bridgeNativePermission(runtime, harness, native, workerID)
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
		runtime.TurnEndedAt = now
		worker.StatusDimensions.Protocol = state.ProtocolIdleBetweenTurns
	}
	runtime.Dimensions = worker.StatusDimensions
	clearedStall := runtime.Stall != nil && (runtime.Stall.State == string(state.ProgressSuspectedStall) || runtime.Stall.State == string(state.ProgressStalled))
	if clearedStall {
		runtime.Stall = nil
	}
	_ = s.saveRuntime(*runtime)
	if clearedStall {
		_ = s.appendEvent(event.Input{
			TaskID: string(runtime.Task.TaskID), WorkerID: string(worker.WorkerID), Source: "supervisor",
			Type: event.WorkerStallCleared, Severity: "info",
			Payload: map[string]any{"reason": "protocol or output progress resumed", "evidence": []string{"new protocol/output event"}},
		})
	}
	// Turn-boundary outbox decisions are owned by runWorkerSession so a queued
	// next turn cannot start and then be terminated by the same ResultSubmitted.
}

func (s *Service) updateProgress(runtime *TaskState, now time.Time) {
	worker := runtime.Worker
	if worker == nil || worker.LastProgressAt.IsZero() {
		return
	}
	assessment := stall.Assess(stall.Input{
		Protocol:            worker.StatusDimensions.Protocol,
		Progress:            worker.StatusDimensions.Progress,
		Process:             worker.StatusDimensions.Process,
		LastProgressAt:      worker.LastProgressAt,
		LastEventAt:         worker.LastEventAt,
		LastProtocolEventAt: runtime.LastProtocolEventAt,
		LastStdoutAt:        runtime.LastStdout,
		LastStderrAt:        runtime.LastStderr,
		LastToolStartAt:     runtime.LastToolStart,
		LastToolFinishAt:    runtime.LastToolFinish,
		TurnStartedAt:       runtime.TurnStartedAt,
		TurnEndedAt:         runtime.TurnEndedAt,
		HasPendingMessage:   s.taskHasPendingMessage(string(runtime.Task.TaskID)),
		QuietAfter:          s.config.QuietAfter,
		StallAfter:          s.config.StallAfter,
		Now:                 now,
	})
	previousState, previousConfidence := "", ""
	if runtime.Stall != nil {
		previousState, previousConfidence = runtime.Stall.State, runtime.Stall.Confidence
	}
	wasSuspected := previousState == string(state.ProgressSuspectedStall) || previousState == string(state.ProgressStalled)
	isSuspected := assessment.State == string(state.ProgressSuspectedStall) || assessment.State == string(state.ProgressStalled)
	assessmentChanged := previousState != assessment.State || previousConfidence != assessment.Confidence
	desired := worker.StatusDimensions.Progress
	switch assessment.State {
	case string(state.ProgressStalled):
		desired = state.ProgressStalled
	case string(state.ProgressSuspectedStall):
		desired = state.ProgressSuspectedStall
	case string(state.ProgressQuiet):
		desired = state.ProgressQuiet
	case string(state.ProgressActive), "none":
		desired = state.ProgressActive
	default:
		return
	}
	if desired == worker.StatusDimensions.Progress && !assessmentChanged {
		return
	}
	from := worker.StatusDimensions.Progress
	if desired != from {
		if err := state.ValidateProgressTransition(from, desired); err != nil {
			return
		}
	}
	worker.StatusDimensions.Progress = desired
	runtime.Dimensions = worker.StatusDimensions
	if isSuspected {
		assessmentCopy := assessment
		runtime.Stall = &assessmentCopy
	} else {
		runtime.Stall = nil
	}
	_ = s.saveRuntime(*runtime)
	if desired != from {
		_ = s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), WorkerID: string(worker.WorkerID), Source: "supervisor", Type: event.ProgressStateChanged, Severity: "warning", Payload: map[string]any{"from": string(from), "to": string(desired), "reason": "stall_watch"}})
	}
	if wasSuspected && !isSuspected {
		_ = s.appendEvent(event.Input{
			TaskID: string(runtime.Task.TaskID), WorkerID: string(worker.WorkerID), Source: "supervisor",
			Type: event.WorkerStallCleared, Severity: "info",
			Payload: map[string]any{"reason": "stall assessment cleared", "evidence": assessment.Evidence},
		})
	} else if isSuspected && assessmentChanged {
		_ = s.appendEvent(event.Input{
			TaskID: string(runtime.Task.TaskID), WorkerID: string(worker.WorkerID), Source: "supervisor",
			Type: event.WorkerStallAssessed, Severity: "warning",
			Payload: map[string]any{
				"from": string(from), "to": string(desired), "quiet_for": assessment.QuietFor.String(),
				"state": assessment.State, "reason": assessment.Reason,
				"confidence": assessment.Confidence, "evidence": assessment.Evidence,
			},
		})
	}
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
		Registry:       s.registry,
		Executable:     s.config.Executable,
		ProbeTimeout:   10 * time.Second,
		PermissionMode: s.config.PermissionMode,
		// Permission hooks are only mandatory for Claude tasks that claim broker-mediated tools.
		RequirePermissionHooks: !s.config.SafeMode && adapter.RequiresPermissionRouting(s.config.PermissionMode, false),
		SafeMode:               s.config.SafeMode,
	})
	// Re-check session-level Claude hook installation only for Claude tasks.
	if !s.config.SafeMode && adapter.RequiresPermissionRouting(s.config.PermissionMode, false) {
		for _, item := range tasks {
			if item.HarnessPreference != string(adapter.HarnessClaudeCode) {
				continue
			}
			if harness, ok := s.registry.Get(adapter.HarnessClaudeCode); ok {
				set := s.computeSessionCapabilities(harness, item)
				if !set.Effective.PermissionEvents {
					result.Issues = append(result.Issues, wave.Issue{
						Kind: wave.IssueHarnessProbeFailed, Severity: wave.SeverityError, Tasks: []string{string(item.TaskID)},
						Details: fmt.Sprintf("harness %s permission_events not effective (hooks not installed or safe mode); downgrades: %v", item.HarnessPreference, set.Downgrades),
					})
					result.Allowed = false
				}
			}
		}
	}
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
// Current workspace/report/message/task facts are re-collected; old barrier-input.json
// is never treated as proof that nothing changed.
func (s *Service) AcceptBarrierWarnings(waveID, actor, reason string) error {
	if strings.TrimSpace(actor) == "" || strings.TrimSpace(reason) == "" {
		return fmt.Errorf("actor and reason are required to accept barrier warnings")
	}
	current, found := s.waveSnapshot(domain.WaveID(waveID))
	if !found {
		return fmt.Errorf("wave %s was not found", waveID)
	}
	if current.Status != domain.WaveWaiting || current.BarrierResult != domain.BarrierPassedWithWarnings || current.BarrierAccepted {
		return fmt.Errorf("wave %s is not awaiting warning acceptance", waveID)
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
	if verification.Result != domain.BarrierPassedWithWarnings || verification.Rejected {
		return fmt.Errorf("wave %s barrier result is %s, not passed_with_warnings", waveID, verification.Result)
	}
	if err := s.revalidateBarrierCurrentFacts(context.Background(), domain.WaveID(waveID), verification.InputHash); err != nil {
		return err
	}
	now := time.Now().UTC()
	verification.Accepted = true
	verification.AcceptReason = reason
	verification.AcceptedBy = actor
	verification.AcceptedAt = &now
	if err := storage.AtomicWriteJSON(paths.Verification, verification, 0o600); err != nil {
		return err
	}
	if err := s.commitMutate(context.Background(), event.Input{
		Source: "supervisor", Type: event.BarrierWarningsAccepted, Severity: "info",
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
	}); err != nil {
		return err
	}
	s.signalAdvance()
	return nil
}

// RejectBarrierWarnings records rejection of warnings and fails the Wave.
// Current facts are re-collected the same way as AcceptBarrierWarnings.
func (s *Service) RejectBarrierWarnings(waveID, actor, reason string) error {
	if strings.TrimSpace(actor) == "" || strings.TrimSpace(reason) == "" {
		return fmt.Errorf("actor and reason are required to reject barrier warnings")
	}
	current, found := s.waveSnapshot(domain.WaveID(waveID))
	if !found {
		return fmt.Errorf("wave %s was not found", waveID)
	}
	if current.Status != domain.WaveWaiting || current.BarrierResult != domain.BarrierPassedWithWarnings || current.BarrierAccepted {
		return fmt.Errorf("wave %s is not awaiting warning acceptance", waveID)
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
		return fmt.Errorf("wave %s verification is stale; re-run barrier", waveID)
	}
	if err := s.revalidateBarrierCurrentFacts(context.Background(), domain.WaveID(waveID), verification.InputHash); err != nil {
		return err
	}
	now := time.Now().UTC()
	verification.Rejected = true
	verification.RejectReason = reason
	verification.RejectedBy = actor
	verification.RejectedAt = &now
	if err := storage.AtomicWriteJSON(paths.Verification, verification, 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteFile(paths.Barrier, []byte(wave.RenderBarrier(verification)), 0o600); err != nil {
		return err
	}
	if err := s.commitMutate(context.Background(), event.Input{
		Source: "supervisor", Type: event.BarrierWarningsRejected, Severity: "error",
		Payload: map[string]any{"wave_id": waveID, "actor": actor, "reason": reason, "from": string(domain.WaveWaiting), "to": string(domain.WaveFailed)},
	}, func(candidate *Snapshot) error {
		for index := range candidate.Waves {
			if string(candidate.Waves[index].WaveID) == waveID {
				candidate.Waves[index].Status = domain.WaveFailed
				candidate.Waves[index].BarrierAccepted = false
				candidate.Waves[index].BarrierReason = reason
				candidate.Waves[index].EndedAt = &now
				if candidate.Wave.WaveID == candidate.Waves[index].WaveID {
					candidate.Wave = candidate.Waves[index]
				}
				break
			}
		}
		candidate.UpdatedAt = time.Now().UTC()
		return nil
	}); err != nil {
		return err
	}
	if err := s.setRunStatus(domain.RunFailed, fmt.Sprintf("Wave %s barrier warnings rejected by %s: %s", waveID, actor, reason)); err != nil {
		return err
	}
	if err := s.appendEvent(event.Input{Source: "supervisor", Type: event.RunFailed, Severity: "error", Payload: map[string]string{"reason": fmt.Sprintf("barrier warnings rejected for %s", waveID)}}); err != nil {
		return err
	}
	s.signalAdvance()
	return nil
}

func (s *Service) waveSnapshot(id domain.WaveID) (domain.Wave, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, value := range s.snapshot.Waves {
		if value.WaveID == id {
			return value, true
		}
	}
	if s.snapshot.Wave.WaveID == id {
		return s.snapshot.Wave, true
	}
	return domain.Wave{}, false
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
	input, err := s.collectFinalVerificationInputs(ctx)
	if err != nil {
		return wave.Verification{}, err
	}
	inputHash := hashBarrierInputs(input)
	if err := storage.AtomicWriteJSON(filepath.Join(s.runDir, "final-verification-input.json"), input, 0o600); err != nil {
		return wave.Verification{}, err
	}
	value := wave.EvaluateBarrier(input, time.Now().UTC())
	value.SchemaVersion = SchemaVersion
	value.StartedAt = started
	value.InputHash = inputHash
	// Preserve prior acceptance if facts are unchanged.
	if prev, readErr := s.readFinalVerification(); readErr == nil && prev.Accepted && prev.InputHash == inputHash {
		value.Accepted = true
		value.AcceptReason = prev.AcceptReason
		value.AcceptedBy = prev.AcceptedBy
		value.AcceptedAt = prev.AcceptedAt
	}
	if err := storage.AtomicWriteJSON(s.paths.Verification, value, 0o600); err != nil {
		return value, err
	}
	_ = s.appendEvent(event.Input{Source: "supervisor", Type: "final_verification.evaluated", Severity: "info", Payload: map[string]any{
		"result": string(value.Result), "input_hash": inputHash,
	}})
	return value, nil
}

func (s *Service) collectFinalVerificationInputs(ctx context.Context) (wave.BarrierInputs, error) {
	checks := make([]wave.CheckResult, 0, len(s.plan.FinalChecks))
	for index, check := range s.plan.FinalChecks {
		checks = append(checks, s.runCheck(ctx, check, filepath.Join(s.runDir, fmt.Sprintf("final-check-%03d.log", index+1))))
	}
	after, err := verify.CaptureWorkspace(s.projectRoot(), s.config.BrokerHome)
	if err != nil {
		return wave.BarrierInputs{}, err
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
		return wave.BarrierInputs{}, err
	}
	highRisk := classifyHighRiskChanges(changed, scopeAudit)
	return wave.BarrierInputs{
		WaveID:          "run-final",
		ChangedFiles:    changed,
		ScopeAudit:      scopeAudit,
		HighRiskChanges: highRisk,
		Checks:          checks,
	}, nil
}

func (s *Service) readFinalVerification() (wave.Verification, error) {
	data, err := os.ReadFile(s.paths.Verification)
	if err != nil {
		return wave.Verification{}, err
	}
	var value wave.Verification
	if err := json.Unmarshal(data, &value); err != nil {
		return wave.Verification{}, err
	}
	return value, nil
}

// revalidateFinalCurrentFacts re-collects final verification inputs and compares
// their hash to the stored verification InputHash.
func (s *Service) revalidateFinalCurrentFacts(ctx context.Context, expectedHash string) error {
	if strings.TrimSpace(expectedHash) == "" {
		return fmt.Errorf("stale_verification: final verification lacks input_hash; re-run final verification")
	}
	current, err := s.collectFinalVerificationInputs(ctx)
	if err != nil {
		return fmt.Errorf("stale_verification: collect final inputs: %w", err)
	}
	if hashBarrierInputs(current) != expectedHash {
		return fmt.Errorf("stale_verification: final verification inputs changed since evaluation (conflict); re-run final verification")
	}
	return nil
}

// AcceptFinalWarnings records formal acceptance of final verification warnings.
// Revalidates current facts against the stored input hash before completing the Run.
func (s *Service) AcceptFinalWarnings(actor, reason string) error {
	if strings.TrimSpace(actor) == "" || strings.TrimSpace(reason) == "" {
		return fmt.Errorf("actor and reason are required to accept final verification warnings")
	}
	status := s.Snapshot().Run.Status
	if status != domain.RunBarrier {
		return fmt.Errorf("run is not awaiting final warning acceptance (status=%s)", status)
	}
	for _, w := range s.Snapshot().Waves {
		if w.Status == domain.WaveWaiting && w.BarrierResult == domain.BarrierPassedWithWarnings && !w.BarrierAccepted {
			return fmt.Errorf("wave %s is still awaiting barrier acceptance; not final verification", w.WaveID)
		}
	}
	verification, err := s.readFinalVerification()
	if err != nil {
		return fmt.Errorf("read final verification: %w", err)
	}
	if verification.Result != domain.BarrierPassedWithWarnings || verification.Rejected {
		return fmt.Errorf("final verification result is %s, not awaiting acceptance", verification.Result)
	}
	if verification.Accepted {
		return s.finishRun(domain.RunCompleted, "final verification warnings already accepted")
	}
	if err := s.revalidateFinalCurrentFacts(context.Background(), verification.InputHash); err != nil {
		return err
	}
	now := time.Now().UTC()
	verification.Accepted = true
	verification.AcceptReason = reason
	verification.AcceptedBy = actor
	verification.AcceptedAt = &now
	if err := storage.AtomicWriteJSON(s.paths.Verification, verification, 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteJSON(filepath.Join(s.runDir, "final-acceptance.json"), map[string]any{
		"actor": actor, "reason": reason, "input_hash": verification.InputHash, "accepted_at": now,
	}, 0o600); err != nil {
		return err
	}
	if err := s.appendEvent(event.Input{Source: "supervisor", Type: "final_verification.warnings_accepted", Severity: "info", Payload: map[string]any{
		"actor": actor, "reason": reason, "input_hash": verification.InputHash,
	}}); err != nil {
		return err
	}
	s.signalAdvance()
	return s.finishRun(domain.RunCompleted, fmt.Sprintf("final verification warnings accepted by %s", actor))
}

// RejectFinalWarnings rejects final verification warnings and fails the Run.
func (s *Service) RejectFinalWarnings(actor, reason string) error {
	if strings.TrimSpace(actor) == "" || strings.TrimSpace(reason) == "" {
		return fmt.Errorf("actor and reason are required to reject final verification warnings")
	}
	status := s.Snapshot().Run.Status
	if status != domain.RunBarrier {
		return fmt.Errorf("run is not awaiting final warning acceptance (status=%s)", status)
	}
	for _, w := range s.Snapshot().Waves {
		if w.Status == domain.WaveWaiting && w.BarrierResult == domain.BarrierPassedWithWarnings && !w.BarrierAccepted {
			return fmt.Errorf("wave %s is still awaiting barrier acceptance; not final verification", w.WaveID)
		}
	}
	verification, err := s.readFinalVerification()
	if err != nil {
		return fmt.Errorf("read final verification: %w", err)
	}
	if verification.Result != domain.BarrierPassedWithWarnings {
		return fmt.Errorf("final verification is not awaiting rejection (result=%s)", verification.Result)
	}
	if err := s.revalidateFinalCurrentFacts(context.Background(), verification.InputHash); err != nil {
		return err
	}
	now := time.Now().UTC()
	verification.Rejected = true
	verification.RejectReason = reason
	verification.RejectedBy = actor
	verification.RejectedAt = &now
	if err := storage.AtomicWriteJSON(s.paths.Verification, verification, 0o600); err != nil {
		return err
	}
	if err := s.appendEvent(event.Input{Source: "supervisor", Type: "final_verification.warnings_rejected", Severity: "error", Payload: map[string]any{
		"actor": actor, "reason": reason, "input_hash": verification.InputHash,
	}}); err != nil {
		return err
	}
	s.signalAdvance()
	return s.finishRun(domain.RunFailed, fmt.Sprintf("final verification warnings rejected by %s: %s", actor, reason))
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
	return adapter.CapabilityMap(value)
}

// harnessNameForTask is the single routing rule used after a Run is loaded:
// the persisted Worker is authoritative during recovery, then the Task
// preference, then the Run default.
func (s *Service) harnessNameForTask(task domain.Task, worker *domain.WorkerSession) string {
	if worker != nil && strings.TrimSpace(worker.Harness) != "" {
		return strings.TrimSpace(worker.Harness)
	}
	if strings.TrimSpace(task.HarnessPreference) != "" {
		return strings.TrimSpace(task.HarnessPreference)
	}
	return strings.TrimSpace(s.config.Harness)
}

func (s *Service) adapterForTask(task domain.Task, worker *domain.WorkerSession) (adapter.Adapter, bool) {
	return s.registry.Get(adapter.HarnessName(s.harnessNameForTask(task, worker)))
}

// resolveHarnessForExecution selects the adapter for executeTask / resume.
// On recovery resume with a native session ID, an empty persisted harness is
// rejected when multiple adapters are registered (ambiguous ownership).
// A non-empty persisted harness always wins and is never overridden by Task preference.
func (s *Service) resolveHarnessForExecution(runtime *TaskState, mode workerpkg.AttemptMode) (adapter.Adapter, string, error) {
	if runtime == nil {
		return nil, "", fmt.Errorf("runtime is required")
	}
	resuming := mode == workerpkg.AttemptRecoveryResume
	if resuming && runtime.Worker != nil && runtime.Worker.NativeSessionID != "" {
		if strings.TrimSpace(runtime.Worker.Harness) == "" && s.registryAdapterCount() > 1 {
			return nil, "", fmt.Errorf(
				"cannot resume native session %q for task %s: persisted worker harness is empty and multiple adapters are registered",
				runtime.Worker.NativeSessionID, runtime.Task.TaskID,
			)
		}
	}
	name := s.harnessNameForTask(runtime.Task, runtime.Worker)
	if strings.TrimSpace(name) == "" {
		return nil, "", fmt.Errorf("no harness selected for task %s", runtime.Task.TaskID)
	}
	// Never hand a native session ID to a different adapter than the one that created it.
	if runtime.Worker != nil && runtime.Worker.NativeSessionID != "" {
		persisted := strings.TrimSpace(runtime.Worker.Harness)
		if persisted != "" && persisted != name {
			return nil, "", fmt.Errorf(
				"harness mismatch for task %s: persisted worker harness %q != selected %q; refusing to resume native session on the wrong adapter",
				runtime.Task.TaskID, persisted, name,
			)
		}
	}
	if s.registry == nil {
		return nil, "", fmt.Errorf("adapter registry is not initialized")
	}
	harness, ok := s.registry.Get(adapter.HarnessName(name))
	if !ok {
		return nil, "", fmt.Errorf("adapter %q is not registered", name)
	}
	return harness, name, nil
}

func (s *Service) registryAdapterCount() int {
	if s.registry == nil {
		return 0
	}
	return len(s.registry.Descriptors())
}

// computeSessionCapabilities derives EffectiveCapabilities for a Task session.
func (s *Service) computeSessionCapabilities(harness adapter.Adapter, task domain.Task) adapter.CapabilitySet {
	declared := harness.Descriptor().Capabilities
	probeCaps := declared
	// Best-effort probe; failures leave probe=declared (unverified probe does not invent support).
	probeCtx, cancelProbe := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelProbe()
	if probe, err := harness.Probe(probeCtx, adapter.ProbeRequest{Executable: s.config.Executable}); err == nil && probe.Installed {
		// Use declared∩probe.Capabilities when probe returns capability bits.
		if probe.Capabilities != (adapter.Capabilities{}) {
			probeCaps = probe.Capabilities
		}
	}
	fact := adapter.SessionConfigFact{
		PermissionMode: s.config.PermissionMode,
		SafeMode:       s.config.SafeMode,
	}
	// Ask adapter for session install facts when supported.
	// Use the resolved harness identity (descriptor), not Task preference alone.
	type factProvider interface {
		SessionConfigFact(adapter.StartRequest) adapter.SessionConfigFact
	}
	harnessName := string(harness.Descriptor().Name)
	if fp, ok := harness.(factProvider); ok {
		req := adapter.StartRequest{
			Options: map[string]string{
				"permission_mode": s.config.PermissionMode,
				"safe_mode":       fmt.Sprintf("%v", s.config.SafeMode),
			},
			Interaction: adapter.InteractionConfig{Enabled: !s.config.SafeMode && harnessName == string(adapter.HarnessClaudeCode)},
		}
		fact = fp.SessionConfigFact(req)
	} else {
		// Generic adapters without SessionConfigFact: only enable hooks when not safe.
		fact.HooksInstalled = !s.config.SafeMode && declared.Hooks
		fact.MCPEnabled = !s.config.SafeMode
		fact.SteerVerified = false
		fact.NextTurnDelivery = declared.BidirectionalStream
	}
	// Contract registry may mark steer verified for this harness/version.
	if contractSteerVerified(string(harness.Descriptor().Name), harness.Descriptor().TestedMaxVersion) {
		fact.SteerVerified = true
		fact.NextTurnDelivery = false
	}
	// Fake harness versions use "fake-1.0.0" in probe; also check empty version.
	if contractSteerVerified(string(harness.Descriptor().Name), "") {
		fact.SteerVerified = true
	}
	_ = task
	return adapter.DeriveEffective(declared, probeCaps, fact)
}

// contractSteerVerified is implemented in contract_bridge.go to avoid import cycles
// when contracttest is not linked; default false.

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
	if expireErr := s.onTaskTerminalMessages(string(runtime.Task.TaskID), "task failed: "+stage); expireErr != nil {
		// Fail-closed: terminal cleanup persistence errors must surface.
		return fmt.Errorf("task failed (%s): %w; also expire messages: %v", stage, err, expireErr)
	}
	if stage != "result" && stage != "result_validation" && runtime.ReportPath == "" {
		failed := report.Envelope{SchemaVersion: report.SchemaVersion, TaskID: string(runtime.Task.TaskID), WorkerID: workerID(runtime), Status: report.StatusFailed, Summary: "Worker execution failed", NoFilesChangedReason: "No verified file list was available after the failure", ValidationNotRunReason: "validation was not reached", FailureStage: stage, ErrorSummary: err.Error(), WorkspaceState: "Workspace was left in place; no rollback was performed", HandoffNotes: []string{"Main Agent should inspect the workspace before retrying."}}
		attemptNumber := reportAttemptNumber(runtime)
		if publishErr := report.Publish(s.taskDir(runtime.Task), failed, attemptNumber, time.Now().UTC()); publishErr == nil {
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
	if expireErr := s.onTaskTerminalMessages(string(runtime.Task.TaskID), "task cancelled: "+reason); expireErr != nil {
		return expireErr
	}
	result := report.Envelope{SchemaVersion: report.SchemaVersion, TaskID: string(runtime.Task.TaskID), WorkerID: workerID(runtime), Status: report.StatusCancelled, Summary: "Task was cancelled", NoFilesChangedReason: "cancellation state was collected before verification", ValidationNotRunReason: "run cancelled", StopReason: reason, WorkspaceState: "workspace was left in place; no rollback was performed", HandoffNotes: []string{"Main Agent must inspect the current workspace before retrying."}}
	if err := report.Publish(s.taskDir(runtime.Task), result, reportAttemptNumber(runtime), time.Now().UTC()); err == nil {
		runtime.ReportPath = filepath.Join(s.taskDir(runtime.Task), "report.md")
		_ = s.appendEvent(event.Input{TaskID: string(runtime.Task.TaskID), Source: "supervisor", Type: event.ReportPublished, Severity: "warning"})
	}
	return s.saveRuntime(*runtime)
}

func reportAttemptNumber(runtime *TaskState) int {
	if runtime == nil {
		return 1
	}
	if runtime.ActiveAttempt > 0 {
		return runtime.ActiveAttempt
	}
	if runtime.Worker != nil && runtime.Worker.Attempt > 0 {
		return runtime.Worker.Attempt
	}
	if n := len(runtime.Attempts); n > 0 {
		return runtime.Attempts[n-1].Number
	}
	return 1
}

func (s *Service) finishRun(status domain.RunStatus, reason string) error {
	// All terminal Run statuses (including RunDegraded) must expire pending messages.
	if runTerminal(status) {
		var expireErrs []error
		for _, runtime := range s.Snapshot().Tasks {
			if err := s.onTaskTerminalMessages(string(runtime.Task.TaskID), "run ended: "+string(status)); err != nil {
				expireErrs = append(expireErrs, err)
			}
		}
		if err := errors.Join(expireErrs...); err != nil {
			// Fail-closed: cleanup persistence failure must surface for every terminal status.
			return fmt.Errorf("expire messages on run %s: %w", status, err)
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
	if s.auth == nil {
		s.auth = newAuthState()
	}
	if _, err := s.auth.InitControlCredential(s.runDir); err != nil {
		return err
	}
	// Control-plane socket (operator methods only).
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
	// Worker-plane socket (worker_request only).
	wpath := WorkerSocketPath(s.runDir)
	_ = os.Remove(wpath)
	wlistener, err := net.Listen("unix", wpath)
	if err != nil {
		_ = listener.Close()
		return err
	}
	if err := os.Chmod(wpath, 0o600); err != nil {
		_ = listener.Close()
		_ = wlistener.Close()
		return err
	}
	s.mu.Lock()
	s.listener = listener
	s.workerListener = wlistener
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
	// Control plane.
	go s.acceptLoop(ctx, func() net.Listener {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.listener
	}, CallerControl)
	// Worker plane.
	s.acceptLoop(ctx, func() net.Listener {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.workerListener
	}, CallerWorker)
}

func (s *Service) acceptLoop(ctx context.Context, get func() net.Listener, plane CallerRole) {
	for {
		listener := get()
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
		go s.handleConnection(ctx, conn, plane)
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

// LeasePath is the run-scoped supervisor lock file (flock).
func LeasePath(runDir string) string {
	return filepath.Join(runDir, "control", "supervisor.lock")
}

func (s *Service) acquireLease() error {
	lockPath := LeasePath(s.runDir)
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
		return fmt.Errorf("create lock directory: %w", err)
	}
	fd, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open lock file: %w", err)
	}
	// Non-blocking exclusive flock: second start fails immediately.
	if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = fd.Close()
		return fmt.Errorf("supervisor lock already held by another process: %w", err)
	}
	s.leaseFD = int(fd.Fd())
	// Write diagnostic metadata (not authoritative; kernel lock is).
	_ = fd.Truncate(0)
	_, _ = fd.Seek(0, 0)
	meta := map[string]any{
		"pid":                 os.Getpid(),
		"process_start_token": os.Getpid(), // placeholder
		"acquired_at":         time.Now().UTC().Format(time.RFC3339),
		"run_id":              s.snapshot.Run.RunID,
	}
	metaJSON, _ := json.Marshal(meta)
	_, _ = fd.Write(append(metaJSON, '\n'))
	return nil
}

func (s *Service) releaseLease() {
	if s.leaseFD > 0 {
		_ = syscall.Flock(s.leaseFD, syscall.LOCK_UN)
		// The fd stays open; kernel releases on process exit.
		// Do NOT close the fd here — we want the kernel to hold the lock for
		// the full process lifetime. Close is done by OS on exit.
	}
	// Remove only the lock file; the fd still holds the kernel lock.
	// Only the owner should unlink the lock file.
	lockPath := LeasePath(s.runDir)
	_ = os.Remove(lockPath)
}

func SocketPath(runDir string) string {
	path := filepath.Join(runDir, "control", "supervisor.sock")
	if len(path) < 100 {
		return path
	}
	sum := sha256.Sum256([]byte(path))
	return filepath.Join(os.TempDir(), "subagent-broker-"+hex.EncodeToString(sum[:8])+".sock")
}
