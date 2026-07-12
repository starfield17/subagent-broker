package domain

import (
	"encoding/json"
	"time"

	"github.com/vnai/subagent-broker/internal/state"
)

type ProjectID string
type RunID string
type WaveID string
type TaskID string
type WorkerID string

type Project struct {
	ProjectID     ProjectID `json:"project_id"`
	CanonicalPath string    `json:"canonical_path"`
	GitRoot       string    `json:"git_root,omitempty"`
	GitCommonDir  string    `json:"git_common_dir,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	LastSeenAt    time.Time `json:"last_seen_at"`
	PathSlug      string    `json:"path_slug"`
	PathHash      string    `json:"path_hash"`
}

type RunStatus string

const (
	RunPlanned    RunStatus = "planned"
	RunStarting   RunStatus = "starting"
	RunRunning    RunStatus = "running"
	RunBarrier    RunStatus = "barrier"
	RunCompleted  RunStatus = "completed"
	RunFailed     RunStatus = "failed"
	RunCancelled  RunStatus = "cancelled"
	RunRecovering RunStatus = "recovering"
	RunDegraded   RunStatus = "degraded"
)

type WorktreeSnapshot struct {
	Revision       string   `json:"revision,omitempty"`
	ChangedFiles   []string `json:"changed_files,omitempty"`
	UntrackedFiles []string `json:"untracked_files,omitempty"`
}

type SupervisorIdentity struct {
	PID               int       `json:"pid"`
	ProcessStartToken string    `json:"process_start_token"`
	Executable        string    `json:"executable"`
	ExecutableVersion string    `json:"executable_version"`
	IPCEndpoint       string    `json:"ipc_endpoint"`
	HeartbeatAt       time.Time `json:"heartbeat_at"`
	ShutdownReason    string    `json:"shutdown_reason,omitempty"`
}

type Run struct {
	RunID                RunID               `json:"run_id"`
	ProjectID            ProjectID           `json:"project_id"`
	CreatedAt            time.Time           `json:"created_at"`
	StartedAt            *time.Time          `json:"started_at,omitempty"`
	EndedAt              *time.Time          `json:"ended_at,omitempty"`
	Goal                 string              `json:"goal"`
	BaseRevision         string              `json:"base_revision,omitempty"`
	BaseWorktreeSnapshot WorktreeSnapshot    `json:"base_worktree_snapshot"`
	Status               RunStatus           `json:"status"`
	CurrentWave          WaveID              `json:"current_wave,omitempty"`
	WaveIDs              []WaveID            `json:"wave_ids,omitempty"`
	TaskIDs              []TaskID            `json:"task_ids"`
	SupervisorIdentity   *SupervisorIdentity `json:"supervisor_identity,omitempty"`
	ConfigSnapshot       json.RawMessage     `json:"config_snapshot"`
	SchemaVersion        string              `json:"schema_version"`
}

type RunPlan struct {
	SchemaVersion string              `json:"schema_version"`
	Waves         []WavePlan          `json:"waves"`
	FinalChecks   []ValidationCommand `json:"final_checks,omitempty"`
}

type WavePlan struct {
	WaveID            WaveID              `json:"wave_id"`
	IntegrationChecks []ValidationCommand `json:"integration_checks,omitempty"`
	Tasks             []Task              `json:"tasks"`
}

type WaveStatus string

const (
	WavePlanned   WaveStatus = "planned"
	WavePreflight WaveStatus = "preflight"
	WaveRunning   WaveStatus = "running"
	WaveWaiting   WaveStatus = "waiting"
	WaveBarrier   WaveStatus = "barrier"
	WaveVerified  WaveStatus = "verified"
	WaveBlocked   WaveStatus = "blocked"
	WaveFailed    WaveStatus = "failed"
	WaveCancelled WaveStatus = "cancelled"
)

type BarrierResult string

const (
	BarrierPassed             BarrierResult = "passed"
	BarrierPassedWithWarnings BarrierResult = "passed_with_warnings"
	BarrierBlocked            BarrierResult = "blocked"
	BarrierFailed             BarrierResult = "failed"
	BarrierCancelled          BarrierResult = "cancelled"
)

type ValidationCommand struct {
	Command string `json:"command"`
	Scope   string `json:"scope,omitempty"`
}

type Wave struct {
	WaveID            WaveID              `json:"wave_id"`
	Ordinal           int                 `json:"ordinal"`
	TaskIDs           []TaskID            `json:"task_ids"`
	Status            WaveStatus          `json:"status"`
	StartedAt         *time.Time          `json:"started_at,omitempty"`
	BarrierStartedAt  *time.Time          `json:"barrier_started_at,omitempty"`
	EndedAt           *time.Time          `json:"ended_at,omitempty"`
	IntegrationChecks []ValidationCommand `json:"integration_checks"`
	BarrierResult     BarrierResult       `json:"barrier_result,omitempty"`
	BarrierAccepted   bool                `json:"barrier_accepted,omitempty"`
	BarrierReason     string              `json:"barrier_acceptance_reason,omitempty"`
}

type Task struct {
	TaskID                     TaskID              `json:"task_id"`
	Title                      string              `json:"title"`
	Objective                  string              `json:"objective"`
	CompletionCriteria         []string            `json:"completion_criteria"`
	WriteScope                 []string            `json:"write_scope"`
	ForbiddenScope             []string            `json:"forbidden_scope"`
	KnownReadDependencies      []string            `json:"known_read_dependencies"`
	ParallelResponsibilities   map[TaskID]string   `json:"parallel_responsibilities"`
	ValidationCommands         []ValidationCommand `json:"validation_commands"`
	HarnessPreference          string              `json:"harness_preference,omitempty"`
	ModelPreference            string              `json:"model_preference,omitempty"`
	DependsOn                  []TaskID            `json:"depends_on"`
	WaveID                     WaveID              `json:"wave_id"`
	Status                     state.Task          `json:"status"`
	ResultRevision             string              `json:"result_revision,omitempty"`
	AllowNestedAgents          bool                `json:"allow_nested_agents"`
	AllowPublicInterfaceChange bool                `json:"allow_public_interface_change"`
	ProjectRoot                string              `json:"project_root"`
}

type WorkerSession struct {
	WorkerID             WorkerID        `json:"worker_id"`
	TaskID               TaskID          `json:"task_id"`
	Harness              string          `json:"harness"`
	AdapterVersion       string          `json:"adapter_version"`
	NativeSessionID      string          `json:"native_session_id,omitempty"`
	NativeTurnID         string          `json:"native_turn_id,omitempty"`
	PID                  int             `json:"pid,omitempty"`
	ProcessStartToken    string          `json:"process_start_token,omitempty"`
	ProcessGroupIdentity string          `json:"process_group_identity,omitempty"`
	StartedAt            time.Time       `json:"started_at"`
	LastEventAt          time.Time       `json:"last_event_at"`
	LastProgressAt       time.Time       `json:"last_progress_at"`
	EndedAt              *time.Time      `json:"ended_at,omitempty"`
	ExitCode             *int            `json:"exit_code,omitempty"`
	Capabilities         map[string]bool `json:"capabilities"` // EffectiveCapabilities map form
	// Capability layers for audit (JSON-friendly maps).
	DeclaredCapabilities   map[string]bool  `json:"declared_capabilities,omitempty"`
	ProbeCapabilities      map[string]bool  `json:"probe_capabilities,omitempty"`
	ConfiguredCapabilities map[string]bool  `json:"configured_capabilities,omitempty"`
	CapabilityDowngrades   []string         `json:"capability_downgrades,omitempty"`
	PermissionMode         string           `json:"permission_mode,omitempty"`
	HooksInstalled         bool             `json:"hooks_installed,omitempty"`
	Attempt                int              `json:"attempt"`
	AttemptMode            string           `json:"attempt_mode,omitempty"`
	StatusDimensions       state.Dimensions `json:"status_dimensions"`
}
