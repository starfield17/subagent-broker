package doctor

import (
	"context"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/verify"
)

const SchemaVersion = "v1alpha1"

type Mode string

const (
	ModeProbe Mode = "probe"
	ModeSmoke Mode = "smoke"
)

// Config is the pure runner configuration used by the CLI and fake-adapter
// tests. TreeManager and Now are injectable so automated tests never need a
// real Harness process or network credentials.
type Config struct {
	Mode          Mode
	Harnesses     []adapter.HarnessName
	Executable    string
	Model         string
	BrokerHome    string
	Timeout       time.Duration
	KeepWorkspace bool
	Scenario      string
	TreeManager   process.TreeManager
	TreePolicy    process.TerminationPolicy
	Now           func() time.Time
}

type RunResult struct {
	SchemaVersion string          `json:"schema_version"`
	DoctorRunID   string          `json:"doctor_run_id"`
	Mode          Mode            `json:"mode"`
	EvidenceDir   string          `json:"evidence_dir"`
	Harnesses     []HarnessResult `json:"harnesses"`
}

type HarnessResult struct {
	SchemaVersion  string              `json:"schema_version"`
	DoctorRunID    string              `json:"doctor_run_id"`
	Harness        adapter.HarnessName `json:"harness"`
	AdapterVersion string              `json:"adapter_version"`
	HarnessVersion string              `json:"harness_version,omitempty"`
	StartedAt      time.Time           `json:"started_at"`
	EndedAt        time.Time           `json:"ended_at"`
	Duration       time.Duration       `json:"duration"`

	// These remain separate so a successful static probe cannot be mistaken for
	// a successful authenticated protocol smoke.
	ProbeStatus         string `json:"probe_status"`
	ProtocolSmokeStatus string `json:"protocol_smoke_status"`
	IdentityStatus      string `json:"identity_status"`
	WorkspaceStatus     string `json:"workspace_status"`
	CleanupStatus       string `json:"cleanup_status"`
	OverallStatus       string `json:"overall_status"`

	// Compatibility fields retained for existing probe consumers.
	Implemented   bool                 `json:"implemented"`
	Compatibility string               `json:"compatibility"`
	Installed     bool                 `json:"installed"`
	Authenticated *bool                `json:"authenticated,omitempty"`
	Capabilities  adapter.Capabilities `json:"capabilities"`
	Warnings      []string             `json:"warnings,omitempty"`
	Errors        []string             `json:"errors,omitempty"`

	Descriptor          adapter.Descriptor         `json:"descriptor"`
	Probe               adapter.ProbeResult        `json:"probe_result"`
	NativeSessionID     string                     `json:"native_session_id,omitempty"`
	NativeTurnID        string                     `json:"native_turn_id,omitempty"`
	ProcessIdentity     process.Identity           `json:"process_identity"`
	Stages              map[string]StageResult     `json:"stage_results,omitempty"`
	Events              []string                   `json:"normalized_event_kinds,omitempty"`
	Result              ResultEvidence             `json:"result,omitempty"`
	TaskWorker          TaskWorkerEvidence         `json:"task_worker_identity"`
	RuntimeIdentity     IdentityAssessment         `json:"runtime_identity"`
	CapabilityEvidence  adapter.CapabilityEvidence `json:"capability_evidence"`
	Workspace           WorkspaceEvidence          `json:"workspace"`
	Cleanup             CleanupEvidence            `json:"cleanup"`
	Artifacts           ArtifactPaths              `json:"artifacts"`
	normalizedEventsLog string                     `json:"-"`
	stderrLog           string                     `json:"-"`
}

type StageResult struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type ResultEvidence struct {
	Observed     bool     `json:"observed"`
	Status       string   `json:"status,omitempty"`
	SHA256       string   `json:"sha256,omitempty"`
	TaskID       string   `json:"task_id,omitempty"`
	WorkerID     string   `json:"worker_id,omitempty"`
	FilesChanged []string `json:"files_changed,omitempty"`
}

type TaskWorkerEvidence struct {
	ExpectedTaskID   string `json:"expected_task_id"`
	ObservedTaskID   string `json:"observed_task_id,omitempty"`
	ExpectedWorkerID string `json:"expected_worker_id"`
	ObservedWorkerID string `json:"observed_worker_id,omitempty"`
	TaskIDMatch      bool   `json:"task_id_match"`
	WorkerIDMatch    bool   `json:"worker_id_match"`
}

const (
	IdentityVerified      = "verified"
	IdentityMismatch      = "mismatch"
	IdentityUnavailable   = "unavailable"
	IdentityRequestedOnly = "requested_only"
)

type IdentityAssessment struct {
	RequestedModel   string                 `json:"requested_model,omitempty"`
	ObservedProvider string                 `json:"observed_provider,omitempty"`
	ObservedModel    string                 `json:"observed_model,omitempty"`
	ProviderStatus   string                 `json:"provider_status"`
	ModelStatus      string                 `json:"model_status"`
	ProviderSource   adapter.EvidenceSource `json:"provider_source,omitempty"`
	ModelSource      adapter.EvidenceSource `json:"model_source,omitempty"`
	Warnings         []string               `json:"warnings,omitempty"`
}

type WorkspaceEvidence struct {
	Root         string                   `json:"root,omitempty"`
	ChangedPaths []string                 `json:"changed_paths,omitempty"`
	Before       verify.WorkspaceSnapshot `json:"before,omitempty"`
	After        verify.WorkspaceSnapshot `json:"after,omitempty"`
	Retained     bool                     `json:"retained"`
	Removed      bool                     `json:"removed"`
}

type CleanupEvidence struct {
	AdapterTerminateAttempted bool   `json:"adapter_terminate_attempted"`
	AdapterTerminateError     string `json:"adapter_terminate_error,omitempty"`

	IdentityComplete  bool  `json:"identity_complete"`
	TreeExitConfirmed bool  `json:"tree_exit_confirmed"`
	PIDReused         bool  `json:"pid_reused"`
	RemainingPIDs     []int `json:"remaining_pids,omitempty"`
	OrphanRisk        bool  `json:"orphan_risk"`

	TerminationRequested bool     `json:"termination_requested"`
	TerminationPhase     string   `json:"termination_phase,omitempty"`
	Errors               []string `json:"errors,omitempty"`
}

type ArtifactPaths struct {
	HarnessDir       string `json:"harness_dir"`
	EvidenceJSON     string `json:"evidence_json"`
	EvidenceMarkdown string `json:"evidence_markdown"`
	NormalizedEvents string `json:"normalized_events,omitempty"`
	Stderr           string `json:"stderr,omitempty"`
	Workspace        string `json:"workspace,omitempty"`
}

// Runner is the small injectable boundary used by command-level tests.
type Runner interface {
	Run(context.Context, Config) (RunResult, error)
}

type AdapterRunner struct {
	Registry *adapter.Registry
}

func (r AdapterRunner) Run(ctx context.Context, cfg Config) (RunResult, error) {
	return Run(ctx, r.Registry, cfg)
}
