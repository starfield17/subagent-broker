package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/report"
)

type HarnessName string

const (
	HarnessClaudeCode HarnessName = "claude-code"
	HarnessCodex      HarnessName = "codex"
	HarnessGrokBuild  HarnessName = "grok-build"
	HarnessOpenCode   HarnessName = "opencode"
	HarnessFake       HarnessName = "fake"
)

type Capabilities struct {
	StructuredStream      bool `json:"structured_stream"`
	BidirectionalStream   bool `json:"bidirectional_stream"`
	ResumeSession         bool `json:"resume_session"`
	SteerActiveTurn       bool `json:"steer_active_turn"`
	InterruptTurn         bool `json:"interrupt_turn"`
	StructuredFinalOutput bool `json:"structured_final_output"`
	PermissionEvents      bool `json:"permission_events"`
	DiffEvents            bool `json:"diff_events"`
	UsageEvents           bool `json:"usage_events"`
	NativeSubagents       bool `json:"native_subagents"`
	NativeServerMode      bool `json:"native_server_mode"`
	ACP                   bool `json:"acp"`
	Hooks                 bool `json:"hooks"`
	SessionHistory        bool `json:"session_history"`
}

type Descriptor struct {
	Name               HarnessName  `json:"name"`
	AdapterVersion     string       `json:"adapter_version"`
	TestedMinVersion   string       `json:"tested_min_version,omitempty"`
	TestedMaxVersion   string       `json:"tested_max_version,omitempty"`
	KnownIncompatible  []string     `json:"known_incompatible_versions,omitempty"`
	Capabilities       Capabilities `json:"capabilities"`
	RuntimeImplemented bool         `json:"runtime_implemented"`
	Compatibility      string       `json:"compatibility"`
}

type ProbeRequest struct {
	Executable string `json:"executable,omitempty"`
}

// ProbeResult is returned by Adapter.Probe for environment preflight.
// Compatibility values used by Wave preflight include:
// verified, compatibility_unverified, incompatible, probe_failed, unavailable.
type ProbeResult struct {
	Installed     bool         `json:"installed"`
	Version       string       `json:"version,omitempty"`
	Authenticated *bool        `json:"authenticated,omitempty"`
	Capabilities  Capabilities `json:"capabilities"`
	Compatibility string       `json:"compatibility"`
	Warnings      []string     `json:"warnings,omitempty"`
}

type StartRequest struct {
	RunID       string            `json:"run_id"`
	TaskID      string            `json:"task_id"`
	WorkerID    string            `json:"worker_id"`
	ProjectRoot string            `json:"project_root"`
	Contract    string            `json:"contract"`
	Model       string            `json:"model,omitempty"`
	Scenario    string            `json:"scenario,omitempty"`
	Options     map[string]string `json:"options,omitempty"`
	Interaction InteractionConfig `json:"interaction,omitempty"`
}

type InteractionConfig struct {
	Enabled          bool   `json:"enabled"`
	BrokerExecutable string `json:"broker_executable,omitempty"`
	RunDir           string `json:"run_dir,omitempty"`
}

type ResumeRequest struct {
	NativeSessionID string            `json:"native_session_id"`
	RunID           string            `json:"run_id"`
	TaskID          string            `json:"task_id"`
	WorkerID        string            `json:"worker_id"`
	ProjectRoot     string            `json:"project_root"`
	Contract        string            `json:"contract"`
	Model           string            `json:"model,omitempty"`
	Options         map[string]string `json:"options,omitempty"`
	Interaction     InteractionConfig `json:"interaction,omitempty"`
}

type OutputChunk struct {
	Timestamp time.Time
	Data      []byte
}

type Session struct {
	NativeSessionID   string             `json:"native_session_id"`
	NativeTurnID      string             `json:"native_turn_id,omitempty"`
	PID               int                `json:"pid,omitempty"`
	ProcessStartToken string             `json:"process_start_token,omitempty"`
	Events            <-chan NativeEvent `json:"-"`
	Exited            <-chan ExitStatus  `json:"-"`
	Stderr            <-chan OutputChunk `json:"-"`
}

type ExitStatus struct {
	Code   int    `json:"code"`
	Signal string `json:"signal,omitempty"`
	Error  string `json:"error,omitempty"`
}

type NativeEvent struct {
	Kind      string          `json:"kind"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type DeliveryMode string

const (
	DeliveryImmediate   DeliveryMode = "immediate"
	DeliveryNextTurn    DeliveryMode = "next_turn"
	DeliveryResume      DeliveryMode = "resume"
	DeliveryUnsupported DeliveryMode = "unsupported"
)

type DeliveryResult struct {
	Mode      DeliveryMode `json:"mode"`
	MessageID string       `json:"message_id,omitempty"`
}

type PermissionDecision struct {
	RequestID string `json:"request_id"`
	Allowed   bool   `json:"allowed"`
	Reason    string `json:"reason,omitempty"`
	// OptionID is the opaque native option selected for ACP-style responses.
	// Empty for Codex/OpenCode decision shapes that do not use option IDs.
	OptionID string `json:"option_id,omitempty"`
}

type Usage struct {
	InputTokens  int64   `json:"input_tokens,omitempty"`
	OutputTokens int64   `json:"output_tokens,omitempty"`
	Cost         float64 `json:"cost,omitempty"`
	Currency     string  `json:"currency,omitempty"`
}

var ErrUnsupported = errors.New("adapter capability unsupported")

type Adapter interface {
	Descriptor() Descriptor
	Probe(context.Context, ProbeRequest) (ProbeResult, error)
	StartSession(context.Context, StartRequest) (Session, error)
	ResumeSession(context.Context, ResumeRequest) (Session, error)
	SendMessage(context.Context, string, string) (DeliveryResult, error)
	SteerActiveTurn(context.Context, string, string) (DeliveryResult, error)
	InterruptTurn(context.Context, string) error
	TerminateSession(context.Context, string) error
	ReadHistory(context.Context, string) ([]NativeEvent, error)
	RespondPermission(context.Context, string, PermissionDecision) error
	GetDiff(context.Context, string) ([]string, error)
	GetUsage(context.Context, string) (Usage, error)
	NormalizeEvent(NativeEvent) (event.Input, error)
	CollectFinalResult(context.Context, string) (report.Envelope, error)
}

type Registry struct {
	mu       sync.RWMutex
	adapters map[HarnessName]Adapter
}

func NewRegistry() *Registry {
	return &Registry{adapters: map[HarnessName]Adapter{}}
}

func (r *Registry) Register(a Adapter) error {
	if a == nil {
		return fmt.Errorf("adapter is nil")
	}
	d := a.Descriptor()
	if d.Name == "" {
		return fmt.Errorf("adapter name is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.adapters[d.Name]; exists {
		return fmt.Errorf("adapter %q already registered", d.Name)
	}
	r.adapters[d.Name] = a
	return nil
}

func (r *Registry) Get(name HarnessName) (Adapter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.adapters[name]
	return a, ok
}

func (r *Registry) Descriptors() []Descriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Descriptor, 0, len(r.adapters))
	for _, a := range r.adapters {
		out = append(out, a.Descriptor())
	}
	return out
}
