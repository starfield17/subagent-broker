package fake

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/report"
)

type Scenario struct {
	Name     string
	Events   []adapter.NativeEvent
	Final    *report.Envelope
	Diff     []string
	Usage    adapter.Usage
	KeepOpen bool
	ExitCode int
}

type sessionState struct {
	session adapter.Session
	events  chan adapter.NativeEvent
	done    chan struct{}
	history []adapter.NativeEvent
	final   *report.Envelope
	diff    []string
	usage   adapter.Usage
	closed  bool
}

type Adapter struct {
	mu           sync.Mutex
	capabilities adapter.Capabilities
	scenarios    map[string]Scenario
	sessions     map[string]*sessionState
	counter      atomic.Uint64
}

func New(capabilities adapter.Capabilities) *Adapter {
	return &Adapter{capabilities: capabilities, scenarios: BuiltinScenarios(), sessions: map[string]*sessionState{}}
}

func (a *Adapter) Descriptor() adapter.Descriptor {
	return adapter.Descriptor{Name: adapter.HarnessFake, AdapterVersion: "1.0.0", RuntimeImplemented: true, Compatibility: "verified", Capabilities: a.capabilities}
}

// SessionConfigFact reports what the fake installs. Steer is considered
// contract-verified for tests that declare SteerActiveTurn (fake models true immediate).
func (a *Adapter) SessionConfigFact(req adapter.StartRequest) adapter.SessionConfigFact {
	safe := strings.EqualFold(req.Options["safe_mode"], "true")
	hooks := a.capabilities.Hooks && !safe
	return adapter.SessionConfigFact{
		PermissionMode:   req.Options["permission_mode"],
		HooksInstalled:   hooks && a.capabilities.PermissionEvents,
		MCPEnabled:       !safe,
		SafeMode:         safe,
		SteerVerified:    a.capabilities.SteerActiveTurn,
		NextTurnDelivery: !a.capabilities.SteerActiveTurn && a.capabilities.BidirectionalStream,
	}
}

func (a *Adapter) Probe(context.Context, adapter.ProbeRequest) (adapter.ProbeResult, error) {
	auth := true
	return adapter.ProbeResult{Installed: true, Version: "fake-1.0.0", Authenticated: &auth, Capabilities: a.capabilities, Compatibility: "verified"}, nil
}

func (a *Adapter) RegisterScenario(s Scenario) error {
	if s.Name == "" {
		return fmt.Errorf("scenario name is required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.scenarios[s.Name] = s
	return nil
}

func (a *Adapter) StartSession(ctx context.Context, req adapter.StartRequest) (adapter.Session, error) {
	a.mu.Lock()
	scenario, ok := a.scenarios[req.Scenario]
	if !ok {
		a.mu.Unlock()
		return adapter.Session{}, fmt.Errorf("unknown fake scenario %q", req.Scenario)
	}
	n := a.counter.Add(1)
	id := fmt.Sprintf("fake-session-%d", n)
	channel := make(chan adapter.NativeEvent, len(scenario.Events)+1)
	exited := make(chan adapter.ExitStatus, 1)
	// Synthetic process identity: PID is chosen so it does not refer to a live
	// process. Supervisor fills ProcessGroupToken; Controller then confirms
	// "tree gone" after Exited (Inspect returns not-found).
	fakePID := int(1_000_000_000 + n)
	session := adapter.Session{
		NativeSessionID: id, NativeTurnID: "turn-1", Events: channel, Exited: exited,
		PID: fakePID, ProcessStartToken: "fake-start-" + id,
	}
	final := cloneEnvelope(scenario.Final)
	if final != nil {
		if req.TaskID != "" {
			final.TaskID = req.TaskID
		}
		if req.WorkerID != "" {
			final.WorkerID = req.WorkerID
		}
	}
	state := &sessionState{session: session, events: channel, done: make(chan struct{}), final: final, diff: append([]string(nil), scenario.Diff...), usage: scenario.Usage}
	a.sessions[id] = state
	a.mu.Unlock()

	go func() {
		defer func() {
			exited <- adapter.ExitStatus{Code: scenario.ExitCode}
			close(exited)
			a.mu.Lock()
			if !state.closed {
				close(channel)
				state.closed = true
			}
			a.mu.Unlock()
		}()
		for _, native := range scenario.Events {
			select {
			case <-ctx.Done():
				return
			case <-state.done:
				return
			case channel <- native:
				a.mu.Lock()
				state.history = append(state.history, native)
				a.mu.Unlock()
			}
		}
		if scenario.KeepOpen {
			select {
			case <-ctx.Done():
			case <-state.done:
			}
		}
	}()
	return session, nil
}

func (a *Adapter) ResumeSession(ctx context.Context, req adapter.ResumeRequest) (adapter.Session, error) {
	if !a.capabilities.ResumeSession {
		return adapter.Session{}, adapter.ErrUnsupported
	}
	a.mu.Lock()
	state, ok := a.sessions[req.NativeSessionID]
	if !ok {
		a.mu.Unlock()
		return adapter.Session{}, fmt.Errorf("unknown session %q", req.NativeSessionID)
	}
	// Open a fresh event/exit stream for the resumed turn so callers can drive a
	// full Worker Session lifecycle (Events → result → Exited).
	channel := make(chan adapter.NativeEvent, 8)
	exited := make(chan adapter.ExitStatus, 1)
	// Keep PID/start-token so supervisor can form a complete process identity.
	pid := state.session.PID
	startTok := state.session.ProcessStartToken
	if pid <= 0 || startTok == "" {
		pid = int(1_000_000_000 + a.counter.Add(1))
		startTok = "fake-start-resume-" + req.NativeSessionID
	}
	session := adapter.Session{
		NativeSessionID:   req.NativeSessionID,
		NativeTurnID:      "turn-resume",
		Events:            channel,
		Exited:            exited,
		PID:               pid,
		ProcessStartToken: startTok,
	}
	if state.final == nil {
		// Ensure resume always has a collectable result for full lifecycle tests.
		final := report.Envelope{
			SchemaVersion: report.SchemaVersion, TaskID: req.TaskID, WorkerID: req.WorkerID,
			Status: report.StatusSucceeded, Summary: "fake resume completed", WorkCompleted: []string{"resumed work"},
			NoFilesChangedReason: "scenario does not edit a real workspace",
			Validation:           []report.Validation{{Command: "fake-check", Passed: true}},
		}
		state.final = &final
	} else {
		if req.TaskID != "" {
			state.final.TaskID = req.TaskID
		}
		if req.WorkerID != "" {
			state.final.WorkerID = req.WorkerID
		}
	}
	state.session = session
	state.events = channel
	state.closed = false
	state.done = make(chan struct{})
	a.mu.Unlock()

	now := time.Now().UTC()
	events := []adapter.NativeEvent{
		{Kind: "session.resumed", Timestamp: now},
		{Kind: "turn.started", Timestamp: now},
		{Kind: "result.submitted", Timestamp: now},
	}
	go func() {
		defer func() {
			exited <- adapter.ExitStatus{Code: 0}
			close(exited)
			a.mu.Lock()
			if !state.closed {
				close(channel)
				state.closed = true
			}
			a.mu.Unlock()
		}()
		for _, native := range events {
			select {
			case <-ctx.Done():
				return
			case <-state.done:
				return
			case channel <- native:
				a.mu.Lock()
				state.history = append(state.history, native)
				a.mu.Unlock()
			}
		}
	}()
	return session, nil
}

func (a *Adapter) SendMessage(_ context.Context, id, message string) (adapter.DeliveryResult, error) {
	if !a.capabilities.BidirectionalStream {
		if a.capabilities.ResumeSession {
			return adapter.DeliveryResult{Mode: adapter.DeliveryResume}, nil
		}
		return adapter.DeliveryResult{Mode: adapter.DeliveryUnsupported}, adapter.ErrUnsupported
	}
	if _, err := a.requireSession(id); err != nil {
		return adapter.DeliveryResult{}, err
	}
	return adapter.DeliveryResult{Mode: adapter.DeliveryNextTurn, MessageID: message}, nil
}

func (a *Adapter) SteerActiveTurn(_ context.Context, id, message string) (adapter.DeliveryResult, error) {
	if !a.capabilities.SteerActiveTurn {
		return adapter.DeliveryResult{Mode: adapter.DeliveryUnsupported}, adapter.ErrUnsupported
	}
	if _, err := a.requireSession(id); err != nil {
		return adapter.DeliveryResult{}, err
	}
	return adapter.DeliveryResult{Mode: adapter.DeliveryImmediate, MessageID: message}, nil
}

func (a *Adapter) InterruptTurn(_ context.Context, id string) error {
	if !a.capabilities.InterruptTurn {
		return adapter.ErrUnsupported
	}
	_, err := a.requireSession(id)
	return err
}

func (a *Adapter) TerminateSession(_ context.Context, id string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	state, ok := a.sessions[id]
	if !ok {
		return fmt.Errorf("unknown session %q", id)
	}
	if !state.closed {
		select {
		case <-state.done:
		default:
			close(state.done)
		}
	}
	return nil
}

func (a *Adapter) ReadHistory(_ context.Context, id string) ([]adapter.NativeEvent, error) {
	if !a.capabilities.SessionHistory {
		return nil, adapter.ErrUnsupported
	}
	state, err := a.requireSession(id)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]adapter.NativeEvent(nil), state.history...), nil
}

func (a *Adapter) RespondPermission(_ context.Context, id string, _ adapter.PermissionDecision) error {
	if !a.capabilities.PermissionEvents {
		return adapter.ErrUnsupported
	}
	_, err := a.requireSession(id)
	return err
}

func (a *Adapter) GetDiff(_ context.Context, id string) ([]string, error) {
	if !a.capabilities.DiffEvents {
		return nil, adapter.ErrUnsupported
	}
	state, err := a.requireSession(id)
	if err != nil {
		return nil, err
	}
	return append([]string(nil), state.diff...), nil
}

func (a *Adapter) GetUsage(_ context.Context, id string) (adapter.Usage, error) {
	if !a.capabilities.UsageEvents {
		return adapter.Usage{}, adapter.ErrUnsupported
	}
	state, err := a.requireSession(id)
	if err != nil {
		return adapter.Usage{}, err
	}
	return state.usage, nil
}

func (a *Adapter) NormalizeEvent(native adapter.NativeEvent) (event.Input, error) {
	if native.Kind == "fake.raw_partial_json" {
		return event.Input{}, fmt.Errorf("incomplete native JSON")
	}
	var payload any
	if len(native.Payload) > 0 {
		if err := json.Unmarshal(native.Payload, &payload); err != nil {
			return event.Input{}, err
		}
	}
	return event.Input{Timestamp: native.Timestamp, Source: "fake", Type: native.Kind, Severity: "info", Payload: payload}, nil
}

func (a *Adapter) CollectFinalResult(_ context.Context, id string) (report.Envelope, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return report.Envelope{}, err
	}
	if state.final == nil {
		return report.Envelope{}, fmt.Errorf("session %q has no final result", id)
	}
	return *state.final, nil
}

func (a *Adapter) requireSession(id string) (*sessionState, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state, ok := a.sessions[id]
	if !ok {
		return nil, fmt.Errorf("unknown session %q", id)
	}
	return state, nil
}

func cloneEnvelope(source *report.Envelope) *report.Envelope {
	if source == nil {
		return nil
	}
	copy := *source
	return &copy
}

func BuiltinScenarios() map[string]Scenario {
	now := time.Unix(0, 0).UTC()
	e := func(kind string, payload any) adapter.NativeEvent {
		raw, _ := json.Marshal(payload)
		return adapter.NativeEvent{Kind: kind, Timestamp: now, Payload: raw}
	}
	valid := report.Envelope{SchemaVersion: report.SchemaVersion, TaskID: "task-fake", WorkerID: "worker-fake", Status: report.StatusSucceeded, Summary: "fake completed", WorkCompleted: []string{"scripted work"}, NoFilesChangedReason: "scenario does not edit a real workspace", Validation: []report.Validation{{Command: "fake-check", Passed: true}}}
	invalid := valid
	invalid.Summary = ""
	scenarios := map[string]Scenario{
		"normal_stream":      {Name: "normal_stream", Events: []adapter.NativeEvent{e("turn.started", nil), e("model.output_delta", map[string]string{"text": "working"}), e("turn.completed", nil), e("result.submitted", nil)}, Final: &valid},
		"long_thinking":      {Name: "long_thinking", Events: []adapter.NativeEvent{e("turn.started", nil), e("protocol.thinking", nil)}, KeepOpen: true},
		"long_command_quiet": {Name: "long_command_quiet", Events: []adapter.NativeEvent{e("tool.started", map[string]string{"command": "go test ./..."})}, KeepOpen: true},
		"waiting_permission": {Name: "waiting_permission", Events: []adapter.NativeEvent{e("permission.requested", nil)}, KeepOpen: true},
		"waiting_user":       {Name: "waiting_user", Events: []adapter.NativeEvent{e("user_input.requested", nil)}, KeepOpen: true},
		"scope_request":      {Name: "scope_request", Events: []adapter.NativeEvent{e("scope_expansion.requested", map[string]any{"paths": []string{"go.mod"}})}, KeepOpen: true},
		"nonzero_exit":       {Name: "nonzero_exit", Events: []adapter.NativeEvent{e("process.exited", map[string]int{"exit_code": 1})}, ExitCode: 1},
		"stalled":            {Name: "stalled", Events: []adapter.NativeEvent{e("session.started", nil)}, KeepOpen: true},
		"orphan_child":       {Name: "orphan_child", Events: []adapter.NativeEvent{e("process.orphaned", nil)}, KeepOpen: true},
		"partial_json":       {Name: "partial_json", Events: []adapter.NativeEvent{{Kind: "fake.raw_partial_json", Timestamp: now, Payload: []byte(`{"incomplete"`)}}, KeepOpen: false},
		"resume":             {Name: "resume", Events: []adapter.NativeEvent{e("session.resumed", nil)}, KeepOpen: true},
		"active_steer":       {Name: "active_steer", Events: []adapter.NativeEvent{e("turn.started", nil)}, KeepOpen: true},
		"invalid_result":     {Name: "invalid_result", Events: []adapter.NativeEvent{e("result.submitted", nil)}, Final: &invalid},
		"supervisor_crash":   {Name: "supervisor_crash", Events: []adapter.NativeEvent{e("system.supervisor_crash", nil)}, KeepOpen: true},
		"pid_reuse":          {Name: "pid_reuse", Events: []adapter.NativeEvent{e("process.pid_reused", map[string]string{"old_start": "a", "new_start": "b"})}},
	}
	return scenarios
}

func ScenarioNames() []string {
	m := BuiltinScenarios()
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
