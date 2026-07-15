package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/protocol"
	"github.com/vnai/subagent-broker/internal/adapter/transport"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/report"
)

type Adapter struct {
	executable string
	mu         sync.Mutex
	sessions   map[string]*sessionState
	counter    atomic.Uint64
}

type sessionState struct {
	mu              sync.Mutex
	process         *transport.Process
	threadID        string
	turnID          string
	workerID        string
	stream          *protocol.EventStream
	resultReady     chan struct{}
	resultOnce      sync.Once
	shutdown        chan struct{}
	shutdownOnce    sync.Once
	history         []adapter.NativeEvent
	final           []byte
	output          strings.Builder
	resultError     string
	usage           adapter.Usage
	diff            []string
	nextID          int64
	pending         map[int64]chan rpcMessage
	serverRequest   map[string]struct{}
	agentPhases     map[string]string
	runtimeIdentity adapter.RuntimeIdentity
	// terminalCommitted is true after exactly one terminal boundary has been
	// recorded in history (ResultSubmitted or TurnFailed). Prevents double commit
	// when process EOF races with a protocol terminal notification.
	terminalCommitted bool
}

// codexNotification separates ordinary stream events from terminal boundaries.
// Terminal notifications use recordTerminalEvent so history is committed before
// resultReady unblocks CollectFinalResult / GetUsage.
type codexNotification struct {
	Event    adapter.NativeEvent
	Terminal bool
}

type rpcMessage struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  json.RawMessage `json:"error,omitempty"`
}

func New(executable string) *Adapter {
	if strings.TrimSpace(executable) == "" {
		executable = "codex"
	}
	return &Adapter{executable: executable, sessions: map[string]*sessionState{}}
}

func (a *Adapter) Descriptor() adapter.Descriptor { return Descriptor() }

func (a *Adapter) Probe(ctx context.Context, req adapter.ProbeRequest) (adapter.ProbeResult, error) {
	executable := a.executable
	if strings.TrimSpace(req.Executable) != "" {
		executable = req.Executable
	}
	versionOutput, err := protocol.CommandOutput(ctx, executable, "--version")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || strings.Contains(strings.ToLower(err.Error()), "executable file not found") {
			return adapter.ProbeResult{Installed: false, Compatibility: "unavailable", Capabilities: Descriptor().Capabilities}, nil
		}
		return adapter.ProbeResult{Installed: true, Compatibility: "probe_failed", Warnings: []string{err.Error()}, Capabilities: Descriptor().Capabilities}, nil
	}
	version := protocol.ParseVersion(versionOutput)
	authenticated := protocol.Bool(false)
	status, statusErr := protocol.CommandOutput(ctx, executable, "login", "status")
	if statusErr == nil && strings.Contains(strings.ToLower(string(status)), "logged in") {
		*authenticated = true
	}
	compatibility := "verified"
	if version != "0.144.1" {
		compatibility = "compatibility_unverified"
	}
	return adapter.ProbeResult{Installed: true, Version: version, Authenticated: authenticated, Capabilities: Descriptor().Capabilities, Compatibility: compatibility}, nil
}

func (a *Adapter) StartSession(ctx context.Context, req adapter.StartRequest) (adapter.Session, error) {
	return a.start(ctx, req, "")
}

func (a *Adapter) ResumeSession(ctx context.Context, req adapter.ResumeRequest) (adapter.Session, error) {
	if strings.TrimSpace(req.NativeSessionID) == "" {
		return adapter.Session{}, fmt.Errorf("native Codex thread id is required")
	}
	return a.start(ctx, adapter.StartRequest{
		RunID: req.RunID, TaskID: req.TaskID, WorkerID: req.WorkerID,
		ProjectRoot: req.ProjectRoot, Contract: req.Contract, Model: req.Model,
		Options: req.Options, Interaction: req.Interaction,
	}, req.NativeSessionID)
}

func (a *Adapter) start(ctx context.Context, req adapter.StartRequest, resumeID string) (adapter.Session, error) {
	args := []string{"app-server", "--listen", "stdio://"}
	p, err := transport.Start(ctx, a.executable, args, req.ProjectRoot)
	if err != nil {
		return adapter.Session{}, fmt.Errorf("start Codex app-server: %w", err)
	}
	state := &sessionState{
		process: p, workerID: req.WorkerID,
		stream:      protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 256, ProgressQueueLimit: 256}),
		resultReady: make(chan struct{}), shutdown: make(chan struct{}),
		pending: map[int64]chan rpcMessage{}, serverRequest: map[string]struct{}{}, agentPhases: map[string]string{},
		runtimeIdentity: adapter.RuntimeIdentity{
			RequestedModel: req.Model,
			ProviderSource: adapter.EvidenceUnavailable,
			ModelSource:    adapter.EvidenceUnavailable,
		},
	}
	go a.readProcess(state)
	if err := a.initialize(ctx, state); err != nil {
		_ = p.CloseInput()
		return adapter.Session{}, err
	}
	threadID := resumeID
	approvalPolicy := codexApprovalPolicy(req)
	if threadID == "" {
		result, err := a.request(ctx, state, "thread/start", map[string]any{
			"cwd": req.ProjectRoot, "approvalPolicy": approvalPolicy, "sandbox": "workspace-write", "model": nil,
		})
		if err != nil {
			_ = p.CloseInput()
			return adapter.Session{}, fmt.Errorf("Codex thread/start: %w", err)
		}
		threadID = responseID(result)
		if threadID == "" {
			return adapter.Session{}, fmt.Errorf("Codex thread/start returned no thread id")
		}
	} else {
		if _, err := a.request(ctx, state, "thread/resume", map[string]any{
			"threadId": threadID, "cwd": req.ProjectRoot, "approvalPolicy": approvalPolicy,
		}); err != nil {
			_ = p.CloseInput()
			return adapter.Session{}, fmt.Errorf("Codex thread/resume: %w", err)
		}
	}
	state.mu.Lock()
	state.threadID = threadID
	state.mu.Unlock()
	turnID, err := a.startTurn(ctx, state, req)
	if err != nil {
		_ = p.CloseInput()
		return adapter.Session{}, err
	}
	state.mu.Lock()
	state.turnID = turnID
	state.mu.Unlock()
	a.mu.Lock()
	a.sessions[threadID] = state
	a.mu.Unlock()
	return sessionFromState(state), nil
}

func (a *Adapter) initialize(ctx context.Context, state *sessionState) error {
	if _, err := a.request(ctx, state, "initialize", map[string]any{
		"clientInfo":   map[string]string{"name": "subagent-broker", "title": "Subagent Broker", "version": "phase4"},
		"capabilities": map[string]any{"experimentalApi": true, "optOutNotificationMethods": []string{}},
	}); err != nil {
		return fmt.Errorf("Codex initialize: %w", err)
	}
	return state.process.WriteJSON(map[string]any{"method": "initialized", "params": map[string]any{}})
}

func (a *Adapter) startTurn(ctx context.Context, state *sessionState, req adapter.StartRequest) (string, error) {
	result, err := a.request(ctx, state, "turn/start", map[string]any{
		"threadId": state.threadID,
		"cwd":      req.ProjectRoot,
		"model":    nullable(req.Model),
		"input":    []map[string]string{{"type": "text", "text": req.Contract}},
	})
	if err != nil {
		return "", fmt.Errorf("Codex turn/start: %w", err)
	}
	turnID := responseTurnID(result)
	if turnID == "" {
		turnID = "turn-unknown"
	}
	return turnID, nil
}

func (a *Adapter) SendMessage(ctx context.Context, id, message string) (adapter.DeliveryResult, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return adapter.DeliveryResult{}, err
	}
	_, err = a.request(ctx, state, "turn/start", map[string]any{
		"threadId": state.threadID, "input": []map[string]string{{"type": "text", "text": message}},
	})
	if err != nil {
		return adapter.DeliveryResult{}, err
	}
	return adapter.DeliveryResult{Mode: adapter.DeliveryNextTurn, MessageID: message}, nil
}

func (a *Adapter) SteerActiveTurn(ctx context.Context, id, message string) (adapter.DeliveryResult, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return adapter.DeliveryResult{}, err
	}
	state.mu.Lock()
	threadID, turnID := state.threadID, state.turnID
	state.mu.Unlock()
	if turnID == "" || turnID == "turn-unknown" {
		return adapter.DeliveryResult{Mode: adapter.DeliveryUnsupported}, adapter.ErrUnsupported
	}
	if _, err := a.request(ctx, state, "turn/steer", map[string]any{
		"threadId": threadID, "expectedTurnId": turnID,
		"input": []map[string]string{{"type": "text", "text": message}},
	}); err != nil {
		return adapter.DeliveryResult{}, err
	}
	return adapter.DeliveryResult{Mode: adapter.DeliveryImmediate, MessageID: message}, nil
}

func (a *Adapter) InterruptTurn(ctx context.Context, id string) error {
	state, err := a.requireSession(id)
	if err != nil {
		return err
	}
	state.mu.Lock()
	threadID, turnID := state.threadID, state.turnID
	state.mu.Unlock()
	if _, err := a.request(ctx, state, "turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}); err != nil {
		return err
	}
	return nil
}

func (a *Adapter) TerminateSession(ctx context.Context, id string) error {
	state, err := a.requireSession(id)
	if err != nil {
		return err
	}
	state.shutdownOnce.Do(func() { close(state.shutdown) })
	// Abort unblocks the event owner if the consumer is stalled.
	if state.stream != nil {
		go state.stream.Abort()
	}
	if err := state.process.Terminate(ctx); err != nil && !errors.Is(err, os.ErrProcessDone) {
		_ = state.process.CloseInput()
		return err
	}
	return state.process.CloseInput()
}

func (a *Adapter) ReadHistory(_ context.Context, id string) ([]adapter.NativeEvent, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return append([]adapter.NativeEvent(nil), state.history...), nil
}

func (a *Adapter) RuntimeIdentity(_ context.Context, id string) (adapter.RuntimeIdentity, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return adapter.RuntimeIdentity{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.runtimeIdentity, nil
}

func (a *Adapter) RespondPermission(ctx context.Context, id string, decision adapter.PermissionDecision) error {
	state, err := a.requireSession(id)
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return a.respondServerRequest(ctx, state, decision.RequestID, map[string]any{"decision": "decline"})
	}
	return a.respondServerRequest(ctx, state, decision.RequestID, map[string]any{"decision": "accept"})
}

func (a *Adapter) GetDiff(_ context.Context, id string) ([]string, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return append([]string(nil), state.diff...), nil
}

func (a *Adapter) GetUsage(ctx context.Context, id string) (adapter.Usage, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return adapter.Usage{}, err
	}
	select {
	case <-state.resultReady:
	case <-ctx.Done():
		return adapter.Usage{}, ctx.Err()
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.usage, nil
}

func (a *Adapter) NormalizeEvent(native adapter.NativeEvent) (event.Input, error) {
	var payload any
	if len(native.Payload) > 0 {
		if err := json.Unmarshal(native.Payload, &payload); err != nil {
			return event.Input{}, err
		}
	}
	severity := "info"
	if native.Kind == event.TurnFailed || native.Kind == "protocol.error" {
		severity = "error"
	}
	return event.Input{Timestamp: native.Timestamp, Source: string(adapter.HarnessCodex), Type: native.Kind, Severity: severity, Payload: payload}, nil
}

func (a *Adapter) CollectFinalResult(ctx context.Context, id string) (report.Envelope, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return report.Envelope{}, err
	}
	select {
	case <-state.resultReady:
	case <-ctx.Done():
		return report.Envelope{}, ctx.Err()
	}
	state.mu.Lock()
	raw := append([]byte(nil), state.final...)
	reason := state.resultError
	if len(raw) == 0 && state.output.Len() > 0 {
		raw = []byte(state.output.String())
	}
	state.mu.Unlock()
	if reason != "" {
		return report.Envelope{}, fmt.Errorf("Codex turn failed: %s", reason)
	}
	return protocol.ParseEnvelope(raw)
}

func (a *Adapter) SessionConfigFact(req adapter.StartRequest) adapter.SessionConfigFact {
	safe := strings.EqualFold(req.Options["safe_mode"], "true")
	mode := req.Options["permission_mode"]
	// Codex uses protocol-native server requests for approvals; no Claude hooks.
	native := !safe && !isBypassPermissionMode(mode)
	return adapter.SessionConfigFact{
		PermissionMode:         mode,
		SafeMode:               safe,
		NativePermissionEvents: native,
		SteerVerified:          true,
		NextTurnDelivery:       false,
	}
}

// codexApprovalPolicy maps Broker permission configuration onto a Codex
// AskForApproval policy. Bypass / safe mode keep "never"; interactive modes
// use "on-request" so permission events reach the Broker.
func codexApprovalPolicy(req adapter.StartRequest) string {
	if strings.EqualFold(req.Options["safe_mode"], "true") {
		return "never"
	}
	if isBypassPermissionMode(req.Options["permission_mode"]) {
		return "never"
	}
	mode := strings.TrimSpace(strings.ToLower(req.Options["permission_mode"]))
	switch mode {
	case "untrusted":
		return "untrusted"
	default:
		// default, acceptEdits, plan, empty, or unknown interactive modes
		return "on-request"
	}
}

func isBypassPermissionMode(mode string) bool {
	switch strings.TrimSpace(strings.ToLower(mode)) {
	case "bypasspermissions", "never", "none":
		return true
	default:
		return false
	}
}

func (a *Adapter) request(ctx context.Context, state *sessionState, method string, params any) (json.RawMessage, error) {
	state.mu.Lock()
	state.nextID++
	id := state.nextID
	response := make(chan rpcMessage, 1)
	state.pending[id] = response
	state.mu.Unlock()
	if err := state.process.WriteJSON(map[string]any{"id": id, "method": method, "params": params}); err != nil {
		a.removePending(state, id)
		return nil, err
	}
	select {
	case message := <-response:
		if len(message.Error) > 0 && string(message.Error) != "null" {
			return nil, fmt.Errorf("%s: %s", method, string(message.Error))
		}
		a.captureRuntimeIdentity(state, message.Result)
		return message.Result, nil
	case <-ctx.Done():
		a.removePending(state, id)
		return nil, ctx.Err()
	}
}

func (a *Adapter) respondServerRequest(ctx context.Context, state *sessionState, requestID string, result any) error {
	if strings.TrimSpace(requestID) == "" {
		return fmt.Errorf("permission request id is required")
	}
	var raw json.RawMessage
	if err := json.Unmarshal([]byte(requestID), &raw); err != nil {
		raw = json.RawMessage(strconv.Quote(requestID))
	}
	return state.process.WriteJSON(map[string]any{"id": json.RawMessage(raw), "result": result})
}

func (a *Adapter) readProcess(state *sessionState) {
	defer state.shutdownOnce.Do(func() { close(state.shutdown) })
	// Fail-safe EOF unblock: if no terminal protocol event arrived, commit an
	// explicit failure once. Never double-commit after a protocol terminal.
	defer a.ensureResultReadyOnEOF(state)
	// Single producer (this reader): graceful close after protocol EOF.
	defer func() {
		if state.stream != nil {
			state.stream.CloseGracefully()
		}
	}()
	for line := range state.process.Lines() {
		a.handleLine(state, line)
	}
}

func (a *Adapter) handleLine(state *sessionState, line []byte) {
	var message rpcMessage
	if err := json.Unmarshal(line, &message); err != nil {
		native := adapter.NativeEvent{Kind: "protocol.error", Timestamp: time.Now().UTC(), Payload: json.RawMessage(fmt.Sprintf(`{"raw":%q,"error":%q}`, strings.TrimSpace(string(line)), err.Error()))}
		a.recordEvent(state, native)
		return
	}
	if len(message.ID) > 0 && message.Method == "" {
		var id int64
		if json.Unmarshal(message.ID, &id) == nil {
			state.mu.Lock()
			waiter := state.pending[id]
			delete(state.pending, id)
			state.mu.Unlock()
			if waiter != nil {
				waiter <- message
			}
		}
		return
	}
	if len(message.ID) > 0 && message.Method != "" {
		requestID := strings.TrimSpace(string(message.ID))
		state.mu.Lock()
		state.serverRequest[requestID] = struct{}{}
		state.mu.Unlock()
		native := adapter.NativeEvent{Kind: event.PermissionRequested, Timestamp: time.Now().UTC(), Payload: append([]byte(nil), line...)}
		a.recordEvent(state, native)
		return
	}
	if message.Method == "" {
		return
	}
	note := a.notificationEvent(state, message.Method, message.Params)
	if note.Terminal {
		a.recordTerminalEvent(state, note.Event)
		return
	}
	a.recordEvent(state, note.Event)
}

func (a *Adapter) notificationEvent(state *sessionState, method string, params json.RawMessage) codexNotification {
	now := time.Now().UTC()
	a.captureRuntimeIdentity(state, params)
	switch method {
	case "thread/started", "thread/resumed":
		return codexNotification{Event: adapter.NativeEvent{Kind: event.SessionStarted, Timestamp: now, Payload: params}}
	case "turn/started":
		var value struct {
			Turn struct {
				ID string `json:"id"`
			} `json:"turn"`
			TurnID string `json:"turnId"`
		}
		_ = json.Unmarshal(params, &value)
		state.mu.Lock()
		if value.Turn.ID != "" {
			state.turnID = value.Turn.ID
		} else if value.TurnID != "" {
			state.turnID = value.TurnID
		}
		state.mu.Unlock()
		return codexNotification{Event: adapter.NativeEvent{Kind: event.TurnStarted, Timestamp: now, Payload: params}}
	case "item/started":
		var value struct {
			Item struct {
				ID    string `json:"id"`
				Type  string `json:"type"`
				Phase string `json:"phase"`
			} `json:"item"`
		}
		_ = json.Unmarshal(params, &value)
		if value.Item.Type == "agentMessage" && value.Item.ID != "" {
			state.mu.Lock()
			state.agentPhases[value.Item.ID] = value.Item.Phase
			state.mu.Unlock()
		}
		return codexNotification{Event: adapter.NativeEvent{Kind: "codex.item/started", Timestamp: now, Payload: params}}
	case "item/agentMessage/delta":
		var value struct {
			Delta  string `json:"delta"`
			ItemID string `json:"itemId"`
		}
		_ = json.Unmarshal(params, &value)
		state.mu.Lock()
		phase := state.agentPhases[value.ItemID]
		if value.ItemID == "" || phase == "" || phase == "final_answer" {
			state.output.WriteString(value.Delta)
		}
		state.mu.Unlock()
		return codexNotification{Event: adapter.NativeEvent{Kind: event.ModelOutputDelta, Timestamp: now, Payload: params}}
	case "item/agentMessage/completed", "item/completed":
		var value struct {
			Item struct {
				Type    string `json:"type"`
				ID      string `json:"id"`
				Phase   string `json:"phase"`
				Text    string `json:"text"`
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
				Path    string `json:"path"`
				Changes []struct {
					Path string `json:"path"`
				} `json:"changes"`
			} `json:"item"`
		}
		_ = json.Unmarshal(params, &value)
		state.mu.Lock()
		phase := value.Item.Phase
		if phase == "" && value.Item.ID != "" {
			phase = state.agentPhases[value.Item.ID]
		}
		if value.Item.Type == "agentMessage" && (phase == "" || phase == "final_answer") {
			if value.Item.Text != "" && state.output.Len() == 0 {
				state.output.WriteString(value.Item.Text)
			}
			for _, part := range value.Item.Content {
				if part.Text != "" && state.output.Len() == 0 {
					state.output.WriteString(part.Text)
				}
			}
		}
		if value.Item.Path != "" {
			state.diff = appendUnique(state.diff, value.Item.Path)
		}
		for _, change := range value.Item.Changes {
			if change.Path != "" {
				state.diff = appendUnique(state.diff, change.Path)
			}
		}
		state.mu.Unlock()
		return codexNotification{Event: adapter.NativeEvent{Kind: event.ModelMessageCompleted, Timestamp: now, Payload: params}}
	case "turn/completed":
		// Terminal freeze happens in recordTerminalEvent so history precedes readiness.
		var value struct {
			Usage struct {
				InputTokens  int64   `json:"inputTokens"`
				OutputTokens int64   `json:"outputTokens"`
				Cost         float64 `json:"cost"`
			} `json:"usage"`
		}
		_ = json.Unmarshal(params, &value)
		// Stash usage on state under lock; recordTerminalEvent freezes final+history.
		state.mu.Lock()
		state.usage = adapter.Usage{InputTokens: value.Usage.InputTokens, OutputTokens: value.Usage.OutputTokens, Cost: value.Usage.Cost, Currency: "USD"}
		state.final = []byte(state.output.String())
		state.mu.Unlock()
		return codexNotification{
			Event:    adapter.NativeEvent{Kind: event.ResultSubmitted, Timestamp: now, Payload: params},
			Terminal: true,
		}
	case "turn/failed", "turn/cancelled":
		state.mu.Lock()
		state.resultError = string(params)
		state.mu.Unlock()
		return codexNotification{
			Event:    adapter.NativeEvent{Kind: event.TurnFailed, Timestamp: now, Payload: params},
			Terminal: true,
		}
	default:
		return codexNotification{Event: adapter.NativeEvent{Kind: "codex." + method, Timestamp: now, Payload: params}}
	}
}

// captureRuntimeIdentity accepts only explicit provider/model fields emitted by
// Codex responses or notifications. It never falls back to the requested model.
func (a *Adapter) captureRuntimeIdentity(state *sessionState, payload []byte) {
	var value struct {
		Provider    string `json:"provider"`
		ProviderID  string `json:"providerId"`
		ProviderID2 string `json:"provider_id"`
		Model       string `json:"model"`
		ModelID     string `json:"modelId"`
		ModelID2    string `json:"model_id"`
		ModelID3    string `json:"modelID"`
	}
	if len(payload) == 0 || json.Unmarshal(payload, &value) != nil {
		return
	}
	provider := strings.TrimSpace(value.Provider)
	if provider == "" {
		provider = strings.TrimSpace(value.ProviderID)
	}
	if provider == "" {
		provider = strings.TrimSpace(value.ProviderID2)
	}
	model := strings.TrimSpace(value.Model)
	if model == "" {
		model = strings.TrimSpace(value.ModelID)
	}
	if model == "" {
		model = strings.TrimSpace(value.ModelID2)
	}
	if model == "" {
		model = strings.TrimSpace(value.ModelID3)
	}
	nativeProvider, nativeModel := adapter.RuntimeIdentityFields(payload)
	if provider == "" {
		provider = nativeProvider
	}
	if model == "" {
		model = nativeModel
	}
	if provider == "" && model == "" {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if provider != "" {
		state.runtimeIdentity.ObservedProvider = provider
		state.runtimeIdentity.ProviderSource = adapter.EvidenceNativeProtocol
	}
	if model != "" {
		state.runtimeIdentity.ObservedModel = model
		state.runtimeIdentity.ModelSource = adapter.EvidenceNativeProtocol
	}
}

// recordTerminalEvent commits terminal state and history before unblocking
// resultReady. Order:
//  1. Freeze final / usage / resultError (already applied by caller where needed)
//  2. Append terminal event to history
//  3. Signal resultReady exactly once
//  4. Publish to EventStream (without holding state.mu)
//  5. Close process input asynchronously
func (a *Adapter) recordTerminalEvent(state *sessionState, native adapter.NativeEvent) {
	state.mu.Lock()
	if state.terminalCommitted {
		state.mu.Unlock()
		return
	}
	// Ensure success path has frozen final from output if still empty.
	if native.Kind == event.ResultSubmitted && len(state.final) == 0 && state.output.Len() > 0 {
		state.final = []byte(state.output.String())
	}
	state.history = append(state.history, native)
	state.terminalCommitted = true
	state.mu.Unlock()

	state.resultOnce.Do(func() { close(state.resultReady) })

	if state.stream != nil {
		_ = state.stream.Publish(native)
	}
	if state.process != nil {
		go func() { _ = state.process.CloseInput() }()
	}
}

// ensureResultReadyOnEOF is the process-EOF fail-safe. If a protocol terminal
// already committed, only ensures readiness is signaled (no second history event).
// Otherwise commits an explicit unexpected-EOF failure — never false success.
func (a *Adapter) ensureResultReadyOnEOF(state *sessionState) {
	state.mu.Lock()
	if state.terminalCommitted {
		state.mu.Unlock()
		state.resultOnce.Do(func() { close(state.resultReady) })
		return
	}
	if state.resultError == "" {
		state.resultError = "unexpected protocol EOF"
	}
	// Do not claim ResultSubmitted on EOF without a terminal notification.
	native := adapter.NativeEvent{
		Kind:      event.TurnFailed,
		Timestamp: time.Now().UTC(),
		Payload:   json.RawMessage(`{"error":"unexpected protocol EOF"}`),
	}
	state.history = append(state.history, native)
	state.terminalCommitted = true
	state.mu.Unlock()

	state.resultOnce.Do(func() { close(state.resultReady) })
	if state.stream != nil {
		_ = state.stream.Publish(native)
	}
}

func (a *Adapter) recordEvent(state *sessionState, native adapter.NativeEvent) {
	// History under session mutex; publication never holds the mutex while
	// waiting on the public consumer (EventStream owns that).
	state.mu.Lock()
	state.history = append(state.history, native)
	state.mu.Unlock()
	if state.stream != nil {
		_ = state.stream.Publish(native)
	}
}

func (a *Adapter) removePending(state *sessionState, id int64) {
	state.mu.Lock()
	delete(state.pending, id)
	state.mu.Unlock()
}

func (a *Adapter) requireSession(id string) (*sessionState, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state, ok := a.sessions[id]
	if !ok {
		return nil, fmt.Errorf("unknown Codex thread %q", id)
	}
	return state, nil
}

func sessionFromState(state *sessionState) adapter.Session {
	identity := state.process.Identity()
	var events <-chan adapter.NativeEvent
	if state.stream != nil {
		events = state.stream.Events()
	}
	return adapter.Session{NativeSessionID: state.threadID, NativeTurnID: state.turnID, PID: identity.PID, ProcessStartToken: identity.StartToken, ProcessGroupToken: identity.ProcessGroupToken, Events: events, Exited: state.process.Exited(), Stderr: state.process.Stderr()}
}

func responseID(raw json.RawMessage) string {
	var value struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
		ThreadID string `json:"threadId"`
		ID       string `json:"id"`
	}
	_ = json.Unmarshal(raw, &value)
	if value.Thread.ID != "" {
		return value.Thread.ID
	}
	if value.ThreadID != "" {
		return value.ThreadID
	}
	return value.ID
}

func responseTurnID(raw json.RawMessage) string {
	var value struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
		TurnID string `json:"turnId"`
		ID     string `json:"id"`
	}
	_ = json.Unmarshal(raw, &value)
	if value.Turn.ID != "" {
		return value.Turn.ID
	}
	if value.TurnID != "" {
		return value.TurnID
	}
	return value.ID
}

func nullable(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func appendUnique(values []string, value string) []string {
	for _, item := range values {
		if item == value {
			return values
		}
	}
	return append(values, value)
}
