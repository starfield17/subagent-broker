package grok

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
	acpSessionID    string
	workerID        string
	events          chan adapter.NativeEvent
	resultReady     chan struct{}
	resultSignaled  bool
	closeEvents     sync.Once
	shutdown        chan struct{}
	shutdownOnce    sync.Once
	history         []adapter.NativeEvent
	output          strings.Builder
	final           []byte
	resultError     string
	usage           adapter.Usage
	droppedProgress uint64
	closed          bool
	nextID          int64
	pending         map[int64]chan rpcMessage
	serverRequest   map[string]struct{}
	// Multi-turn generation: only the latest generation may finalize results.
	generation     uint64
	promptInFlight bool
	promptGen      uint64
	promptRPCID    int64
}

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

func New(executable string) *Adapter {
	if strings.TrimSpace(executable) == "" {
		executable = "grok"
	}
	return &Adapter{executable: executable, sessions: map[string]*sessionState{}}
}

func (a *Adapter) Descriptor() adapter.Descriptor { return Descriptor() }

func (a *Adapter) Probe(ctx context.Context, req adapter.ProbeRequest) (adapter.ProbeResult, error) {
	executable := a.executable
	if strings.TrimSpace(req.Executable) != "" {
		executable = req.Executable
	}
	output, err := protocol.CommandOutput(ctx, executable, "--no-auto-update", "--version")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || strings.Contains(strings.ToLower(err.Error()), "executable file not found") {
			return adapter.ProbeResult{Installed: false, Compatibility: "unavailable", Capabilities: Descriptor().Capabilities}, nil
		}
		return adapter.ProbeResult{Installed: true, Compatibility: "probe_failed", Warnings: []string{err.Error()}, Capabilities: Descriptor().Capabilities}, nil
	}
	version := protocol.ParseVersion(output)
	authenticated, authErr := a.probeAuth(ctx, executable)
	compatibility := "verified"
	if version != "0.2.99" {
		compatibility = "compatibility_unverified"
	}
	result := adapter.ProbeResult{Installed: true, Version: version, Authenticated: &authenticated, Capabilities: Descriptor().Capabilities, Compatibility: compatibility}
	if authErr != nil {
		result.Warnings = []string{"ACP authentication probe: " + authErr.Error()}
	}
	return result, nil
}

func (a *Adapter) StartSession(ctx context.Context, req adapter.StartRequest) (adapter.Session, error) {
	return a.start(ctx, req, "")
}

func (a *Adapter) ResumeSession(ctx context.Context, req adapter.ResumeRequest) (adapter.Session, error) {
	if strings.TrimSpace(req.NativeSessionID) == "" {
		return adapter.Session{}, fmt.Errorf("native Grok session id is required")
	}
	return a.start(ctx, adapter.StartRequest{
		RunID: req.RunID, TaskID: req.TaskID, WorkerID: req.WorkerID,
		ProjectRoot: req.ProjectRoot, Contract: req.Contract, Model: req.Model,
		Options: req.Options, Interaction: req.Interaction,
	}, req.NativeSessionID)
}

func (a *Adapter) start(ctx context.Context, req adapter.StartRequest, resumeID string) (adapter.Session, error) {
	p, err := transport.Start(ctx, a.executable, []string{"--no-auto-update", "agent", "stdio"}, req.ProjectRoot)
	if err != nil {
		return adapter.Session{}, fmt.Errorf("start Grok ACP: %w", err)
	}
	state := &sessionState{
		process: p, workerID: req.WorkerID, events: make(chan adapter.NativeEvent, 256),
		resultReady: make(chan struct{}), shutdown: make(chan struct{}),
		pending: map[int64]chan rpcMessage{}, serverRequest: map[string]struct{}{},
	}
	go a.readProcess(state)
	if _, err := a.request(ctx, state, "initialize", map[string]any{
		"protocolVersion":    1,
		"clientInfo":         map[string]string{"name": "subagent-broker", "version": "phase4"},
		"clientCapabilities": map[string]any{},
	}); err != nil {
		_ = p.CloseInput()
		return adapter.Session{}, fmt.Errorf("Grok ACP initialize: %w", err)
	}
	methodID := req.Options["auth_method"]
	if methodID == "" {
		methodID = "xai.api_key"
	}
	if _, err := a.request(ctx, state, "authenticate", map[string]any{"methodId": methodID}); err != nil {
		_ = p.CloseInput()
		return adapter.Session{}, fmt.Errorf("Grok ACP authenticate: %w", err)
	}
	sessionID := resumeID
	if sessionID == "" {
		result, err := a.request(ctx, state, "session/new", map[string]any{"cwd": req.ProjectRoot, "mcpServers": []any{}})
		if err != nil {
			_ = p.CloseInput()
			return adapter.Session{}, fmt.Errorf("Grok ACP session/new: %w", err)
		}
		sessionID = responseSessionID(result)
		if sessionID == "" {
			return adapter.Session{}, fmt.Errorf("Grok ACP session/new returned no session id")
		}
	} else if _, err := a.request(ctx, state, "session/load", map[string]any{"sessionId": sessionID, "cwd": req.ProjectRoot, "mcpServers": []any{}}); err != nil {
		_ = p.CloseInput()
		return adapter.Session{}, fmt.Errorf("Grok ACP session/load: %w", err)
	}
	state.mu.Lock()
	state.acpSessionID = sessionID
	state.mu.Unlock()
	a.mu.Lock()
	a.sessions[sessionID] = state
	a.mu.Unlock()
	// Start the initial prompt without blocking on completion so Supervisor can
	// consume events/permissions while the turn runs.
	if err := a.startPrompt(ctx, state, req.Contract); err != nil {
		_ = p.CloseInput()
		return adapter.Session{}, err
	}
	return sessionFromState(state), nil
}

func (a *Adapter) SendMessage(ctx context.Context, id, message string) (adapter.DeliveryResult, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return adapter.DeliveryResult{}, err
	}
	// Non-blocking: returns after session/prompt is written, not after turn completion.
	if err := a.startPrompt(ctx, state, message); err != nil {
		return adapter.DeliveryResult{}, err
	}
	return adapter.DeliveryResult{Mode: adapter.DeliveryNextTurn, MessageID: message}, nil
}

func (a *Adapter) SteerActiveTurn(context.Context, string, string) (adapter.DeliveryResult, error) {
	return adapter.DeliveryResult{Mode: adapter.DeliveryUnsupported}, adapter.ErrUnsupported
}

func (a *Adapter) InterruptTurn(ctx context.Context, id string) error {
	state, err := a.requireSession(id)
	if err != nil {
		return err
	}
	state.mu.Lock()
	sessionID := state.acpSessionID
	state.mu.Unlock()
	// ACP session/cancel is a notification: no id, no response waiter.
	// Completion is observed through the original prompt request and events.
	return state.process.WriteJSON(map[string]any{
		"jsonrpc": "2.0",
		"method":  "session/cancel",
		"params":  map[string]any{"sessionId": sessionID},
	})
}

func (a *Adapter) TerminateSession(ctx context.Context, id string) error {
	state, err := a.requireSession(id)
	if err != nil {
		return err
	}
	state.shutdownOnce.Do(func() { close(state.shutdown) })
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

func (a *Adapter) RespondPermission(ctx context.Context, id string, decision adapter.PermissionDecision) error {
	state, err := a.requireSession(id)
	if err != nil {
		return err
	}
	// ACP v1: client must select one of the Agent-supplied options by opaque optionId.
	optionID := strings.TrimSpace(decision.OptionID)
	if optionID == "" {
		return fmt.Errorf("Grok ACP permission response requires a native optionId")
	}
	result := map[string]any{
		"outcome": map[string]any{
			"outcome":  "selected",
			"optionId": optionID,
		},
	}
	return a.respondServerRequest(ctx, state, decision.RequestID, result)
}

func (a *Adapter) GetDiff(context.Context, string) ([]string, error) {
	return nil, adapter.ErrUnsupported
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
	return event.Input{Timestamp: native.Timestamp, Source: string(adapter.HarnessGrokBuild), Type: native.Kind, Severity: severity, Payload: payload}, nil
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
	if len(raw) == 0 {
		raw = []byte(state.output.String())
	}
	state.mu.Unlock()
	if reason != "" {
		return report.Envelope{}, fmt.Errorf("Grok session failed: %s", reason)
	}
	return protocol.ParseEnvelope(raw)
}

func (a *Adapter) SessionConfigFact(req adapter.StartRequest) adapter.SessionConfigFact {
	safe := strings.EqualFold(req.Options["safe_mode"], "true")
	mode := req.Options["permission_mode"]
	native := !safe && !strings.EqualFold(strings.TrimSpace(mode), "bypassPermissions")
	return adapter.SessionConfigFact{
		PermissionMode:         mode,
		SafeMode:               safe,
		NativePermissionEvents: native,
		SteerVerified:          false,
		NextTurnDelivery:       true,
	}
}

// startPrompt writes session/prompt and returns after the request is accepted.
// Turn completion is finalized asynchronously by waitPromptCompletion.
func (a *Adapter) startPrompt(ctx context.Context, state *sessionState, text string) error {
	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		return fmt.Errorf("Grok session is closed")
	}
	if state.promptInFlight {
		state.mu.Unlock()
		return fmt.Errorf("Grok prompt already in flight; concurrent SendMessage rejected")
	}
	// Re-arm result channel for a new generation after a prior finalization.
	if state.resultSignaled {
		state.resultReady = make(chan struct{})
		state.resultSignaled = false
	}
	state.generation++
	gen := state.generation
	state.promptInFlight = true
	state.promptGen = gen
	state.output.Reset()
	// Do not clear final yet — only matching gen may overwrite on completion.
	state.resultError = ""
	sessionID := state.acpSessionID
	state.nextID++
	rpcID := state.nextID
	state.promptRPCID = rpcID
	response := make(chan rpcMessage, 1)
	state.pending[rpcID] = response
	state.mu.Unlock()

	params := map[string]any{
		"sessionId": sessionID,
		"prompt":    []map[string]string{{"type": "text", "text": text}},
	}
	if err := state.process.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": rpcID, "method": "session/prompt", "params": params}); err != nil {
		a.clearPromptInFlight(state, gen, rpcID)
		return fmt.Errorf("Grok ACP session/prompt write: %w", err)
	}
	// Completion phase runs async; SendMessage returns before turn completion.
	go a.waitPromptCompletion(ctx, state, gen, rpcID, response)
	return nil
}

func (a *Adapter) clearPromptInFlight(state *sessionState, gen uint64, rpcID int64) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.promptGen == gen {
		state.promptInFlight = false
	}
	if waiter := state.pending[rpcID]; waiter != nil {
		delete(state.pending, rpcID)
	}
}

func (a *Adapter) waitPromptCompletion(ctx context.Context, state *sessionState, gen uint64, rpcID int64, response <-chan rpcMessage) {
	select {
	case message := <-response:
		// Clear in-flight before emitting ResultSubmitted so a Supervisor-driven
		// next SendMessage is not rejected as concurrent.
		a.clearPromptInFlight(state, gen, rpcID)
		// Only the matching generation may finalize authoritative results.
		state.mu.Lock()
		if gen != state.generation {
			state.mu.Unlock()
			return
		}
		if len(message.Error) > 0 && string(message.Error) != "null" {
			state.resultError = string(message.Error)
			state.mu.Unlock()
			a.finalizeGeneration(state, gen, false)
			return
		}
		state.final = []byte(state.output.String())
		var completion struct {
			Usage struct {
				InputTokens  int64   `json:"inputTokens"`
				OutputTokens int64   `json:"outputTokens"`
				Cost         float64 `json:"cost"`
			} `json:"usage"`
		}
		_ = json.Unmarshal(message.Result, &completion)
		state.usage = adapter.Usage{
			InputTokens: completion.Usage.InputTokens, OutputTokens: completion.Usage.OutputTokens,
			Cost: completion.Usage.Cost, Currency: "USD",
		}
		state.mu.Unlock()
		a.finalizeGeneration(state, gen, true)
	case <-ctx.Done():
		a.clearPromptInFlight(state, gen, rpcID)
		state.mu.Lock()
		if gen == state.generation && state.resultError == "" {
			state.resultError = ctx.Err().Error()
		}
		state.mu.Unlock()
		a.finalizeGeneration(state, gen, false)
	case <-state.shutdown:
		a.clearPromptInFlight(state, gen, rpcID)
		state.mu.Lock()
		if gen == state.generation && state.resultError == "" {
			state.resultError = "session shutdown"
		}
		state.mu.Unlock()
		a.finalizeGeneration(state, gen, false)
	}
}

// finalizeGeneration emits exactly one authoritative ResultSubmitted or TurnFailed
// for gen and signals CollectFinalResult. Stale generations are ignored.
func (a *Adapter) finalizeGeneration(state *sessionState, gen uint64, success bool) {
	state.mu.Lock()
	if gen != state.generation {
		state.mu.Unlock()
		return
	}
	// Snapshot under lock; emit after unlock.
	payload := json.RawMessage(`{}`)
	if !success && state.resultError != "" {
		payload, _ = json.Marshal(map[string]string{"error": state.resultError})
	}
	state.mu.Unlock()

	kind := event.ResultSubmitted
	if !success {
		kind = event.TurnFailed
	}
	a.recordEvent(state, adapter.NativeEvent{Kind: kind, Timestamp: time.Now().UTC(), Payload: payload})
	a.signalResult(state)
}

func (a *Adapter) signalResult(state *sessionState) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.resultSignaled {
		return
	}
	state.resultSignaled = true
	close(state.resultReady)
}

func (a *Adapter) request(ctx context.Context, state *sessionState, method string, params any) (json.RawMessage, error) {
	state.mu.Lock()
	state.nextID++
	id := state.nextID
	response := make(chan rpcMessage, 1)
	state.pending[id] = response
	state.mu.Unlock()
	if err := state.process.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		a.removePending(state, id)
		return nil, err
	}
	select {
	case message := <-response:
		if len(message.Error) > 0 && string(message.Error) != "null" {
			return nil, fmt.Errorf("%s: %s", method, string(message.Error))
		}
		return message.Result, nil
	case <-ctx.Done():
		a.removePending(state, id)
		return nil, ctx.Err()
	case <-state.shutdown:
		a.removePending(state, id)
		return nil, fmt.Errorf("session shutdown")
	}
}

func (a *Adapter) respondServerRequest(_ context.Context, state *sessionState, requestID string, result any) error {
	if strings.TrimSpace(requestID) == "" {
		return fmt.Errorf("permission request id is required")
	}
	// Preserve numeric vs string JSON-RPC ids (same encoding rules as Codex).
	var rawID json.RawMessage
	if err := json.Unmarshal([]byte(requestID), &rawID); err != nil {
		rawID = json.RawMessage(strconv.Quote(requestID))
	}
	return state.process.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": rawID, "result": result})
}

func (a *Adapter) readProcess(state *sessionState) {
	defer state.shutdownOnce.Do(func() { close(state.shutdown) })
	defer state.closeEvents.Do(func() {
		state.mu.Lock()
		state.closed = true
		state.mu.Unlock()
		close(state.events)
	})
	defer a.signalResult(state)
	for line := range state.process.Lines() {
		a.handleLine(state, line)
	}
}

func (a *Adapter) handleLine(state *sessionState, line []byte) {
	var message rpcMessage
	if err := json.Unmarshal(line, &message); err != nil {
		a.recordEvent(state, adapter.NativeEvent{Kind: "protocol.error", Timestamp: time.Now().UTC(), Payload: json.RawMessage(fmt.Sprintf(`{"raw":%q,"error":%q}`, strings.TrimSpace(string(line)), err.Error()))})
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
		state.mu.Lock()
		state.serverRequest[strings.TrimSpace(string(message.ID))] = struct{}{}
		state.mu.Unlock()
		a.recordEvent(state, adapter.NativeEvent{Kind: event.PermissionRequested, Timestamp: time.Now().UTC(), Payload: append([]byte(nil), line...)})
		return
	}
	if message.Method == "" {
		return
	}
	now := time.Now().UTC()
	if message.Method == "session/update" {
		var value struct {
			Update struct {
				SessionUpdate string `json:"sessionUpdate"`
				Content       struct {
					Text string `json:"text"`
				} `json:"content"`
				Delta string `json:"delta"`
				Text  string `json:"text"`
			} `json:"update"`
		}
		_ = json.Unmarshal(message.Params, &value)
		text := value.Update.Content.Text
		if text == "" {
			text = value.Update.Delta
		}
		if text == "" {
			text = value.Update.Text
		}
		if value.Update.SessionUpdate == "agent_message_chunk" && text != "" {
			state.mu.Lock()
			state.output.WriteString(text)
			state.mu.Unlock()
		}
		kind := "grok.session_update"
		if value.Update.SessionUpdate == "agent_message_chunk" {
			kind = event.ModelOutputDelta
		}
		if value.Update.SessionUpdate == "tool_call" {
			kind = event.ToolStarted
		}
		a.recordEvent(state, adapter.NativeEvent{Kind: kind, Timestamp: now, Payload: append([]byte(nil), line...)})
		return
	}
	kind := "grok." + message.Method
	switch message.Method {
	case "session/started":
		kind = event.SessionStarted
	case "session/prompt_started":
		kind = event.TurnStarted
	case "session/prompt_completed":
		// Notification is informational only. Authoritative ResultSubmitted is
		// emitted when the matching session/prompt JSON-RPC response arrives.
		kind = event.TurnCompleted
	case "session/cancelled":
		kind = event.TurnFailed
	}
	a.recordEvent(state, adapter.NativeEvent{Kind: kind, Timestamp: now, Payload: append([]byte(nil), line...)})
}

func (a *Adapter) recordEvent(state *sessionState, native adapter.NativeEvent) {
	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		return
	}
	state.history = append(state.history, native)
	state.mu.Unlock()
	protocol.EmitNativeEvent(protocol.EmitOptions{
		Events:          state.events,
		Shutdown:        state.shutdown,
		Mu:              &state.mu,
		DroppedProgress: &state.droppedProgress,
	}, native)
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
		return nil, fmt.Errorf("unknown Grok session %q", id)
	}
	return state, nil
}

func sessionFromState(state *sessionState) adapter.Session {
	i := state.process.Identity()
	return adapter.Session{NativeSessionID: state.acpSessionID, PID: i.PID, ProcessStartToken: i.StartToken, Events: state.events, Exited: state.process.Exited(), Stderr: state.process.Stderr()}
}

func responseSessionID(raw json.RawMessage) string {
	var value struct {
		SessionID string `json:"sessionId"`
	}
	_ = json.Unmarshal(raw, &value)
	return value.SessionID
}

func (a *Adapter) probeAuth(ctx context.Context, executable string) (bool, error) {
	p, err := transport.Start(ctx, executable, []string{"--no-auto-update", "agent", "stdio"}, ".")
	if err != nil {
		return false, err
	}
	defer func() { _ = p.CloseInput() }()
	if err := p.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{"protocolVersion": 1, "clientInfo": map[string]string{"name": "subagent-broker", "version": "phase4"}, "clientCapabilities": map[string]any{}}}); err != nil {
		return false, err
	}
	if err := p.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 2, "method": "authenticate", "params": map[string]any{"methodId": "xai.api_key"}}); err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		select {
		case line, ok := <-p.Lines():
			if !ok {
				return false, fmt.Errorf("ACP exited before authentication response")
			}
			var message rpcMessage
			if json.Unmarshal(line, &message) == nil && string(message.ID) == "2" {
				return len(message.Error) == 0 || string(message.Error) == "null", nil
			}
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}
