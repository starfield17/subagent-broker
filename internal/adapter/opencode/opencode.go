package opencode

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
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
	mu             sync.Mutex
	process        *transport.Process
	baseURL        string
	directory      string
	sessionID      string
	workerID       string
	stream         *protocol.EventStream
	resultReady    chan struct{}
	resultOnce     sync.Once
	finishOnce     sync.Once
	shutdown       chan struct{}
	shutdownOnce   sync.Once
	history        []adapter.NativeEvent
	final          []byte
	resultError    string
	usage          adapter.Usage
	generation     uint64
	promptInFlight bool
	promptGen      uint64
	resultSignaled bool
	currentTurn    *ocTurnResult
	// SSE producer coordination with watchExit.
	sseDone chan struct{}
}

// ocTurnResult is generation-specific OpenCode completion state.
type ocTurnResult struct {
	generation         uint64
	ready              chan struct{}
	baselineCaptured   bool // true even when baselineMessageIDs is empty (first turn)
	baselineMessageIDs map[string]struct{}
	promptMessageID    string
	sawCurrentActivity bool
	completed          bool
	success            bool
	final              []byte
	resultErr          string
	usage              adapter.Usage
	once               sync.Once
}

// collectedTurnResult is a generation-specific collection outcome that does not
// mutate global session state during the HTTP request.
type collectedTurnResult struct {
	Final         []byte
	Usage         adapter.Usage
	SawActivity   bool
	HasEnvelope   bool
	InvalidOutput bool // assistant output exists but no valid Result Envelope
}

type sseEvent struct {
	Type       string          `json:"type"`
	Properties json.RawMessage `json:"properties"`
}

// openCodeMessage is the canonical OpenCode session message shape.
// Message identity and session ownership live under info, never top-level id.
type openCodeMessage struct {
	Info struct {
		ID        string `json:"id"`
		SessionID string `json:"sessionID"`
		Role      string `json:"role"`
		Tokens    struct {
			Input     int64 `json:"input"`
			Output    int64 `json:"output"`
			Reasoning int64 `json:"reasoning"`
			Cache     struct {
				Read  int64 `json:"read"`
				Write int64 `json:"write"`
			} `json:"cache"`
		} `json:"tokens"`
		Cost float64 `json:"cost"`
	} `json:"info"`
	Parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"parts"`
}

// Typed SSE property shapes for session-scoped OpenCode events.
type openCodeSessionEvent struct {
	SessionID string `json:"sessionID"`
}

type openCodeSessionStatusEvent struct {
	SessionID string `json:"sessionID"`
	Status    struct {
		Type string `json:"type"`
	} `json:"status"`
}

type openCodeMessageUpdatedEvent struct {
	SessionID string          `json:"sessionID"`
	Info      json.RawMessage `json:"info"`
}

type openCodePartUpdatedEvent struct {
	SessionID string          `json:"sessionID"`
	Part      json.RawMessage `json:"part"`
}

// errOpenCodeContract is a stable provenance/protocol contract failure.
type errOpenCodeContract struct {
	reason string
}

func (e *errOpenCodeContract) Error() string {
	return "OpenCode protocol contract: " + e.reason
}

func New(executable string) *Adapter {
	if strings.TrimSpace(executable) == "" {
		executable = "opencode"
	}
	return &Adapter{executable: executable, sessions: map[string]*sessionState{}}
}

func (a *Adapter) Descriptor() adapter.Descriptor { return Descriptor() }

func (a *Adapter) Probe(ctx context.Context, req adapter.ProbeRequest) (adapter.ProbeResult, error) {
	executable := a.executable
	if strings.TrimSpace(req.Executable) != "" {
		executable = req.Executable
	}
	output, err := protocol.CommandOutput(ctx, executable, "--version")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || strings.Contains(strings.ToLower(err.Error()), "command not found") || strings.Contains(strings.ToLower(err.Error()), "executable file not found") {
			return adapter.ProbeResult{Installed: false, Compatibility: "unavailable", Capabilities: Descriptor().Capabilities}, nil
		}
		return adapter.ProbeResult{Installed: true, Compatibility: "probe_failed", Warnings: []string{err.Error()}, Capabilities: Descriptor().Capabilities}, nil
	}
	version := protocol.ParseVersion(output)
	authOutput, authErr := protocol.CommandOutput(ctx, executable, "auth", "list")
	authenticated := authErr == nil && strings.Contains(strings.ToLower(string(authOutput)), "credential")
	compatibility := "verified"
	if version != "1.17.15" {
		compatibility = "compatibility_unverified"
	}
	result := adapter.ProbeResult{Installed: true, Version: version, Authenticated: &authenticated, Capabilities: Descriptor().Capabilities, Compatibility: compatibility}
	if authErr != nil {
		result.Warnings = append(result.Warnings, "OpenCode auth probe: "+authErr.Error())
	}
	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	p, baseURL, startErr := a.startServer(probeCtx, executable, ".")
	if startErr != nil {
		result.Compatibility = "probe_failed"
		result.Warnings = append(result.Warnings, "server probe: "+startErr.Error())
		return result, nil
	}
	if healthErr := a.waitHealth(probeCtx, baseURL); healthErr != nil {
		result.Compatibility = "probe_failed"
		result.Warnings = append(result.Warnings, "server health: "+healthErr.Error())
	}
	_ = p.Terminate(context.Background())
	_ = p.CloseInput()
	_, _ = p.Wait(context.Background())
	return result, nil
}

func (a *Adapter) StartSession(ctx context.Context, req adapter.StartRequest) (adapter.Session, error) {
	return a.start(ctx, req, "")
}

func (a *Adapter) ResumeSession(ctx context.Context, req adapter.ResumeRequest) (adapter.Session, error) {
	if strings.TrimSpace(req.NativeSessionID) == "" {
		return adapter.Session{}, fmt.Errorf("native OpenCode session id is required")
	}
	return a.start(ctx, adapter.StartRequest{
		RunID: req.RunID, TaskID: req.TaskID, WorkerID: req.WorkerID,
		ProjectRoot: req.ProjectRoot, Contract: req.Contract, Model: req.Model,
		Options: req.Options, Interaction: req.Interaction,
	}, req.NativeSessionID)
}

func (a *Adapter) start(ctx context.Context, req adapter.StartRequest, resumeID string) (adapter.Session, error) {
	p, baseURL, err := a.startServer(ctx, a.executable, req.ProjectRoot)
	if err != nil {
		return adapter.Session{}, err
	}
	state := &sessionState{
		process: p, baseURL: baseURL, directory: req.ProjectRoot, workerID: req.WorkerID,
		stream:      protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 256, ProgressQueueLimit: 256}),
		resultReady: make(chan struct{}), shutdown: make(chan struct{}),
		sseDone: make(chan struct{}),
	}
	go a.drainServerOutput(p)
	go a.watchExit(state)
	go a.subscribeEvents(state)
	sessionID := resumeID
	if sessionID == "" {
		var created struct {
			ID string `json:"id"`
		}
		if err := a.requestJSON(ctx, state, http.MethodPost, "/session", nil, map[string]any{}, &created); err != nil {
			_ = a.stop(state)
			return adapter.Session{}, fmt.Errorf("OpenCode create session: %w", err)
		}
		sessionID = created.ID
		if sessionID == "" {
			_ = a.stop(state)
			return adapter.Session{}, fmt.Errorf("OpenCode create session returned no id")
		}
	} else {
		var existing map[string]any
		if err := a.requestJSON(ctx, state, http.MethodGet, "/session/"+url.PathEscape(sessionID), nil, nil, &existing); err != nil {
			_ = a.stop(state)
			return adapter.Session{}, fmt.Errorf("OpenCode resume session: %w", err)
		}
	}
	state.mu.Lock()
	state.sessionID = sessionID
	state.mu.Unlock()
	a.mu.Lock()
	a.sessions[sessionID] = state
	a.mu.Unlock()
	if err := a.promptAsync(ctx, state, req); err != nil {
		_ = a.stop(state)
		return adapter.Session{}, err
	}
	return sessionFromState(state), nil
}

func (a *Adapter) SendMessage(ctx context.Context, id, message string) (adapter.DeliveryResult, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return adapter.DeliveryResult{}, err
	}
	err = a.promptAsync(ctx, state, adapter.StartRequest{ProjectRoot: state.directory, Contract: message})
	if err != nil {
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
	return a.requestJSON(ctx, state, http.MethodPost, "/session/"+url.PathEscape(id)+"/abort", nil, map[string]any{}, nil)
}

func (a *Adapter) TerminateSession(ctx context.Context, id string) error {
	state, err := a.requireSession(id)
	if err != nil {
		return err
	}
	state.shutdownOnce.Do(func() { close(state.shutdown) })
	if state.stream != nil {
		go state.stream.Abort()
	}
	_ = a.requestJSON(ctx, state, http.MethodPost, "/session/"+url.PathEscape(id)+"/abort", nil, map[string]any{}, nil)
	return a.stop(state)
}

func (a *Adapter) ReadHistory(ctx context.Context, id string) ([]adapter.NativeEvent, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return nil, err
	}
	var messages []json.RawMessage
	if err := a.requestJSON(ctx, state, http.MethodGet, "/session/"+url.PathEscape(id)+"/message", nil, nil, &messages); err != nil {
		return nil, err
	}
	result := make([]adapter.NativeEvent, 0, len(messages))
	for _, message := range messages {
		result = append(result, adapter.NativeEvent{Kind: event.ModelMessageCompleted, Timestamp: time.Now().UTC(), Payload: message})
	}
	return result, nil
}

func (a *Adapter) RespondPermission(ctx context.Context, id string, decision adapter.PermissionDecision) error {
	state, err := a.requireSession(id)
	if err != nil {
		return err
	}
	if strings.TrimSpace(decision.RequestID) == "" {
		return fmt.Errorf("permission request id is required")
	}
	// OpenCode 1.17.15: POST /permission/{requestID}/reply with {"reply":"once"|"reject"}.
	// Session id is not part of the route; directory is still attached by requestJSON.
	reply := "reject"
	if decision.Allowed {
		reply = "once"
	}
	return a.requestJSON(ctx, state, http.MethodPost, "/permission/"+url.PathEscape(decision.RequestID)+"/reply", nil, map[string]string{"reply": reply}, nil)
}

func (a *Adapter) GetDiff(ctx context.Context, id string) ([]string, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return nil, err
	}
	var values []struct {
		File string `json:"file"`
		Path string `json:"path"`
	}
	if err := a.requestJSON(ctx, state, http.MethodGet, "/session/"+url.PathEscape(id)+"/diff", nil, nil, &values); err != nil {
		return nil, err
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		path := value.File
		if path == "" {
			path = value.Path
		}
		if path != "" {
			result = append(result, path)
		}
	}
	return result, nil
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
	return event.Input{Timestamp: native.Timestamp, Source: string(adapter.HarnessOpenCode), Type: native.Kind, Severity: severity, Payload: payload}, nil
}

func (a *Adapter) CollectFinalResult(ctx context.Context, id string) (report.Envelope, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return report.Envelope{}, err
	}
	state.mu.Lock()
	turn := state.currentTurn
	state.mu.Unlock()
	if turn == nil {
		// No authoritative generation — do not fall back to historical envelopes.
		return report.Envelope{}, fmt.Errorf("OpenCode: no authoritative generation")
	}
	select {
	case <-turn.ready:
	case <-ctx.Done():
		return report.Envelope{}, ctx.Err()
	}
	state.mu.Lock()
	if state.currentTurn != turn {
		state.mu.Unlock()
		return report.Envelope{}, fmt.Errorf("OpenCode: generation superseded")
	}
	if !turn.completed || !turn.success {
		reason := turn.resultErr
		state.mu.Unlock()
		if reason == "" {
			reason = "generation incomplete"
		}
		return report.Envelope{}, fmt.Errorf("OpenCode session failed: %s", reason)
	}
	raw := append([]byte(nil), turn.final...)
	state.mu.Unlock()
	if len(raw) == 0 {
		return report.Envelope{}, fmt.Errorf("OpenCode: empty final for current generation")
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

func (a *Adapter) startServer(ctx context.Context, executable, directory string) (*transport.Process, string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	p, err := transport.Start(ctx, executable, []string{"serve", "--pure", "--hostname", "127.0.0.1", "--port", strconv.Itoa(port)}, directory)
	if err != nil {
		return nil, "", fmt.Errorf("start OpenCode server: %w", err)
	}
	baseURL := "http://127.0.0.1:" + strconv.Itoa(port)
	if err := a.waitHealth(ctx, baseURL); err != nil {
		_ = p.Terminate(context.Background())
		_ = p.CloseInput()
		return nil, "", fmt.Errorf("OpenCode server health: %w", err)
	}
	return p, baseURL, nil
}

func (a *Adapter) waitHealth(ctx context.Context, baseURL string) error {
	client := &http.Client{Timeout: 750 * time.Millisecond}
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/global/health", nil)
		if err == nil {
			response, requestErr := client.Do(request)
			if requestErr == nil {
				body, _ := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK && bytes.Contains(body, []byte(`"healthy":true`)) {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(75 * time.Millisecond):
		}
	}
}

func (a *Adapter) promptAsync(ctx context.Context, state *sessionState, req adapter.StartRequest) error {
	state.mu.Lock()
	if state.promptInFlight {
		state.mu.Unlock()
		return fmt.Errorf("OpenCode prompt already in flight; concurrent prompt rejected")
	}
	// New generation with independent readiness — never re-arm a prior channel.
	state.generation++
	gen := state.generation
	turn := &ocTurnResult{
		generation: gen,
		ready:      make(chan struct{}),
	}
	state.currentTurn = turn
	state.promptInFlight = true
	state.promptGen = gen
	state.resultError = ""
	state.final = nil
	state.resultSignaled = false
	sessionID := state.sessionID
	state.mu.Unlock()

	// Capture baseline before prompt_async. Failure fails prompt startup with no
	// legacy history fallback — an empty baseline is authoritative for first turn.
	baseline, baselineErr := a.captureMessageIDs(ctx, state)
	if baselineErr != nil {
		state.mu.Lock()
		if state.promptGen == gen && state.currentTurn == turn {
			state.promptInFlight = false
			turn.resultErr = "baseline capture failed: " + baselineErr.Error()
			turn.completed = true
			turn.success = false
			turn.once.Do(func() { close(turn.ready) })
			state.resultError = turn.resultErr
		}
		state.mu.Unlock()
		return fmt.Errorf("OpenCode baseline capture failed: %w", baselineErr)
	}
	state.mu.Lock()
	if turn.generation == gen {
		turn.baselineMessageIDs = baseline
		turn.baselineCaptured = true
	}
	state.mu.Unlock()

	parts := []map[string]string{{"type": "text", "text": req.Contract}}
	body := map[string]any{"parts": parts}
	if req.Model != "" {
		pieces := strings.SplitN(req.Model, "/", 2)
		if len(pieces) == 2 {
			body["model"] = map[string]string{"providerID": pieces[0], "modelID": pieces[1]}
		}
	}
	// prompt_async may return a prompt/message ID; capture it.
	var promptResponse struct {
		ID string `json:"id"`
	}
	err := a.requestJSON(ctx, state, http.MethodPost, "/session/"+url.PathEscape(sessionID)+"/prompt_async", nil, body, &promptResponse)
	if err != nil {
		state.mu.Lock()
		if state.promptGen == gen {
			state.promptInFlight = false
			if state.currentTurn == turn && !turn.completed {
				turn.resultErr = err.Error()
				turn.completed = true
				turn.success = false
				turn.once.Do(func() { close(turn.ready) })
			}
		}
		state.mu.Unlock()
		return err
	}
	// Store the prompt message ID for turn attribution.
	state.mu.Lock()
	if turn.generation == gen && promptResponse.ID != "" {
		turn.promptMessageID = promptResponse.ID
	}
	state.mu.Unlock()
	// prompt_async returns after accept; completion arrives via session.idle.
	return nil
}

// captureMessageIDs returns the set of existing message IDs at prompt start.
// Every returned message must carry non-empty info.id and info.sessionID matching
// the active session. Malformed messages fail closed — never silently skipped.
// An empty response array is valid and authoritative (first-turn baseline).
func (a *Adapter) captureMessageIDs(ctx context.Context, state *sessionState) (map[string]struct{}, error) {
	var messages []openCodeMessage
	if err := a.requestJSON(ctx, state, http.MethodGet, "/session/"+url.PathEscape(state.sessionID)+"/message", nil, nil, &messages); err != nil {
		return nil, err
	}
	set := make(map[string]struct{}, len(messages))
	for _, m := range messages {
		if err := validateOpenCodeMessage(m, state.sessionID); err != nil {
			return nil, err
		}
		set[m.Info.ID] = struct{}{}
	}
	return set, nil
}

// validateOpenCodeMessage enforces the real {info, parts} ownership contract.
func validateOpenCodeMessage(m openCodeMessage, sessionID string) error {
	if strings.TrimSpace(m.Info.ID) == "" {
		return &errOpenCodeContract{reason: "message info.id is required"}
	}
	if strings.TrimSpace(m.Info.SessionID) == "" {
		return &errOpenCodeContract{reason: "message info.sessionID is required"}
	}
	if m.Info.SessionID != sessionID {
		return &errOpenCodeContract{reason: "message info.sessionID does not match active session"}
	}
	return nil
}

func (a *Adapter) subscribeEvents(state *sessionState) {
	defer close(state.sseDone)
	// SSE producer is the graceful closer of the EventStream after it exits.
	defer func() {
		if state.stream != nil {
			state.stream.CloseGracefully()
		}
	}()
	request, err := http.NewRequest(http.MethodGet, state.baseURL+"/event?directory="+url.QueryEscape(state.directory), nil)
	if err != nil {
		return
	}
	// Bound the SSE client so shutdown can progress.
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()
	// Close body when session shuts down so Scan unblocks.
	go func() {
		select {
		case <-state.shutdown:
			_ = response.Body.Close()
		case <-state.sseDone:
		}
	}()
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		select {
		case <-state.shutdown:
			return
		default:
		}
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" {
			continue
		}
		var value sseEvent
		if err := json.Unmarshal([]byte(data), &value); err != nil {
			continue
		}
		a.handleEvent(state, value)
	}
}

func (a *Adapter) handleEvent(state *sessionState, value sseEvent) {
	now := time.Now().UTC()
	native := adapter.NativeEvent{Kind: "opencode." + value.Type, Timestamp: now, Payload: value.Properties}
	publish := true
	switch value.Type {
	case "server.connected":
		// Global server event — unscoped.
		native.Kind = event.SessionStarted
	case "session.created":
		// Session-scoped create: require matching session when present.
		if !a.eventSessionMatches(state, value.Properties) {
			return
		}
		native.Kind = event.SessionStarted
	case "message.part.updated", "message.part.delta", "message.updated":
		if !a.eventSessionMatches(state, value.Properties) {
			return
		}
		native.Kind = event.ModelOutputDelta
		// Mark current-generation activity only for the in-flight turn.
		state.mu.Lock()
		if state.promptInFlight && state.currentTurn != nil && !state.currentTurn.completed {
			state.currentTurn.sawCurrentActivity = true
		}
		state.mu.Unlock()
	case "permission.asked":
		// Permission events are not turn-ownership events. When sessionID is
		// present it must match; when absent (legacy fixtures), still publish.
		if sessionIDFromProps(value.Properties) != "" && !a.eventSessionMatches(state, value.Properties) {
			return
		}
		native.Kind = event.PermissionRequested
	case "session.status":
		publish = a.handleSessionStatus(state, value.Properties, &native)
	case "session.idle":
		// Deprecated idle event — same handler after session correlation.
		if !a.eventSessionMatches(state, value.Properties) {
			return
		}
		publish = a.handleSessionIdle(state, &native)
	case "session.error":
		publish = a.handleSessionError(state, value.Properties, &native)
	}
	if !publish {
		return
	}
	state.mu.Lock()
	state.history = append(state.history, native)
	state.mu.Unlock()
	if state.stream != nil {
		_ = state.stream.Publish(native)
	}
}

// eventSessionMatches requires properties.sessionID == state.sessionID.
// Missing sessionID on a session-scoped event is a contract violation and is ignored.
func (a *Adapter) eventSessionMatches(state *sessionState, properties json.RawMessage) bool {
	sid := sessionIDFromProps(properties)
	if sid == "" {
		return false
	}
	state.mu.Lock()
	active := state.sessionID
	state.mu.Unlock()
	return sid == active
}

func sessionIDFromProps(properties json.RawMessage) string {
	var props openCodeSessionEvent
	if len(properties) == 0 || json.Unmarshal(properties, &props) != nil {
		return ""
	}
	return strings.TrimSpace(props.SessionID)
}

// handleSessionStatus processes current OpenCode session.status events.
// status.type=="idle" completes via handleSessionIdle; busy/retry mark activity only.
func (a *Adapter) handleSessionStatus(state *sessionState, properties json.RawMessage, native *adapter.NativeEvent) bool {
	var props openCodeSessionStatusEvent
	if len(properties) == 0 || json.Unmarshal(properties, &props) != nil {
		return false
	}
	if strings.TrimSpace(props.SessionID) == "" {
		return false
	}
	state.mu.Lock()
	active := state.sessionID
	inFlight := state.promptInFlight
	turn := state.currentTurn
	state.mu.Unlock()
	if props.SessionID != active {
		return false
	}
	switch props.Status.Type {
	case "idle":
		return a.handleSessionIdle(state, native)
	case "busy", "retry":
		if inFlight && turn != nil && !turn.completed {
			state.mu.Lock()
			if state.promptInFlight && state.currentTurn != nil && !state.currentTurn.completed {
				state.currentTurn.sawCurrentActivity = true
			}
			state.mu.Unlock()
		}
		return true
	default:
		// Unknown status values must not complete the turn.
		return true
	}
}

// handleSessionError fails the current in-flight generation exactly once when
// sessionID matches. Error text is redacted/stable — no provider payloads.
func (a *Adapter) handleSessionError(state *sessionState, properties json.RawMessage, native *adapter.NativeEvent) bool {
	var props openCodeSessionEvent
	if len(properties) == 0 || json.Unmarshal(properties, &props) != nil {
		return false
	}
	if strings.TrimSpace(props.SessionID) == "" {
		// No session ID: do not attribute to the active turn.
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if props.SessionID != state.sessionID {
		return false
	}
	turn := state.currentTurn
	gen := state.promptGen
	if !state.promptInFlight || turn == nil || turn.completed || turn.generation != gen {
		return false
	}
	turn.resultErr = "OpenCode session error"
	turn.success = false
	turn.completed = true
	state.promptInFlight = false
	state.resultError = turn.resultErr
	turn.once.Do(func() { close(turn.ready) })
	state.resultSignaled = true
	native.Kind = event.TurnFailed
	return true
}

// handleSessionIdle implements the production current-turn idle path.
// Returns false when the idle is ignored (stale/duplicate/early) so no boundary
// event is published.
func (a *Adapter) handleSessionIdle(state *sessionState, native *adapter.NativeEvent) bool {
	// 1) Capture exact turn pointer, generation, and prompt state under lock.
	state.mu.Lock()
	turn := state.currentTurn
	gen := state.promptGen
	inFlight := state.promptInFlight
	if !inFlight || turn == nil || turn.completed || turn.generation != gen {
		state.mu.Unlock()
		// No prompt in flight / already completed / generation mismatch: ignore.
		return false
	}
	sawActivity := turn.sawCurrentActivity
	state.mu.Unlock()

	// 2) Without current-generation activity, treat idle as stale/early.
	if !sawActivity {
		return false
	}

	// 3) Collect outside the lock using the captured turn pointer.
	collected, err := a.collectFinalTurn(state, turn)

	// 4) Re-lock and verify ownership before freezing completion.
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.currentTurn != turn || state.promptGen != gen || !state.promptInFlight {
		// Turn superseded or no longer in flight — collection must not mutate.
		return false
	}
	if turn.completed {
		return false
	}

	// 5) Apply collection semantics.
	if err != nil {
		// Transport/collection error with attributable activity → TurnFailed.
		turn.resultErr = err.Error()
		turn.success = false
		turn.completed = true
		state.promptInFlight = false
		state.resultError = err.Error()
		turn.once.Do(func() { close(turn.ready) })
		state.resultSignaled = true
		native.Kind = event.TurnFailed
		return true
	}
	if collected.HasEnvelope {
		turn.final = append([]byte(nil), collected.Final...)
		turn.usage = collected.Usage
		turn.success = true
		turn.completed = true
		state.final = append([]byte(nil), collected.Final...)
		state.usage = collected.Usage
		state.promptInFlight = false
		turn.once.Do(func() { close(turn.ready) })
		state.resultSignaled = true
		native.Kind = event.ResultSubmitted
		return true
	}
	if collected.InvalidOutput {
		// Assistant output exists but no valid Result Envelope.
		turn.resultErr = "OpenCode: current-turn assistant output without valid Result Envelope"
		turn.success = false
		turn.completed = true
		state.promptInFlight = false
		state.resultError = turn.resultErr
		turn.once.Do(func() { close(turn.ready) })
		state.resultSignaled = true
		native.Kind = event.TurnFailed
		return true
	}
	// No attributable current-turn assistant output → ignore as stale/early.
	// Do not clear promptInFlight, do not close readiness, do not publish.
	return false
}

// collectFinalTestOnly is a legacy test utility for fixtures that inject final
// state without promptAsync. Production idle must use collectFinalTurn with a
// non-nil turn. Still parses the real {info, parts} message schema.
func (a *Adapter) collectFinalTestOnly(state *sessionState) error {
	// Build a synthetic turn with empty authoritative baseline so first-turn
	// semantics apply without whole-history fallback.
	turn := &ocTurnResult{
		baselineCaptured:   true,
		baselineMessageIDs: map[string]struct{}{},
	}
	result, err := a.collectFinalTurn(state, turn)
	if err != nil {
		return err
	}
	if !result.HasEnvelope {
		return fmt.Errorf("OpenCode returned no valid Result Envelope in assistant messages")
	}
	state.mu.Lock()
	state.final = append([]byte(nil), result.Final...)
	state.usage = result.Usage
	state.mu.Unlock()
	return nil
}

// collectFinalTurn scans messages attributable to the captured turn only.
// Production calls must pass a non-nil turn with baselineCaptured==true.
// Filtering uses message.Info.ID against baselineMessageIDs even when the map
// is empty (authoritative first-turn boundary). Does not use map length as
// proof of capture. Does not mutate global session state.
func (a *Adapter) collectFinalTurn(state *sessionState, turn *ocTurnResult) (collectedTurnResult, error) {
	if turn == nil {
		return collectedTurnResult{}, fmt.Errorf("OpenCode: collectFinalTurn requires a non-nil turn")
	}
	if !turn.baselineCaptured {
		return collectedTurnResult{}, fmt.Errorf("OpenCode: collectFinalTurn requires baselineCaptured")
	}
	var messages []openCodeMessage
	if err := a.requestJSON(context.Background(), state, http.MethodGet, "/session/"+url.PathEscape(state.sessionID)+"/message", nil, nil, &messages); err != nil {
		return collectedTurnResult{}, err
	}

	var firstInvalid []byte
	var firstInvalidUsage adapter.Usage
	for i := len(messages) - 1; i >= 0; i-- {
		message := messages[i]
		// Validate ownership before examining result contents.
		if err := validateOpenCodeMessage(message, state.sessionID); err != nil {
			return collectedTurnResult{}, err
		}
		if message.Info.Role != "assistant" {
			continue
		}
		if _, existed := turn.baselineMessageIDs[message.Info.ID]; existed {
			continue
		}
		var text strings.Builder
		for _, part := range message.Parts {
			if part.Type == "text" {
				text.WriteString(part.Text)
			}
		}
		if text.Len() == 0 {
			continue
		}
		raw := []byte(text.String())
		usage := adapter.Usage{
			InputTokens: message.Info.Tokens.Input, OutputTokens: message.Info.Tokens.Output,
			Cost: message.Info.Cost, Currency: "USD",
		}
		if _, err := protocol.ParseEnvelope(raw); err != nil {
			// Track invalid assistant output for TurnFailed when no envelope found.
			if firstInvalid == nil {
				firstInvalid = raw
				firstInvalidUsage = usage
			}
			continue
		}
		return collectedTurnResult{
			Final: raw, Usage: usage, SawActivity: true, HasEnvelope: true,
		}, nil
	}
	if firstInvalid != nil {
		return collectedTurnResult{
			Final: firstInvalid, Usage: firstInvalidUsage,
			SawActivity: true, InvalidOutput: true,
		}, nil
	}
	// No attributable current-turn assistant output.
	return collectedTurnResult{}, nil
}

func (a *Adapter) requestJSON(ctx context.Context, state *sessionState, method, route string, query url.Values, body any, target any) error {
	requestURL := state.baseURL + route
	values := url.Values{"directory": []string{state.directory}}
	for key, items := range query {
		values[key] = items
	}
	if len(values) > 0 {
		requestURL += "?" + values.Encode()
	}
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, requestURL, reader)
	if err != nil {
		return err
	}
	if body != nil {
		request.Header.Set("content-type", "application/json")
	}
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("OpenCode %s %s: HTTP %d: %s", method, route, response.StatusCode, strings.TrimSpace(string(data)))
	}
	if target != nil && len(bytes.TrimSpace(data)) > 0 {
		return json.Unmarshal(data, target)
	}
	return nil
}

func (a *Adapter) stop(state *sessionState) error {
	state.shutdownOnce.Do(func() { close(state.shutdown) })
	if state.stream != nil {
		go state.stream.Abort()
	}
	err := state.process.Terminate(context.Background())
	_ = state.process.CloseInput()
	return err
}

func (a *Adapter) drainServerOutput(p *transport.Process) {
	for range p.Lines() {
	}
}

func (a *Adapter) watchExit(state *sessionState) {
	status, err := state.process.Wait(context.Background())
	if err != nil {
		return
	}
	state.finishOnce.Do(func() {
		state.mu.Lock()
		if status.Code != 0 && len(state.final) == 0 {
			state.resultError = status.Error
		}
		signaled := state.resultSignaled
		if !signaled {
			state.resultSignaled = true
		}
		state.mu.Unlock()
		if !signaled {
			close(state.resultReady)
		}
	})
	// Stop SSE and let subscribeEvents gracefully close the EventStream.
	state.shutdownOnce.Do(func() { close(state.shutdown) })
	// If SSE is stalled, Abort unblocks the stream owner.
	if state.stream != nil {
		select {
		case <-state.sseDone:
		case <-time.After(2 * time.Second):
			state.stream.Abort()
		}
	}
}

func (a *Adapter) requireSession(id string) (*sessionState, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state, ok := a.sessions[id]
	if !ok {
		return nil, fmt.Errorf("unknown OpenCode session %q", id)
	}
	return state, nil
}

func sessionFromState(state *sessionState) adapter.Session {
	i := state.process.Identity()
	var events <-chan adapter.NativeEvent
	if state.stream != nil {
		events = state.stream.Events()
	}
	return adapter.Session{NativeSessionID: state.sessionID, PID: i.PID, ProcessStartToken: i.StartToken, Events: events, Exited: state.process.Exited(), Stderr: state.process.Stderr()}
}
