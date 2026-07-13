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
	mu              sync.Mutex
	process         *transport.Process
	baseURL         string
	directory       string
	sessionID       string
	workerID        string
	events          chan adapter.NativeEvent
	resultReady     chan struct{}
	resultOnce      sync.Once
	finishOnce      sync.Once
	closeEvents     sync.Once
	shutdown        chan struct{}
	shutdownOnce    sync.Once
	history         []adapter.NativeEvent
	final           []byte
	resultError     string
	usage           adapter.Usage
	droppedProgress uint64
	closed          bool
	generation      uint64
	promptInFlight  bool
	promptGen       uint64
	resultSignaled  bool
}

type sseEvent struct {
	Type       string          `json:"type"`
	Properties json.RawMessage `json:"properties"`
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
		events: make(chan adapter.NativeEvent, 256), resultReady: make(chan struct{}), shutdown: make(chan struct{}),
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
	select {
	case <-state.resultReady:
	case <-ctx.Done():
		return report.Envelope{}, ctx.Err()
	}
	state.mu.Lock()
	raw := append([]byte(nil), state.final...)
	reason := state.resultError
	state.mu.Unlock()
	if reason != "" {
		return report.Envelope{}, fmt.Errorf("OpenCode session failed: %s", reason)
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
	// Multi-turn: re-arm result readiness after a prior session.idle completed.
	if state.resultSignaled {
		state.resultReady = make(chan struct{})
		state.finishOnce = sync.Once{}
		state.resultOnce = sync.Once{}
		state.resultSignaled = false
		state.resultError = ""
	}
	state.generation++
	gen := state.generation
	state.promptInFlight = true
	state.promptGen = gen
	sessionID := state.sessionID
	state.mu.Unlock()

	parts := []map[string]string{{"type": "text", "text": req.Contract}}
	body := map[string]any{"parts": parts}
	if req.Model != "" {
		pieces := strings.SplitN(req.Model, "/", 2)
		if len(pieces) == 2 {
			body["model"] = map[string]string{"providerID": pieces[0], "modelID": pieces[1]}
		}
	}
	err := a.requestJSON(ctx, state, http.MethodPost, "/session/"+url.PathEscape(sessionID)+"/prompt_async", nil, body, nil)
	if err != nil {
		state.mu.Lock()
		if state.promptGen == gen {
			state.promptInFlight = false
		}
		state.mu.Unlock()
		return err
	}
	// prompt_async returns after accept; completion arrives via session.idle.
	return nil
}

func (a *Adapter) subscribeEvents(state *sessionState) {
	request, err := http.NewRequest(http.MethodGet, state.baseURL+"/event?directory="+url.QueryEscape(state.directory), nil)
	if err != nil {
		return
	}
	response, err := (&http.Client{}).Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
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
	switch value.Type {
	case "server.connected", "session.created":
		native.Kind = event.SessionStarted
	case "message.part.updated", "message.updated":
		native.Kind = event.ModelOutputDelta
	case "permission.asked":
		native.Kind = event.PermissionRequested
	case "session.idle":
		// Freeze current generation result, then emit authoritative ResultSubmitted.
		// Keep the server alive; Supervisor decides next turn vs terminate.
		state.mu.Lock()
		gen := state.promptGen
		if state.promptInFlight && state.promptGen == gen {
			state.promptInFlight = false
		}
		state.mu.Unlock()
		if err := a.collectFinal(state); err != nil {
			state.mu.Lock()
			if gen == state.generation || gen == state.promptGen {
				state.resultError = err.Error()
			}
			state.mu.Unlock()
			native.Kind = event.TurnFailed
		} else {
			native.Kind = event.ResultSubmitted
		}
	}
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
	if value.Type == "session.idle" {
		state.mu.Lock()
		if !state.resultSignaled {
			state.resultSignaled = true
			close(state.resultReady)
		}
		state.mu.Unlock()
	}
}

func (a *Adapter) collectFinal(state *sessionState) error {
	var messages []struct {
		Info struct {
			Role   string `json:"role"`
			Tokens struct {
				Input  int64 `json:"input"`
				Output int64 `json:"output"`
			} `json:"tokens"`
			Cost float64 `json:"cost"`
		} `json:"info"`
		Parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"parts"`
	}
	if err := a.requestJSON(context.Background(), state, http.MethodGet, "/session/"+url.PathEscape(state.sessionID)+"/message", nil, nil, &messages); err != nil {
		return err
	}
	// Newest → oldest: pick the newest assistant text that is a valid Result Envelope.
	// Do not concatenate historical turns into one document.
	for i := len(messages) - 1; i >= 0; i-- {
		message := messages[i]
		if message.Info.Role != "assistant" {
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
		if _, err := protocol.ParseEnvelope(raw); err != nil {
			continue
		}
		state.mu.Lock()
		state.final = raw
		state.usage = adapter.Usage{
			InputTokens: message.Info.Tokens.Input, OutputTokens: message.Info.Tokens.Output,
			Cost: message.Info.Cost, Currency: "USD",
		}
		state.mu.Unlock()
		return nil
	}
	return fmt.Errorf("OpenCode returned no valid Result Envelope in assistant messages")
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
		state.mu.Unlock()
		close(state.resultReady)
	})
	state.shutdownOnce.Do(func() { close(state.shutdown) })
	state.closeEvents.Do(func() {
		state.mu.Lock()
		state.closed = true
		state.mu.Unlock()
		close(state.events)
	})
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
	return adapter.Session{NativeSessionID: state.sessionID, PID: i.PID, ProcessStartToken: i.StartToken, Events: state.events, Exited: state.process.Exited(), Stderr: state.process.Stderr()}
}
