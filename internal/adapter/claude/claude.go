package claude

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/project"
	"github.com/vnai/subagent-broker/internal/report"
)

const testedVersion = "2.1.197"

const resultSchema = `{"type":"object","required":["schema_version","task_id","worker_id","status","summary","work_completed","files_changed","no_files_changed_reason","validation","remaining_work","blocking_issues","risks","handoff_notes"],"properties":{"schema_version":{"const":"v1alpha1"},"task_id":{"type":"string"},"worker_id":{"type":"string"},"status":{"enum":["succeeded","partial","blocked","failed","cancelled"]},"summary":{"type":"string"},"work_completed":{"type":"array","items":{"type":"string"}},"files_changed":{"type":"array","items":{"type":"string"}},"no_files_changed_reason":{"type":"string"},"validation":{"type":"array","items":{"type":"object","required":["command","passed"],"properties":{"command":{"type":"string"},"passed":{"type":"boolean"},"details":{"type":"string"}}}},"remaining_work":{"type":"array","items":{"type":"string"}},"blocking_issues":{"type":"array","items":{"type":"string"}},"risks":{"type":"array","items":{"type":"string"}},"handoff_notes":{"type":"array","items":{"type":"string"}}}}`

type Adapter struct {
	mu           sync.Mutex
	executable   string
	capabilities adapter.Capabilities
	sessions     map[string]*sessionState
}

type sessionState struct {
	mu            sync.Mutex
	id            string
	cmd           *exec.Cmd
	stdin         io.WriteCloser
	events        chan adapter.NativeEvent
	exited        chan adapter.ExitStatus
	stderr        chan adapter.OutputChunk
	resultReady   chan struct{}
	resultOnce    sync.Once
	result        json.RawMessage
	resultSubtype string
	usage         adapter.Usage
	history       []adapter.NativeEvent
	identity      process.Identity
	projectRoot   string
	closed        bool
	closeEvents   sync.Once
	closeStderr   sync.Once
}

func New(executable string) *Adapter {
	if strings.TrimSpace(executable) == "" {
		executable = "claude"
	}
	return &Adapter{
		executable: executable,
		capabilities: adapter.Capabilities{
			StructuredStream: true, BidirectionalStream: true, ResumeSession: true,
			SteerActiveTurn: true, InterruptTurn: true, StructuredFinalOutput: true,
			NativeSubagents: true, Hooks: true, SessionHistory: true, UsageEvents: true, PermissionEvents: true,
		},
		sessions: map[string]*sessionState{},
	}
}

func (a *Adapter) Descriptor() adapter.Descriptor {
	return adapter.Descriptor{
		Name:               adapter.HarnessClaudeCode,
		AdapterVersion:     "phase1",
		TestedMinVersion:   testedVersion,
		TestedMaxVersion:   testedVersion,
		Capabilities:       a.capabilities,
		RuntimeImplemented: true,
		Compatibility:      "verified",
	}
}

func (a *Adapter) Probe(ctx context.Context, req adapter.ProbeRequest) (adapter.ProbeResult, error) {
	executable := a.executable
	if strings.TrimSpace(req.Executable) != "" {
		executable = req.Executable
	}
	path, err := exec.LookPath(executable)
	if err != nil {
		return adapter.ProbeResult{Installed: false, Compatibility: "unavailable", Warnings: []string{err.Error()}}, nil
	}
	versionOutput, err := exec.CommandContext(ctx, path, "--version").CombinedOutput()
	if err != nil {
		return adapter.ProbeResult{Installed: true, Compatibility: "probe_failed", Warnings: []string{string(versionOutput)}}, nil
	}
	version := strings.TrimSpace(string(versionOutput))
	compatibility := "verified"
	var warnings []string
	if !strings.Contains(version, testedVersion) {
		compatibility = "compatibility_unverified"
		warnings = append(warnings, fmt.Sprintf("adapter was validated against Claude Code %s, found %s", testedVersion, version))
	}
	authenticated, authWarning := probeAuth(ctx, path)
	if authWarning != "" {
		warnings = append(warnings, authWarning)
	}
	return adapter.ProbeResult{
		Installed: true, Version: version, Authenticated: authenticated,
		Capabilities: a.capabilities, Compatibility: compatibility, Warnings: warnings,
	}, nil
}

func probeAuth(ctx context.Context, executable string) (*bool, string) {
	output, err := exec.CommandContext(ctx, executable, "auth", "status").CombinedOutput()
	if err != nil {
		return nil, fmt.Sprintf("auth status unavailable: %s", strings.TrimSpace(string(output)))
	}
	var status struct {
		LoggedIn bool `json:"loggedIn"`
	}
	if err := json.Unmarshal(output, &status); err != nil {
		return nil, fmt.Sprintf("could not parse auth status: %v", err)
	}
	return &status.LoggedIn, ""
}

func (a *Adapter) StartSession(ctx context.Context, req adapter.StartRequest) (adapter.Session, error) {
	if strings.TrimSpace(req.ProjectRoot) == "" {
		return adapter.Session{}, fmt.Errorf("project root is required")
	}
	if strings.TrimSpace(req.Contract) == "" {
		return adapter.Session{}, fmt.Errorf("task contract is required")
	}
	id := strings.TrimSpace(req.Options["session_id"])
	if id == "" {
		generated, err := project.UUIDv7(time.Now().UTC(), rand.Reader)
		if err != nil {
			return adapter.Session{}, err
		}
		id = generated
	}
	args := []string{"-p", "--output-format", "stream-json", "--input-format", "stream-json", "--include-partial-messages", "--verbose", "--json-schema", resultSchema}
	if strings.EqualFold(req.Options["resume"], "true") {
		args = append(args, "--resume", id)
	} else {
		args = append(args, "--session-id", id)
	}
	if req.Model != "" {
		args = append(args, "--model", req.Model)
	}
	if mode := req.Options["permission_mode"]; mode != "" {
		args = append(args, "--permission-mode", mode)
	}
	if maxTurns := req.Options["max_turns"]; maxTurns != "" {
		args = append(args, "--max-turns", maxTurns)
	}
	if strings.EqualFold(req.Options["safe_mode"], "true") {
		args = append(args, "--safe-mode")
	}
	if req.Interaction.Enabled {
		launch, err := BuildInteractionLaunch(req)
		if err != nil {
			return adapter.Session{}, err
		}
		args = append(args, launch.ExtraArgs...)
	}

	cmd := exec.CommandContext(ctx, a.executable, args...)
	cmd.Dir = req.ProjectRoot
	process.ConfigureCommand(cmd)
	if req.Interaction.Enabled && req.Interaction.WorkerToken != "" {
		// Worker token and run metadata via parent env only (never argv / MCP config / hook command).
		cmd.Env = append(os.Environ(), WorkerProcessEnv(req)...)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return adapter.Session{}, err
	}
	stdout, stdoutWriter, err := os.Pipe()
	if err != nil {
		return adapter.Session{}, err
	}
	stderr, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		return adapter.Session{}, err
	}
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		_ = stderr.Close()
		_ = stderrWriter.Close()
		return adapter.Session{}, err
	}
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	identity, identityErr := process.Inspect(context.Background(), cmd.Process.Pid)
	if identityErr != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return adapter.Session{}, fmt.Errorf("inspect Claude process: %w", identityErr)
	}

	state := &sessionState{
		id:          id,
		cmd:         cmd,
		stdin:       stdin,
		events:      make(chan adapter.NativeEvent, 256),
		exited:      make(chan adapter.ExitStatus, 1),
		stderr:      make(chan adapter.OutputChunk, 64),
		resultReady: make(chan struct{}),
		identity:    identity,
		projectRoot: req.ProjectRoot,
	}
	a.mu.Lock()
	a.sessions[id] = state
	a.mu.Unlock()

	go a.readStdout(state, stdout)
	go a.readStderr(state, stderr)
	go a.waitProcess(state)
	if err := sendUserMessage(state, req.Contract); err != nil {
		_ = a.TerminateSession(context.Background(), id)
		return adapter.Session{}, err
	}
	return sessionFromState(state), nil
}

func (a *Adapter) ResumeSession(ctx context.Context, req adapter.ResumeRequest) (adapter.Session, error) {
	if strings.TrimSpace(req.NativeSessionID) == "" {
		return adapter.Session{}, fmt.Errorf("native session id is required")
	}
	options := cloneOptions(req.Options)
	options["session_id"] = req.NativeSessionID
	options["resume"] = "true"
	return a.StartSession(ctx, adapter.StartRequest{
		RunID: req.RunID, TaskID: req.TaskID, WorkerID: req.WorkerID,
		ProjectRoot: req.ProjectRoot, Contract: req.Contract, Model: req.Model, Options: options,
		Interaction: req.Interaction,
	})
}

func (a *Adapter) SendMessage(_ context.Context, id, message string) (adapter.DeliveryResult, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return adapter.DeliveryResult{}, err
	}
	if err := sendUserMessage(state, message); err != nil {
		return adapter.DeliveryResult{}, err
	}
	// Stdin write alone only proves next-turn queueing unless a contract test
	// has verified same-turn steer for this harness version.
	return adapter.DeliveryResult{Mode: adapter.DeliveryNextTurn, MessageID: message}, nil
}

func (a *Adapter) SteerActiveTurn(ctx context.Context, id, message string) (adapter.DeliveryResult, error) {
	// Without a verified active-turn contract, do not claim DeliveryImmediate.
	// Writing stdin successfully is not proof of same-turn injection.
	return a.SendMessage(ctx, id, message)
}

// SessionConfigFact returns what this adapter actually installs for a start request.
func (a *Adapter) SessionConfigFact(req adapter.StartRequest) adapter.SessionConfigFact {
	mode := req.Options["permission_mode"]
	safe := strings.EqualFold(req.Options["safe_mode"], "true")
	hooks := req.Interaction.Enabled && !safe
	return adapter.SessionConfigFact{
		PermissionMode:   mode,
		HooksInstalled:   hooks,
		MCPEnabled:       req.Interaction.Enabled && !safe,
		SafeMode:         safe,
		SteerVerified:    false, // real contract test may flip this via contract registry
		NextTurnDelivery: true,
	}
}

func (a *Adapter) InterruptTurn(ctx context.Context, id string) error {
	state, err := a.requireSession(id)
	if err != nil {
		return err
	}
	return process.Interrupt(ctx, state.identity)
}

func (a *Adapter) TerminateSession(ctx context.Context, id string) error {
	state, err := a.requireSession(id)
	if err != nil {
		return err
	}
	if err := process.TerminateGracefully(ctx, state.identity); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	_ = state.stdin.Close()
	return nil
}

func (a *Adapter) ReadHistory(_ context.Context, id string) ([]adapter.NativeEvent, error) {
	state, err := a.requireSession(id)
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	result := make([]adapter.NativeEvent, len(state.history))
	copy(result, state.history)
	return result, nil
}

func (a *Adapter) RespondPermission(context.Context, string, adapter.PermissionDecision) error {
	return adapter.ErrUnsupported
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
	return event.Input{Timestamp: native.Timestamp, Source: "claude-code", Type: native.Kind, Severity: severity, Payload: payload}, nil
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
	raw := append(json.RawMessage(nil), state.result...)
	subtype := state.resultSubtype
	state.mu.Unlock()
	if subtype != "" && subtype != "success" {
		return report.Envelope{}, fmt.Errorf("Claude result subtype %q", subtype)
	}
	if len(raw) == 0 {
		return report.Envelope{}, fmt.Errorf("Claude returned no structured result")
	}
	if len(raw) > 0 && raw[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return report.Envelope{}, fmt.Errorf("decode Claude result text: %w", err)
		}
		raw = json.RawMessage(text)
	}
	var result report.Envelope
	if err := json.Unmarshal(raw, &result); err != nil {
		return report.Envelope{}, fmt.Errorf("decode Result Envelope: %w", err)
	}
	if err := report.ValidateEnvelope(result); err != nil {
		return report.Envelope{}, err
	}
	return result, nil
}

func (a *Adapter) requireSession(id string) (*sessionState, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state, ok := a.sessions[id]
	if !ok {
		return nil, fmt.Errorf("unknown Claude session %q", id)
	}
	return state, nil
}

func sessionFromState(state *sessionState) adapter.Session {
	return adapter.Session{
		NativeSessionID: state.id, PID: state.identity.PID, ProcessStartToken: state.identity.StartToken,
		Events: state.events, Exited: state.exited, Stderr: state.stderr,
	}
}

func sendUserMessage(state *sessionState, message string) error {
	payload := map[string]any{
		"type": "user",
		"message": map[string]any{
			"role":    "user",
			"content": []map[string]string{{"type": "text", "text": message}},
		},
	}
	line, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return os.ErrProcessDone
	}
	_, err = state.stdin.Write(append(line, '\n'))
	return err
}

func (a *Adapter) readStdout(state *sessionState, stdout io.Reader) {
	defer state.closeEvents.Do(func() { close(state.events) })
	defer state.resultOnce.Do(func() { close(state.resultReady) })
	reader := bufio.NewReader(stdout)
	for {
		line, err := reader.ReadBytes('\n')
		if len(strings.TrimSpace(string(line))) > 0 {
			native := nativeEvent(line)
			state.mu.Lock()
			state.history = append(state.history, native)
			state.mu.Unlock()
			captureResult(state, native)
			state.events <- native
		}
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			if errors.Is(err, os.ErrClosed) || strings.Contains(err.Error(), "file already closed") {
				return
			}
			payload, _ := json.Marshal(map[string]string{"error": err.Error()})
			state.events <- adapter.NativeEvent{Kind: "protocol.error", Timestamp: time.Now().UTC(), Payload: payload}
			return
		}
	}
}

func (a *Adapter) readStderr(state *sessionState, stderr io.Reader) {
	defer state.closeStderr.Do(func() { close(state.stderr) })
	buffer := make([]byte, 4096)
	for {
		n, err := stderr.Read(buffer)
		if n > 0 {
			data := append([]byte(nil), buffer[:n]...)
			state.stderr <- adapter.OutputChunk{Timestamp: time.Now().UTC(), Data: data}
		}
		if err != nil {
			return
		}
	}
}

func (a *Adapter) waitProcess(state *sessionState) {
	err := state.cmd.Wait()
	status := adapter.ExitStatus{Code: 0}
	if err != nil {
		status.Code = -1
		status.Error = err.Error()
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			status.Code = exitErr.ExitCode()
			if waitStatus, ok := exitErr.Sys().(syscall.WaitStatus); ok && waitStatus.Signaled() {
				status.Signal = waitStatus.Signal().String()
			}
		}
	}
	state.mu.Lock()
	state.closed = true
	_ = state.stdin.Close()
	state.mu.Unlock()
	state.exited <- status
	close(state.exited)
}

func nativeEvent(line []byte) adapter.NativeEvent {
	now := time.Now().UTC()
	trimmed := bytesTrimSpace(line)
	var message struct {
		Type      string          `json:"type"`
		Subtype   string          `json:"subtype"`
		Message   json.RawMessage `json:"message"`
		Result    json.RawMessage `json:"result"`
		SessionID string          `json:"session_id"`
		Cost      float64         `json:"total_cost_usd"`
		IsError   bool            `json:"is_error"`
	}
	if err := json.Unmarshal(trimmed, &message); err != nil {
		payload, _ := json.Marshal(map[string]string{"raw": string(trimmed), "error": err.Error()})
		return adapter.NativeEvent{Kind: "protocol.error", Timestamp: now, Payload: payload}
	}
	kind := eventForMessage(message.Type, message.Subtype, message.Message, message.IsError)
	return adapter.NativeEvent{Kind: kind, Timestamp: now, Payload: append([]byte(nil), trimmed...)}
}

func captureResult(state *sessionState, native adapter.NativeEvent) {
	if native.Kind != event.ResultSubmitted && native.Kind != event.TurnFailed {
		return
	}
	var message struct {
		Subtype string          `json:"subtype"`
		Result  json.RawMessage `json:"result"`
		Cost    float64         `json:"total_cost_usd"`
		IsError bool            `json:"is_error"`
	}
	if err := json.Unmarshal(native.Payload, &message); err != nil {
		return
	}
	state.mu.Lock()
	state.result = append(json.RawMessage(nil), message.Result...)
	state.resultSubtype = message.Subtype
	if message.IsError {
		state.resultSubtype = "error"
	}
	state.usage = adapter.Usage{Cost: message.Cost, Currency: "USD"}
	state.mu.Unlock()
	state.resultOnce.Do(func() { close(state.resultReady) })
}

func eventForMessage(messageType, subtype string, rawMessage json.RawMessage, isError bool) string {
	switch messageType {
	case "system":
		if subtype == "init" {
			return event.SessionStarted
		}
		if subtype != "" {
			return "protocol." + subtype
		}
		return "protocol.system"
	case "assistant":
		var message struct {
			Content []struct {
				Type string `json:"type"`
			} `json:"content"`
		}
		if json.Unmarshal(rawMessage, &message) == nil {
			for _, content := range message.Content {
				if content.Type == "tool_use" {
					return event.ToolStarted
				}
			}
		}
		return event.ModelOutputDelta
	case "user":
		return event.ToolOutput
	case "result":
		if subtype == "success" && !isError {
			return event.ResultSubmitted
		}
		return event.TurnFailed
	default:
		if messageType == "" {
			return "protocol.unknown"
		}
		return "claude." + messageType
	}
}

func bytesTrimSpace(value []byte) []byte {
	return []byte(strings.TrimSpace(string(value)))
}

func cloneOptions(options map[string]string) map[string]string {
	result := make(map[string]string, len(options)+2)
	for key, value := range options {
		result[key] = value
	}
	return result
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

// InteractionLaunch is the pure production command-construction result for
// Claude MCP + permission-hook wiring. Used by StartSession and tests.
type InteractionLaunch struct {
	// ExtraArgs are Claude CLI flags: --mcp-config, --settings, tools, etc.
	ExtraArgs []string
	// MCPConfigJSON is the serialized MCP config (never contains the Worker token).
	MCPConfigJSON string
	// SettingsJSON is the serialized Claude settings including the hook command.
	SettingsJSON string
	// HookCommand is the shell command for PreToolUse (no secrets / identity).
	HookCommand string
}

// BuildInteractionLaunch constructs the production MCP config and hook settings
// from a StartRequest. Identity and token live only in WorkerProcessEnv.
func BuildInteractionLaunch(req adapter.StartRequest) (InteractionLaunch, error) {
	// MCP config contains only the executable and non-secret command shape.
	mcpServer := map[string]any{
		"type": "stdio", "command": req.Interaction.BrokerExecutable,
		"args": []string{"mcp-worker"},
	}
	config, err := json.Marshal(map[string]any{"mcpServers": map[string]any{"subagent-broker": mcpServer}})
	if err != nil {
		return InteractionLaunch{}, err
	}
	// Hook command avoids embedding run directories or credentials in argv.
	hookCommand := strings.Join([]string{shellQuote(req.Interaction.BrokerExecutable), "claude-hook"}, " ")
	settingsValue := map[string]any{
		"permissions": map[string]any{"allow": []string{"Bash", "Write", "Edit"}},
		"hooks": map[string]any{"PreToolUse": []any{map[string]any{
			"matcher": "Bash|Write|Edit",
			"hooks":   []any{map[string]any{"type": "command", "command": hookCommand}},
		}}},
	}
	settings, err := json.Marshal(settingsValue)
	if err != nil {
		return InteractionLaunch{}, err
	}
	extra := []string{
		"--mcp-config", string(config),
		"--settings", string(settings),
		"--include-hook-events",
		"--allowedTools", "mcp__subagent-broker__ask_main_agent,mcp__subagent-broker__request_scope_expansion",
		"--disallowedTools", "Agent,Task,AskUserQuestion",
	}
	return InteractionLaunch{
		ExtraArgs:     extra,
		MCPConfigJSON: string(config),
		SettingsJSON:  string(settings),
		HookCommand:   hookCommand,
	}, nil
}

// WorkerProcessEnv returns the environment entries injected into the Claude
// Worker process. The raw Worker token is present only here — never argv/JSON.
func WorkerProcessEnv(req adapter.StartRequest) []string {
	env := []string{
		"BROKER_WORKER_TOKEN=" + req.Interaction.WorkerToken,
		"BROKER_WORKER_SOCKET=" + req.Interaction.WorkerSocket,
		"BROKER_RUN_DIR=" + req.Interaction.RunDir,
		"BROKER_RUN_ID=" + req.RunID,
		"BROKER_TASK_ID=" + req.TaskID,
		"BROKER_WORKER_ID=" + req.WorkerID,
	}
	if req.Interaction.NativeSessionID != "" {
		env = append(env, "BROKER_NATIVE_SESSION_ID="+req.Interaction.NativeSessionID)
	}
	return env
}
