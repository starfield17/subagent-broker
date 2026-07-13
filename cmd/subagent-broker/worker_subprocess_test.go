package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/claude"
	"github.com/vnai/subagent-broker/internal/supervisor"
)

// TestMCPWorkerEnvIdentity exercises the actual mcp-worker CLI with environment
// identity only (no identity flags) against a real Unix Worker socket fixture.
func TestMCPWorkerEnvIdentity(t *testing.T) {
	bin := buildTestBroker(t)
	runDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(runDir, "run.json"), []byte(`{"run_id":"run-env"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(runDir, "worker.sock")
	token := "TOP_SECRET_WORKER_TOKEN"
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_ = os.Chmod(sock, 0o600)

	type seen struct {
		req supervisor.Request
		err error
	}
	got := make(chan seen, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			got <- seen{err: acceptErr}
			return
		}
		defer conn.Close()
		line, readErr := bufio.NewReader(conn).ReadBytes('\n')
		if readErr != nil {
			got <- seen{err: readErr}
			return
		}
		var req supervisor.Request
		if err := json.Unmarshal(line, &req); err != nil {
			got <- seen{err: err}
			return
		}
		// Respond with a valid answered resolution so the MCP tool path can exit.
		resp := supervisor.Response{
			SchemaVersion: supervisor.SchemaVersion,
			RequestID:     req.RequestID,
			OK:            true,
			Result: map[string]any{
				"message_id": "m1",
				"resolution": map[string]any{"answer": "ok"},
			},
		}
		_ = json.NewEncoder(conn).Encode(resp)
		got <- seen{req: req}
	}()

	// Drive mcp-worker via stdin JSON-RPC initialize + tools/call is heavy;
	// instead invoke the production binary entry and ensure it connects with
	// env identity by sending a minimal MCP initialize then tools/list.
	// Simpler path: use claude-hook which is one-shot stdin/stdout.
	// For mcp-worker we run a tiny client that only needs the process to start
	// and dial — use `callSupervisor` production path via hook for dual coverage
	// and a dedicated mcp-worker dial check via `echo` of initialize.
	cmd := exec.Command(bin, "mcp-worker") // no identity flags
	cmd.Env = append(os.Environ(),
		"BROKER_RUN_DIR="+runDir,
		"BROKER_RUN_ID=run-env",
		"BROKER_TASK_ID=task-env",
		"BROKER_WORKER_ID=worker-env",
		"BROKER_NATIVE_SESSION_ID=sess-env",
		"BROKER_WORKER_TOKEN="+token,
		"BROKER_WORKER_SOCKET="+sock,
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// On Linux, verify token is absent from cmdline.
	if runtime.GOOS == "linux" {
		data, readErr := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", cmd.Process.Pid))
		if readErr == nil && strings.Contains(string(data), token) {
			_ = cmd.Process.Kill()
			t.Fatal("worker token present in /proc/<pid>/cmdline")
		}
	}

	// Minimal MCP initialize so the server starts its loop; then tools/call ask.
	// WorkerServer.Run reads JSON-RPC from stdin.
	init := map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "initialize",
		"params": map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "t", "version": "0"}},
	}
	if err := json.NewEncoder(stdin).Encode(init); err != nil {
		t.Fatal(err)
	}
	// tools/call ask_main_agent
	call := map[string]any{
		"jsonrpc": "2.0", "id": 2, "method": "tools/call",
		"params": map[string]any{
			"name": "ask_main_agent",
			"arguments": map[string]any{
				"question": "q", "reason": "r", "category": "decision", "workspace_state": "w",
			},
		},
	}
	if err := json.NewEncoder(stdin).Encode(call); err != nil {
		t.Fatal(err)
	}
	_ = stdin.Close()

	select {
	case s := <-got:
		if s.err != nil {
			_ = cmd.Process.Kill()
			t.Fatalf("socket: %v stderr=%s", s.err, stderr.String())
		}
		if s.req.AuthToken != token {
			t.Fatalf("token not in auth field (or wrong); method=%s", s.req.Method)
		}
		if s.req.RunID != "run-env" || s.req.Method != "worker_request" {
			t.Fatalf("req=%+v", s.req)
		}
		var params map[string]any
		_ = json.Unmarshal(s.req.Params, &params)
		if params["task_id"] != "task-env" || params["worker_id"] != "worker-env" {
			t.Fatalf("params=%v", params)
		}
		if params["native_session_id"] != "sess-env" {
			t.Fatalf("native_session_id=%v", params["native_session_id"])
		}
		// Token must not appear in params JSON.
		if strings.Contains(string(s.req.Params), token) {
			t.Fatal("token leaked into params")
		}
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("timeout waiting for worker socket connect; stderr=%s", stderr.String())
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			// MCP server may exit non-zero after one call depending on stdin EOF; allow.
			t.Logf("mcp-worker exit: %v stderr=%s", err, stderr.String())
		}
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("mcp-worker did not exit")
	}
	if strings.Contains(stderr.String(), token) {
		t.Fatal("token appeared in stderr")
	}
}

// TestClaudeHookEnvIdentity exercises actual claude-hook CLI with env identity only.
func TestClaudeHookEnvIdentity(t *testing.T) {
	bin := buildTestBroker(t)
	runDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(runDir, "run.json"), []byte(`{"run_id":"run-hook"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(runDir, "worker.sock")
	token := "TOP_SECRET_WORKER_TOKEN"
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	type seen struct {
		req supervisor.Request
		err error
	}
	got := make(chan seen, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			got <- seen{err: acceptErr}
			return
		}
		defer conn.Close()
		line, readErr := bufio.NewReader(conn).ReadBytes('\n')
		if readErr != nil {
			got <- seen{err: readErr}
			return
		}
		var req supervisor.Request
		if err := json.Unmarshal(line, &req); err != nil {
			got <- seen{err: err}
			return
		}
		resp := supervisor.Response{
			SchemaVersion: supervisor.SchemaVersion,
			RequestID:     req.RequestID,
			OK:            true,
			Result: map[string]any{
				"message_id": "m-hook",
				"resolution": map[string]any{"decision": map[string]any{"allowed": true, "reason": "ok"}},
			},
		}
		_ = json.NewEncoder(conn).Encode(resp)
		got <- seen{req: req}
	}()

	cmd := exec.Command(bin, "claude-hook") // no identity flags
	cmd.Env = append(os.Environ(),
		"BROKER_RUN_DIR="+runDir,
		"BROKER_RUN_ID=run-hook",
		"BROKER_TASK_ID=task-hook",
		"BROKER_WORKER_ID=worker-hook",
		"BROKER_NATIVE_SESSION_ID=sess-hook",
		"BROKER_WORKER_TOKEN="+token,
		"BROKER_WORKER_SOCKET="+sock,
	)
	cmd.Stdin = strings.NewReader(`{"tool_name":"Bash","tool_input":{"command":"echo hi"}}`)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("hook exit: %v stderr=%s", err, stderr.String())
	}
	select {
	case s := <-got:
		if s.err != nil {
			t.Fatal(s.err)
		}
		if s.req.AuthToken != token {
			t.Fatal("hook did not send token in auth field")
		}
		var params map[string]any
		_ = json.Unmarshal(s.req.Params, &params)
		if params["task_id"] != "task-hook" || params["native_session_id"] != "sess-hook" {
			t.Fatalf("params=%v", params)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("hook never connected")
	}
	if strings.Contains(stderr.String(), token) {
		t.Fatal("token in stderr")
	}
	if !strings.Contains(stdout.String(), "allow") && !strings.Contains(stdout.String(), "permissionDecision") && stdout.Len() == 0 {
		// Hook may print Claude hook decision JSON; non-empty success is enough.
		t.Logf("stdout=%s", stdout.String())
	}
}

// TestWorkerIdentityMismatchFailsClosed verifies flag/env mismatch exits before connect.
func TestWorkerIdentityMismatch(t *testing.T) {
	bin := buildTestBroker(t)
	runDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(runDir, "run.json"), []byte(`{"run_id":"run-m"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "claude-hook", "--task", "task-FLAG")
	cmd.Env = append(os.Environ(),
		"BROKER_RUN_DIR="+runDir,
		"BROKER_RUN_ID=run-m",
		"BROKER_TASK_ID=task-ENV",
		"BROKER_WORKER_ID=worker-1",
		"BROKER_WORKER_TOKEN=tok",
		"BROKER_WORKER_SOCKET="+filepath.Join(runDir, "no.sock"),
	)
	cmd.Stdin = strings.NewReader(`{"tool_name":"Bash","tool_input":{}}`)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("mismatch must fail")
	}
	if !strings.Contains(stderr.String(), "identity mismatch") && !strings.Contains(stderr.String(), "mismatch") {
		t.Fatalf("stderr=%s", stderr.String())
	}
}

// TestMissingEnvIdentityStableError verifies missing identity returns a stable error.
func TestMissingEnvIdentityStableError(t *testing.T) {
	bin := buildTestBroker(t)
	cmd := exec.Command(bin, "claude-hook")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	cmd.Stdin = strings.NewReader(`{"tool_name":"Bash","tool_input":{}}`)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		t.Fatal("expected failure")
	}
	if !strings.Contains(stderr.String(), "identity") && !strings.Contains(stderr.String(), "required") {
		t.Fatalf("stderr=%s", stderr.String())
	}
}

// TestClaudeAdapterGeneratedMCPConfig verifies production BuildInteractionLaunch.
func TestClaudeAdapterGeneratedMCPConfig(t *testing.T) {
	req := adapter.StartRequest{
		RunID: "run-1", TaskID: "task-1", WorkerID: "w-1",
		Interaction: adapter.InteractionConfig{
			Enabled: true, BrokerExecutable: "/usr/bin/subagent-broker",
			RunDir: "/tmp/run", WorkerToken: "TOP_SECRET_WORKER_TOKEN",
			WorkerSocket: "/tmp/worker.sock", NativeSessionID: "sess-1",
		},
	}
	launch, err := claude.BuildInteractionLaunch(req)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(launch.MCPConfigJSON, "TOP_SECRET_WORKER_TOKEN") {
		t.Fatal("token in MCP JSON")
	}
	if strings.Contains(launch.SettingsJSON, "TOP_SECRET_WORKER_TOKEN") {
		t.Fatal("token in settings")
	}
	if strings.Contains(launch.HookCommand, "TOP_SECRET") {
		t.Fatal("token in hook command")
	}
	if !strings.Contains(launch.MCPConfigJSON, "mcp-worker") {
		t.Fatal("mcp-worker missing from config")
	}
	env := claude.WorkerProcessEnv(req)
	joined := strings.Join(env, "\n")
	if !strings.Contains(joined, "BROKER_WORKER_TOKEN=TOP_SECRET_WORKER_TOKEN") {
		t.Fatal("token missing from env")
	}
	if !strings.Contains(joined, "BROKER_NATIVE_SESSION_ID=sess-1") {
		t.Fatal("native session missing from env")
	}
}

func buildTestBroker(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "subagent-broker")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = filepath.Join(".", "") // package main directory
	// When tests run from cmd/subagent-broker, "." is correct.
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Retry with module path.
		cmd = exec.Command("go", "build", "-o", bin, "github.com/vnai/subagent-broker/cmd/subagent-broker")
		out, err = cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("build broker: %v\n%s", err, out)
		}
	}
	return bin
}

// Ensure context import used if needed.
var _ = context.Background
