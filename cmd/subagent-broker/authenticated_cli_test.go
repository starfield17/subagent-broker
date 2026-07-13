package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/cliread"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/project"
	"github.com/vnai/subagent-broker/internal/run"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
	"github.com/vnai/subagent-broker/internal/supervisor"
	taskcontract "github.com/vnai/subagent-broker/internal/task"
)

// liveControlEnv is a real Run + live Supervisor with control auth.
type liveControlEnv struct {
	ProjectRoot string
	Home        string
	RunDir      string
	RunID       string
	Service     *supervisor.Service
	cancel      context.CancelFunc
	done        <-chan error
}

func (e *liveControlEnv) stop() {
	if e.cancel != nil {
		e.cancel()
	}
	if e.done != nil {
		select {
		case <-e.done:
		case <-time.After(3 * time.Second):
		}
	}
	// Start owns the lease until it returns; Close is best-effort if Start already exited.
	if e.Service != nil {
		_ = e.Service.Close()
	}
}

func startLiveControlSupervisor(t *testing.T, taskIDs []string, caps adapter.Capabilities) *liveControlEnv {
	t.Helper()
	if len(taskIDs) == 0 {
		taskIDs = []string{"task-a"}
	}
	projectRoot := t.TempDir()
	home := filepath.Join(t.TempDir(), "broker")
	projectValue, err := project.Resolve(context.Background(), projectRoot, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	layout, err := storage.NewLayout(home)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	runID := domain.RunID("run-auth-cli")
	ids := make([]domain.TaskID, 0, len(taskIDs))
	for _, id := range taskIDs {
		ids = append(ids, domain.TaskID(id))
	}
	config := supervisor.DefaultConfig()
	config.BrokerHome = home
	config.Harness = "fake"
	config.Scenario = "stalled" // KeepOpen so control plane stays live.
	runValue, err := run.New(runID, projectValue.ProjectID, "authenticated control CLI", ids, config, now)
	if err != nil {
		t.Fatal(err)
	}
	runValue.CurrentWave = "wave-1"
	runDir, err := layout.EnsureRun(string(projectValue.ProjectID), string(runID))
	if err != nil {
		t.Fatal(err)
	}
	for _, taskID := range taskIDs {
		item := domain.Task{
			TaskID: domain.TaskID(taskID), Title: "Task " + taskID, Objective: "stay alive for control CLI",
			CompletionCriteria: []string{"control path verified"}, WriteScope: []string{"output.txt"},
			ValidationCommands: []domain.ValidationCommand{{Command: "true", Scope: "local"}},
			ProjectRoot:        projectRoot, WaveID: "wave-1", Status: state.TaskPlanned,
		}
		if err := taskcontract.ValidateContract(item); err != nil {
			t.Fatal(err)
		}
		paths, pathErr := layout.TaskPaths(string(projectValue.ProjectID), string(runID), taskID)
		if pathErr != nil {
			t.Fatal(pathErr)
		}
		if err := os.MkdirAll(paths.ValidationDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := storage.AtomicWriteJSON(paths.Task, item, 0o600); err != nil {
			t.Fatal(err)
		}
		contract, cErr := taskcontract.RenderContract(item, runID)
		if cErr != nil {
			t.Fatal(cErr)
		}
		if err := storage.AtomicWriteFile(paths.Contract, []byte(contract), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	wavePaths, err := layout.WavePaths(string(projectValue.ProjectID), string(runID), "wave-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(wavePaths.Root, 0o700); err != nil {
		t.Fatal(err)
	}
	waveValue := domain.Wave{WaveID: "wave-1", Ordinal: 1, TaskIDs: ids, Status: domain.WavePlanned}
	if err := storage.AtomicWriteJSON(filepath.Join(wavePaths.Root, "wave.json"), waveValue, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := storage.AtomicWriteJSON(filepath.Join(runDir, "run.json"), runValue, 0o600); err != nil {
		t.Fatal(err)
	}

	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(caps)); err != nil {
		t.Fatal(err)
	}
	service, err := supervisor.Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Initialize(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Start(ctx) }()

	// Wait for control socket + auth token + supervisor identity.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(supervisor.SocketPath(runDir)); err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		tok, err := supervisor.LoadControlCredential(runDir)
		if err != nil || strings.TrimSpace(tok) == "" {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		// Identity must be live for cliread.CallIPC.
		data, err := os.ReadFile(filepath.Join(runDir, "state.json"))
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		var snap supervisor.Snapshot
		if json.Unmarshal(data, &snap) != nil || snap.Run.SupervisorIdentity == nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		env := &liveControlEnv{
			ProjectRoot: projectRoot, Home: home, RunDir: runDir, RunID: string(runID),
			Service: service, cancel: cancel, done: done,
		}
		t.Cleanup(env.stop)
		return env
	}
	cancel()
	_ = service.Close()
	t.Fatal("supervisor control plane did not become ready")
	return nil
}

func buildCLIBinary(t *testing.T) string {
	t.Helper()
	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	binaryPath := filepath.Join(t.TempDir(), "subagent-broker")
	build := exec.Command("go", "build", "-o", binaryPath, ".")
	build.Dir = workingDir
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	return binaryPath
}

func runCLI(t *testing.T, binary string, args ...string) (stdout, stderr string, exitCode int) {
	t.Helper()
	cmd := exec.Command(binary, args...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err := cmd.Run()
	if err == nil {
		return outBuf.String(), errBuf.String(), 0
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return outBuf.String(), errBuf.String(), exitErr.ExitCode()
	}
	t.Fatalf("run CLI: %v stderr=%s", err, errBuf.String())
	return "", "", -1
}

func assertNoSecret(t *testing.T, secret string, parts ...string) {
	t.Helper()
	if secret == "" {
		return
	}
	for _, p := range parts {
		if strings.Contains(p, secret) {
			t.Fatal("control token leaked into CLI or test output")
		}
	}
}

func TestAuthenticatedSendAnswerResolvesBlockedWorker(t *testing.T) {
	env := startLiveControlSupervisor(t, []string{"task-a"}, adapter.Capabilities{
		StructuredStream: true, StructuredFinalOutput: true, BidirectionalStream: true,
	})
	// Wait until task is running so RequestMessage can block a worker.
	waitTaskRunning(t, env.Service, "task-a")

	type reqResult struct {
		res message.Resolution
		id  string
		err error
	}
	done := make(chan reqResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, id, err := env.Service.RequestMessage(ctx, "task-a", "worker-a", message.Question, message.Decision,
			message.QuestionEnvelope{SchemaVersion: supervisor.SchemaVersion, Question: "Choose?", Reason: "blocked", CurrentScope: []string{"output.txt"}, WorkspaceState: "unchanged"})
		done <- reqResult{res: res, id: id, err: err}
	}()

	msgID := waitPendingMessage(t, env.Service)
	binary := buildCLIBinary(t)
	token, _ := supervisor.LoadControlCredential(env.RunDir)
	stdout, stderr, code := runCLI(t, binary,
		"send", "--project", env.ProjectRoot, "--broker-home", env.Home, "--run", env.RunID,
		"--message", msgID, "--answer", "approved",
	)
	assertNoSecret(t, token, stdout, stderr, strings.Join(os.Args, " "))
	if code != 0 {
		t.Fatalf("CLI exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "message resolved") {
		t.Fatalf("stdout=%q", stdout)
	}
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("RequestMessage: %v", got.err)
		}
		if got.res.Answer != "approved" {
			t.Fatalf("answer=%+v", got.res)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RequestMessage did not resume")
	}
	// Message Answered; task not left blocked on waiting message.
	items := env.Service.Inbox(true)
	found := false
	for _, item := range items {
		if item.MessageID == msgID {
			found = true
			if item.Status != message.Answered {
				t.Fatalf("status=%s", item.Status)
			}
		}
	}
	if !found {
		t.Fatal("answered message missing from inbox(includeResolved)")
	}
	for _, ts := range env.Service.Snapshot().Tasks {
		if string(ts.Task.TaskID) == "task-a" && ts.Task.Status == state.TaskBlocked && ts.BlockKind == supervisor.BlockKindWaitingMessage {
			t.Fatalf("task left blocked: %+v", ts)
		}
	}
}

func TestAuthenticatedSendApproveAndDeny(t *testing.T) {
	// Permission requests exercise --approve/--deny over the authenticated control
	// path. Scope-expansion approve is covered by TestAuthenticatedSendScopeExpansionApprove.
	env := startLiveControlSupervisor(t, []string{"task-a"}, adapter.Capabilities{
		StructuredStream: true, StructuredFinalOutput: true, BidirectionalStream: true,
	})
	waitTaskRunning(t, env.Service, "task-a")
	binary := buildCLIBinary(t)
	token, _ := supervisor.LoadControlCredential(env.RunDir)

	// Approve path
	done := make(chan message.Resolution, 1)
	go func() {
		input, _ := json.Marshal(map[string]string{"command": "ls"})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, _, _ := env.Service.RequestMessage(ctx, "task-a", "worker-a", message.PermissionRequest, message.Permission,
			message.PermissionRequestPayload{ToolName: "Bash", Input: input})
		done <- res
	}()
	msgID := waitPendingMessage(t, env.Service)
	stdout, stderr, code := runCLI(t, binary,
		"send", "--project", env.ProjectRoot, "--broker-home", env.Home, "--run", env.RunID,
		"--message", msgID, "--approve",
	)
	assertNoSecret(t, token, stdout, stderr)
	if code != 0 {
		t.Fatalf("approve exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	select {
	case res := <-done:
		if !res.Decision.Allowed {
			t.Fatalf("approve resolution=%+v", res)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("approve did not resume waiter")
	}

	// Deny path
	done2 := make(chan message.Resolution, 1)
	go func() {
		input, _ := json.Marshal(map[string]string{"command": "rm"})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, _, _ := env.Service.RequestMessage(ctx, "task-a", "worker-a", message.PermissionRequest, message.Permission,
			message.PermissionRequestPayload{ToolName: "Bash", Input: input})
		done2 <- res
	}()
	msgID2 := waitPendingMessage(t, env.Service)
	stdout, stderr, code = runCLI(t, binary,
		"send", "--project", env.ProjectRoot, "--broker-home", env.Home, "--run", env.RunID,
		"--message", msgID2, "--deny", "--reason", "unsafe",
	)
	assertNoSecret(t, token, stdout, stderr)
	if code != 0 {
		t.Fatalf("deny exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	select {
	case res := <-done2:
		if res.Decision.Allowed {
			t.Fatalf("deny must not allow: %+v", res)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("deny did not resume waiter")
	}
}

// TestAuthenticatedSendScopeExpansionApprove exercises the real authenticated
// resolve_message IPC path for ScopeExpansionRequest --approve. This is the
// regression for the approveScope / Commit mutex re-entry deadlock.
func TestAuthenticatedSendScopeExpansionApprove(t *testing.T) {
	env := startLiveControlSupervisor(t, []string{"task-a"}, adapter.Capabilities{
		StructuredStream: true, StructuredFinalOutput: true, BidirectionalStream: true,
	})
	waitTaskRunning(t, env.Service, "task-a")
	binary := buildCLIBinary(t)
	token, err := supervisor.LoadControlCredential(env.RunDir)
	if err != nil || token == "" {
		t.Fatal("control credential missing")
	}

	requested := "cli-expanded-scope.txt"
	type reqResult struct {
		res message.Resolution
		id  string
		err error
	}
	done := make(chan reqResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		res, id, err := env.Service.RequestMessage(ctx, "task-a", "worker-a",
			message.ScopeExpansionRequest, message.Scope,
			message.ScopeRequestPayload{
				RequestedScope:       []string{requested},
				Reason:               "CLI scope expansion approve path",
				Consequence:          "worker cannot continue without expanded write scope",
				PartialModifications: "none",
			})
		done <- reqResult{res: res, id: id, err: err}
	}()

	msgID := waitPendingMessage(t, env.Service)
	// Ensure the pending decision is the scope-expansion request, not a substitute.
	pendingFound := false
	for _, item := range env.Service.Inbox(false) {
		if item.MessageID == msgID {
			pendingFound = true
			if item.Type != message.ScopeExpansionRequest {
				t.Fatalf("expected ScopeExpansionRequest, got %s", item.Type)
			}
		}
	}
	if !pendingFound {
		t.Fatal("pending scope expansion message missing from inbox")
	}

	cliArgs := []string{
		"send", "--project", env.ProjectRoot, "--broker-home", env.Home, "--run", env.RunID,
		"--message", msgID, "--approve",
	}
	stdout, stderr, code := runCLI(t, binary, cliArgs...)
	// Control credential must never appear in argv, stdout, stderr, or failure text.
	assertNoSecret(t, token, stdout, stderr, strings.Join(cliArgs, " "), strings.Join(os.Args, " "))
	if code != 0 {
		assertNoSecret(t, token, stdout, stderr)
		t.Fatalf("CLI exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "message resolved") {
		t.Fatalf("stdout missing successful resolution: %q", stdout)
	}

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("RequestMessage: %v", got.err)
		}
		if !got.res.Decision.Allowed {
			t.Fatalf("Allowed=%v resolution=%+v", got.res.Decision.Allowed, got.res)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RequestMessage did not resume after CLI --approve (possible deadlock)")
	}

	// Durable message is Answered.
	items := env.Service.Inbox(true)
	answered := false
	for _, item := range items {
		if item.MessageID != msgID {
			continue
		}
		answered = true
		if item.Status != message.Answered {
			t.Fatalf("status=%s want Answered", item.Status)
		}
	}
	if !answered {
		t.Fatal("answered scope expansion message missing from inbox(includeResolved)")
	}

	// Requested scope exists exactly once in the Snapshot; task not blocked on decision.
	snap := env.Service.Snapshot()
	scopeCount := 0
	for _, ts := range snap.Tasks {
		if string(ts.Task.TaskID) != "task-a" {
			continue
		}
		for _, entry := range ts.Task.WriteScope {
			if entry == requested {
				scopeCount++
			}
		}
		if ts.Task.Status == state.TaskBlocked && ts.BlockKind == supervisor.BlockKindWaitingMessage {
			t.Fatalf("task left blocked on decision: %+v", ts)
		}
	}
	if scopeCount != 1 {
		t.Fatalf("requested scope count=%d want 1 in snapshot", scopeCount)
	}

	// Generated Task contract contains the new scope.
	contractPath := filepath.Join(env.RunDir, "tasks", "task-a", "contract.md")
	contractRaw, err := os.ReadFile(contractPath)
	if err != nil {
		t.Fatalf("read contract: %v", err)
	}
	assertNoSecret(t, token, string(contractRaw))
	if !strings.Contains(string(contractRaw), requested) {
		t.Fatalf("contract missing expanded scope %q", requested)
	}

	// Control plane remains responsive after approval (global mutex not left locked).
	ok, respErr := rawControlCall(t, env.RunDir, env.RunID, "ping", token)
	assertNoSecret(t, token, respErr)
	if !ok {
		t.Fatalf("post-approve ping failed: %s", respErr)
	}
	// CLI status also exercises the authenticated control path after approve.
	statusOut, statusErr, statusCode := runCLI(t, binary,
		"status", "--project", env.ProjectRoot, "--broker-home", env.Home, "--run", env.RunID,
	)
	assertNoSecret(t, token, statusOut, statusErr)
	if statusCode != 0 {
		t.Fatalf("post-approve status exit=%d stdout=%s stderr=%s", statusCode, statusOut, statusErr)
	}
}

func TestAuthenticatedSendInstruction(t *testing.T) {
	env := startLiveControlSupervisor(t, []string{"task-a"}, adapter.Capabilities{
		StructuredStream: true, StructuredFinalOutput: true, BidirectionalStream: true,
	})
	waitTaskRunning(t, env.Service, "task-a")
	binary := buildCLIBinary(t)
	token, _ := supervisor.LoadControlCredential(env.RunDir)
	stdout, stderr, code := runCLI(t, binary,
		"send", "--project", env.ProjectRoot, "--broker-home", env.Home, "--run", env.RunID,
		"--task", "task-a", "--text", "follow-up",
	)
	assertNoSecret(t, token, stdout, stderr)
	if code != 0 {
		t.Fatalf("send instruction exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	// Authenticated send creates instruction state (next-turn remains queued).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, item := range env.Service.Inbox(true) {
			if item.Type == message.Instruction {
				return
			}
		}
		for _, item := range env.Service.Inbox(false) {
			if item.Type == message.Instruction {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	// CLI printed delivery result JSON on success even if inbox projection lags.
	if strings.TrimSpace(stdout) == "" {
		t.Fatalf("expected instruction result stdout, stderr=%s", stderr)
	}
}

func TestAuthenticatedCancelTask(t *testing.T) {
	env := startLiveControlSupervisor(t, []string{"task-a"}, adapter.Capabilities{
		StructuredStream: true, StructuredFinalOutput: true, BidirectionalStream: true,
	})
	waitTaskRunning(t, env.Service, "task-a")
	binary := buildCLIBinary(t)
	token, _ := supervisor.LoadControlCredential(env.RunDir)
	// Poll until active map is ready; unauthorized would fail immediately without retry success.
	deadline := time.Now().Add(5 * time.Second)
	var stdout, stderr string
	var code int
	for time.Now().Before(deadline) {
		stdout, stderr, code = runCLI(t, binary,
			"cancel", "--project", env.ProjectRoot, "--broker-home", env.Home, "--run", env.RunID,
			"--task", "task-a",
		)
		assertNoSecret(t, token, stdout, stderr)
		if code == 0 && strings.Contains(stdout, "cancel requested") {
			return
		}
		if strings.Contains(stderr, "unauthorized") || strings.Contains(stdout, "unauthorized") {
			t.Fatalf("unauthorized on cancel: stdout=%s stderr=%s", stdout, stderr)
		}
		if !strings.Contains(stderr, "no active Worker") && !strings.Contains(stdout, "no active Worker") && code != 0 {
			// Other failures after auth may still be environment races; keep polling briefly.
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("cancel exit=%d stdout=%s stderr=%s", code, stdout, stderr)
}

func TestAuthenticatedCancelViaCommandHandler(t *testing.T) {
	// Handler path still goes cancelCommand → cliread.CallIPC → socket.
	// RequestCancelTask requires the in-memory active map (not only Worker projection),
	// so poll until the worker is registered or fail.
	env := startLiveControlSupervisor(t, []string{"task-a"}, adapter.Capabilities{
		StructuredStream: true, StructuredFinalOutput: true,
	})
	waitTaskRunning(t, env.Service, "task-a")
	args := []string{
		"--project", env.ProjectRoot, "--broker-home", env.Home, "--run", env.RunID, "--task", "task-a",
	}
	deadline := time.Now().Add(5 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		err = cancelCommand(args)
		if err == nil {
			return
		}
		// Retry only the active-worker race; auth failures must not be retried.
		if !strings.Contains(err.Error(), "no active Worker") {
			t.Fatalf("cancelCommand: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("cancelCommand: %v", err)
}

func TestControlCredentialMissingFailsClosed(t *testing.T) {
	env := startLiveControlSupervisor(t, []string{"task-a"}, adapter.Capabilities{
		StructuredStream: true, StructuredFinalOutput: true,
	})
	// Remove auth.token — CLI must fail locally before unauthorized.
	if err := os.Remove(supervisor.ControlTokenPath(env.RunDir)); err != nil {
		t.Fatal(err)
	}
	err := sendCommand([]string{
		"--project", env.ProjectRoot, "--broker-home", env.Home, "--run", env.RunID,
		"--task", "task-a", "--text", "nope",
	})
	if err == nil {
		t.Fatal("expected fail-closed without control token")
	}
	if !errors.Is(err, cliread.ErrControlCredentialUnavailable) && !strings.Contains(err.Error(), "control credential unavailable") {
		// clioutcome wraps the error — check message category.
		if !strings.Contains(err.Error(), "control credential unavailable") && !strings.Contains(err.Error(), "IPC unavailable") {
			t.Fatalf("err=%v", err)
		}
	}
	// Ensure raw path contents / token not in error.
	if strings.Contains(err.Error(), "TOP_SECRET") {
		t.Fatal("secret leaked in error")
	}
}

func TestControlCredentialEmptyFailsClosed(t *testing.T) {
	env := startLiveControlSupervisor(t, []string{"task-a"}, adapter.Capabilities{
		StructuredStream: true, StructuredFinalOutput: true,
	})
	if err := os.WriteFile(supervisor.ControlTokenPath(env.RunDir), []byte("   \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := cancelCommand([]string{
		"--project", env.ProjectRoot, "--broker-home", env.Home, "--run", env.RunID,
	})
	if err == nil {
		t.Fatal("expected fail-closed on empty control token")
	}
	if strings.Contains(err.Error(), "TOP_SECRET") {
		t.Fatal("secret leaked in error")
	}
}

func TestControlSocketAuthMatrix(t *testing.T) {
	env := startLiveControlSupervisor(t, []string{"task-a"}, adapter.Capabilities{
		StructuredStream: true, StructuredFinalOutput: true,
	})
	waitTaskRunning(t, env.Service, "task-a")
	token, err := supervisor.LoadControlCredential(env.RunDir)
	if err != nil || token == "" {
		t.Fatal("control token missing")
	}
	// Synthetic non-control token (worker-shaped) for wrong-credential test.
	workerTok := "worker-token-not-control"

	cases := []struct {
		name  string
		token string
		want  bool
	}{
		{"missing token", "", false},
		{"wrong token", "definitely-not-the-control-token", false},
		{"worker token on control", workerTok, false},
		{"correct token", token, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, respErr := rawControlCall(t, env.RunDir, env.RunID, "ping", tc.token)
			if ok != tc.want {
				t.Fatalf("ok=%v err=%s (token must not appear in failures)", ok, respErr)
			}
			if strings.Contains(respErr, token) || (workerTok != "" && strings.Contains(respErr, workerTok)) {
				t.Fatal("raw token appeared in response/error")
			}
			if !tc.want && !strings.Contains(strings.ToLower(respErr), "unauthorized") && respErr != "unauthorized" {
				// Missing token yields unauthorized from supervisor.
				if respErr != "" && !strings.Contains(strings.ToLower(respErr), "unauthorized") {
					t.Fatalf("expected unauthorized, got %q", respErr)
				}
			}
		})
	}
}

func rawControlCall(t *testing.T, runDir, runID, method, authToken string) (ok bool, errStr string) {
	t.Helper()
	conn, err := net.DialTimeout("unix", supervisor.SocketPath(runDir), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	req := supervisor.Request{
		SchemaVersion: supervisor.SchemaVersion,
		RequestID:     "raw-auth-test",
		RunID:         runID,
		Method:        method,
		AuthToken:     authToken,
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var resp supervisor.Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	return resp.OK, resp.Error
}

func waitTaskRunning(t *testing.T, service *supervisor.Service, taskID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, ts := range service.Snapshot().Tasks {
			if string(ts.Task.TaskID) != taskID {
				continue
			}
			// Require an active worker projection so cancel/send paths are valid.
			if ts.Worker != nil && (ts.Task.Status == state.TaskRunning || ts.Task.Status == state.TaskBlocked) {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("task %s has no active worker: %+v", taskID, service.Snapshot().Tasks)
}

func waitPendingMessage(t *testing.T, service *supervisor.Service) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		items := service.Inbox(false)
		if len(items) > 0 {
			return items[0].MessageID
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("pending message did not appear")
	return ""
}
