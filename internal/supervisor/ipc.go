package supervisor

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/state"
)

// domain is used for TaskID cast in credential staleness checks.

type Request struct {
	SchemaVersion string `json:"schema_version"`
	RequestID     string `json:"request_id"`
	RunID         string `json:"run_id"`
	Method        string `json:"method"`
	// AuthToken is the control or worker credential. Never log or echo this field.
	AuthToken string          `json:"auth_token,omitempty"`
	Params    json.RawMessage `json:"params,omitempty"`
}

type Response struct {
	SchemaVersion string `json:"schema_version"`
	RequestID     string `json:"request_id"`
	OK            bool   `json:"ok"`
	Error         string `json:"error,omitempty"`
	Result        any    `json:"result,omitempty"`
}

// WaitParams is the IPC long-poll contract used by the CLI. The Supervisor
// evaluates the target against its live Snapshot; the client does not poll the
// disk while IPC is healthy.
type WaitParams struct {
	Timeout         time.Duration `json:"timeout"`
	For             string        `json:"for,omitempty"`
	TaskID          string        `json:"task_id,omitempty"`
	WaveID          string        `json:"wave_id,omitempty"`
	SinceSeq        uint64        `json:"since_seq,omitempty"`
	ReturnOnBlocked bool          `json:"return_on_blocked,omitempty"`
}

type EventsResult struct {
	Events         []event.Event `json:"events"`
	IncompleteTail bool          `json:"incomplete_tail,omitempty"`
	TailRepaired   bool          `json:"tail_repaired,omitempty"`
	QuarantinePath string        `json:"quarantine_path,omitempty"`
	SnapshotTime   time.Time     `json:"snapshot_time,omitempty"`
}

func (s *Service) handleConnection(ctx context.Context, conn net.Conn, plane CallerRole) {
	defer conn.Close()
	decoder := bufio.NewScanner(conn)
	// Allow large worker payloads without unbounded growth from secrets in errors.
	buf := make([]byte, 0, 64*1024)
	decoder.Buffer(buf, 1024*1024)
	encoder := json.NewEncoder(conn)
	for decoder.Scan() {
		var request Request
		if err := json.Unmarshal(decoder.Bytes(), &request); err != nil {
			_ = encoder.Encode(Response{SchemaVersion: SchemaVersion, OK: false, Error: "invalid request"})
			continue
		}
		response := s.handleRequest(ctx, request, plane)
		// Never echo auth material.
		if err := encoder.Encode(response); err != nil {
			return
		}
	}
}

func (s *Service) handleRequest(ctx context.Context, request Request, plane CallerRole) Response {
	response := Response{SchemaVersion: SchemaVersion, RequestID: request.RequestID}
	if request.SchemaVersion != SchemaVersion {
		response.Error = "unsupported IPC schema version"
		return response
	}
	if request.RunID != string(s.Snapshot().Run.RunID) {
		response.Error = "request Run ID does not match Supervisor"
		return response
	}

	// Authenticate and authorize by plane + method. Fail closed without leaking secrets.
	role, workerBinding, authErr := s.authenticateRequest(request, plane)
	if authErr != nil {
		response.Error = "unauthorized"
		return response
	}
	if err := Authorize(role, request.Method); err != nil {
		response.Error = "unauthorized"
		return response
	}
	// Worker plane hard-limit: only worker_request (Authorize already checks).
	if plane == CallerWorker && request.Method != "worker_request" {
		response.Error = "unauthorized"
		return response
	}
	if plane == CallerControl && request.Method == "worker_request" {
		// Control socket must not accept worker_request (use worker socket).
		response.Error = "unauthorized"
		return response
	}

	switch request.Method {
	case "ping", "status":
		response.OK = true
		response.Result = s.Snapshot()
	case "events":
		var params struct {
			SinceSeq uint64 `json:"since_seq"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil && len(request.Params) > 0 {
			response.Error = err.Error()
			return response
		}
		replay, err := event.Replay(s.paths.Events)
		if err != nil {
			response.Error = err.Error()
			return response
		}
		result := EventsResult{Events: make([]event.Event, 0), IncompleteTail: replay.IncompleteTail, TailRepaired: replay.TailRepaired, QuarantinePath: replay.QuarantinePath}
		for _, item := range replay.Events {
			if item.Seq > params.SinceSeq {
				result.Events = append(result.Events, item)
			}
			if item.Timestamp.After(result.SnapshotTime) {
				result.SnapshotTime = item.Timestamp
			}
		}
		response.OK = true
		response.Result = result
	case "cancel":
		var params struct {
			TaskID   string `json:"task_id"`
			WorkerID string `json:"worker_id"`
			WaveID   string `json:"wave_id"`
		}
		_ = json.Unmarshal(request.Params, &params)
		var err error
		if params.TaskID != "" {
			err = s.RequestCancelTask(ctx, params.TaskID)
		} else if params.WorkerID != "" {
			taskID := ""
			for _, runtime := range s.Snapshot().Tasks {
				if runtime.Worker != nil && string(runtime.Worker.WorkerID) == params.WorkerID {
					taskID = string(runtime.Task.TaskID)
					break
				}
			}
			if taskID == "" {
				err = fmt.Errorf("worker %q was not found", params.WorkerID)
			} else {
				err = s.RequestCancelTask(ctx, taskID)
			}
		} else if params.WaveID != "" {
			found := false
			for _, runtime := range s.Snapshot().Tasks {
				if string(runtime.Task.WaveID) == params.WaveID {
					found = true
					if runtime.Worker != nil && runtime.Worker.ExitCode == nil {
						if cancelErr := s.RequestCancelTask(ctx, string(runtime.Task.TaskID)); cancelErr != nil {
							err = cancelErr
						}
					}
				}
			}
			if !found {
				err = fmt.Errorf("Wave %q was not found", params.WaveID)
			}
		} else {
			err = s.RequestCancel(ctx)
		}
		if err != nil {
			response.Error = err.Error()
			return response
		}
		response.OK = true
		response.Result = s.Snapshot()
	case "inbox":
		var params struct {
			IncludeResolved bool `json:"include_resolved"`
		}
		_ = json.Unmarshal(request.Params, &params)
		response.OK = true
		response.Result = s.Inbox(params.IncludeResolved)
	case "send":
		var params struct {
			TaskID string `json:"task_id"`
			Text   string `json:"text"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil {
			response.Error = err.Error()
			return response
		}
		result, err := s.SendInstruction(ctx, params.TaskID, params.Text)
		if err != nil {
			response.Error = err.Error()
			response.Result = result
			return response
		}
		response.OK = true
		response.Result = result
	case "resolve_message":
		var params struct {
			MessageID  string             `json:"message_id"`
			Resolution message.Resolution `json:"resolution"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil {
			response.Error = err.Error()
			return response
		}
		if err := s.ResolveMessage(params.MessageID, params.Resolution); err != nil {
			response.Error = err.Error()
			return response
		}
		response.OK = true
		response.Result = s.Snapshot()
	case "barrier.accept", "barrier_accept", "accept_barrier_warnings":
		var params struct {
			WaveID string `json:"wave_id"`
			Actor  string `json:"actor"`
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil {
			response.Error = err.Error()
			return response
		}
		if err := s.AcceptBarrierWarnings(params.WaveID, params.Actor, params.Reason); err != nil {
			response.Error = err.Error()
			return response
		}
		response.OK = true
		response.Result = s.Snapshot()
	case "barrier.reject", "barrier_reject", "reject_barrier_warnings":
		var params struct {
			WaveID string `json:"wave_id"`
			Actor  string `json:"actor"`
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil {
			response.Error = err.Error()
			return response
		}
		if err := s.RejectBarrierWarnings(params.WaveID, params.Actor, params.Reason); err != nil {
			response.Error = err.Error()
			return response
		}
		response.OK = true
		response.Result = s.Snapshot()
	case "final.accept", "final_accept", "accept_final_warnings":
		var params struct {
			Actor  string `json:"actor"`
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil {
			response.Error = err.Error()
			return response
		}
		if err := s.AcceptFinalWarnings(params.Actor, params.Reason); err != nil {
			response.Error = err.Error()
			return response
		}
		response.OK = true
		response.Result = s.Snapshot()
	case "final.reject", "final_reject", "reject_final_warnings":
		var params struct {
			Actor  string `json:"actor"`
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil {
			response.Error = err.Error()
			return response
		}
		if err := s.RejectFinalWarnings(params.Actor, params.Reason); err != nil {
			response.Error = err.Error()
			return response
		}
		response.OK = true
		response.Result = s.Snapshot()
	case "worker_request":
		if workerBinding == nil {
			response.Error = "unauthorized"
			return response
		}
		var params struct {
			TaskID          string           `json:"task_id"`
			WorkerID        string           `json:"worker_id"`
			NativeSessionID string           `json:"native_session_id"`
			Type            message.Type     `json:"type"`
			Category        message.Category `json:"category"`
			Payload         json.RawMessage  `json:"payload"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil {
			response.Error = err.Error()
			return response
		}
		// Authoritative identity comes from the credential, not caller-supplied fields.
		if params.TaskID != "" && params.TaskID != workerBinding.TaskID {
			response.Error = "unauthorized"
			return response
		}
		if params.WorkerID != "" && params.WorkerID != workerBinding.WorkerID {
			response.Error = "unauthorized"
			return response
		}
		// request native session ID == credential NativeSessionID == active session (when registered).
		// Do not reveal expected session IDs in errors.
		reqNative := strings.TrimSpace(params.NativeSessionID)
		if reqNative == "" || reqNative != workerBinding.NativeSessionID {
			response.Error = "unauthorized"
			return response
		}
		if s.workerNativeSessionMismatch(workerBinding, reqNative) {
			response.Error = "unauthorized"
			return response
		}
		var payload any
		if err := json.Unmarshal(params.Payload, &payload); err != nil {
			response.Error = err.Error()
			return response
		}
		resolution, id, err := s.RequestMessage(ctx, workerBinding.TaskID, workerBinding.WorkerID, params.Type, params.Category, payload)
		if err != nil {
			response.Error = err.Error()
			response.Result = map[string]any{"message_id": id}
			return response
		}
		response.OK = true
		response.Result = map[string]any{"message_id": id, "resolution": resolution}
	case "wait":
		var params WaitParams
		if err := json.Unmarshal(request.Params, &params); err != nil && len(request.Params) > 0 {
			response.Error = err.Error()
			return response
		}
		waitCtx := ctx
		cancel := func() {}
		if params.Timeout > 0 {
			waitCtx, cancel = context.WithTimeout(ctx, params.Timeout)
		}
		snapshot, matched, err := s.waitFor(waitCtx, params)
		cancel()
		response.Result = snapshot
		if err != nil {
			response.Error = err.Error()
			return response
		}
		if matched {
			response.OK = true
		}
	default:
		response.Error = fmt.Sprintf("unsupported IPC method %q", request.Method)
	}
	return response
}

func (s *Service) waitFor(ctx context.Context, params WaitParams) (Snapshot, bool, error) {
	if params.For == "" {
		params.For = "run"
	}
	check := func() (Snapshot, bool, error) {
		snapshot := s.Snapshot()
		switch params.For {
		case "run":
			return snapshot, runTerminal(snapshot.Run.Status), nil
		case "wave":
			waveID := params.WaveID
			if waveID == "" {
				waveID = string(snapshot.Run.CurrentWave)
			}
			for _, value := range snapshot.Waves {
				if string(value.WaveID) != waveID {
					continue
				}
				matched := value.Status == domain.WaveVerified || value.Status == domain.WaveWaiting || value.Status == domain.WaveBlocked || value.Status == domain.WaveFailed || value.Status == domain.WaveCancelled
				return snapshot, matched, nil
			}
			return snapshot, false, fmt.Errorf("Wave %q was not found", waveID)
		case "task":
			if params.TaskID == "" {
				return snapshot, false, fmt.Errorf("--for task requires --task")
			}
			for _, runtime := range snapshot.Tasks {
				if string(runtime.Task.TaskID) != params.TaskID {
					continue
				}
				matched := runtime.Task.Status == state.TaskVerifiedSuccess || runtime.Task.Status == state.TaskVerifiedPartial || runtime.Task.Status == state.TaskVerificationFailed || runtime.Task.Status == state.TaskFailed || runtime.Task.Status == state.TaskCancelled || (runtime.Task.Status == state.TaskBlocked && (params.ReturnOnBlocked || runtime.BlockKind == BlockKindFinal))
				return snapshot, matched, nil
			}
			return snapshot, false, fmt.Errorf("task %q was not found", params.TaskID)
		case "event":
			replay, err := event.Replay(s.paths.Events)
			if err != nil {
				return snapshot, false, err
			}
			for _, item := range replay.Events {
				if item.Seq > params.SinceSeq {
					return snapshot, true, nil
				}
			}
			return snapshot, false, nil
		case "inbox":
			return snapshot, len(s.Inbox(false)) > 0, nil
		default:
			return snapshot, false, fmt.Errorf("unsupported wait condition %q", params.For)
		}
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, matched, err := check()
		if err != nil || matched {
			return snapshot, matched, err
		}
		select {
		case <-ctx.Done():
			return s.Snapshot(), false, ctx.Err()
		case <-s.terminal:
			final, matched, err := check()
			if err != nil || matched {
				return final, matched, err
			}
			return final, false, nil
		case <-ticker.C:
		}
	}
}

func runTerminal(status domain.RunStatus) bool {
	switch status {
	case domain.RunCompleted, domain.RunFailed, domain.RunCancelled, domain.RunDegraded:
		return true
	default:
		return false
	}
}

func readIPCResponse(conn net.Conn, response *Response) error {
	decoder := bufio.NewReader(conn)
	line, err := decoder.ReadBytes('\n')
	if err != nil {
		if err == io.EOF && len(line) == 0 {
			return io.EOF
		}
	}
	return json.Unmarshal(line, response)
}

// authenticateRequest maps plane + token to a role. Errors are opaque ("unauthorized").
func (s *Service) authenticateRequest(request Request, plane CallerRole) (CallerRole, *WorkerCredentialBinding, error) {
	if s.auth == nil {
		// Tests without auth: allow control plane only for backward-compatible unit tests.
		if plane == CallerControl {
			return CallerControl, nil, nil
		}
		return CallerNone, nil, fmt.Errorf("unauthorized")
	}
	token := request.AuthToken
	switch plane {
	case CallerControl:
		if !s.auth.AuthenticateControl(token) {
			return CallerNone, nil, fmt.Errorf("unauthorized")
		}
		return CallerControl, nil, nil
	case CallerWorker:
		binding, ok := s.auth.AuthenticateWorker(token)
		if !ok {
			return CallerNone, nil, fmt.Errorf("unauthorized")
		}
		if binding.RunID != "" && binding.RunID != request.RunID {
			return CallerNone, nil, fmt.Errorf("unauthorized")
		}
		// Reject stale attempt when a newer active attempt exists for the same task.
		if s.workerCredentialStale(binding) {
			return CallerNone, nil, fmt.Errorf("unauthorized")
		}
		return CallerWorker, binding, nil
	default:
		return CallerNone, nil, fmt.Errorf("unauthorized")
	}
}

func (s *Service) workerCredentialStale(b *WorkerCredentialBinding) bool {
	if b == nil {
		return true
	}
	s.mu.Lock()
	active, ok := s.active[b.TaskID]
	s.mu.Unlock()
	if !ok {
		// No active worker: credential may still be valid during blocked wait.
		// Reject only when a different active attempt is registered.
		if runtime, found := s.taskState(domain.TaskID(b.TaskID)); found && runtime.ActiveAttempt > 0 {
			return runtime.ActiveAttempt != b.AttemptNumber
		}
		return false
	}
	if active.workerID != b.WorkerID {
		return true
	}
	if active.attempt != b.AttemptNumber {
		return true
	}
	if b.NativeSessionID != "" && active.sessionID != "" && b.NativeSessionID != active.sessionID {
		return true
	}
	return false
}

// workerNativeSessionMismatch reports whether the request native session conflicts
// with the active Worker session when one is registered.
func (s *Service) workerNativeSessionMismatch(b *WorkerCredentialBinding, requestNative string) bool {
	if b == nil {
		return true
	}
	s.mu.Lock()
	active, ok := s.active[b.TaskID]
	s.mu.Unlock()
	if !ok {
		return false
	}
	if active.sessionID != "" && active.sessionID != requestNative {
		return true
	}
	return false
}
