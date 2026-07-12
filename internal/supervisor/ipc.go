package supervisor

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/state"
)

type Request struct {
	SchemaVersion string          `json:"schema_version"`
	RequestID     string          `json:"request_id"`
	RunID         string          `json:"run_id"`
	Method        string          `json:"method"`
	Params        json.RawMessage `json:"params,omitempty"`
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

func (s *Service) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	decoder := bufio.NewScanner(conn)
	encoder := json.NewEncoder(conn)
	for decoder.Scan() {
		var request Request
		if err := json.Unmarshal(decoder.Bytes(), &request); err != nil {
			_ = encoder.Encode(Response{SchemaVersion: SchemaVersion, OK: false, Error: fmt.Sprintf("invalid request: %v", err)})
			continue
		}
		response := s.handleRequest(ctx, request)
		if err := encoder.Encode(response); err != nil {
			return
		}
	}
}

func (s *Service) handleRequest(ctx context.Context, request Request) Response {
	response := Response{SchemaVersion: SchemaVersion, RequestID: request.RequestID}
	if request.SchemaVersion != SchemaVersion {
		response.Error = "unsupported IPC schema version"
		return response
	}
	if request.RunID != string(s.Snapshot().Run.RunID) {
		response.Error = "request Run ID does not match Supervisor"
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
	case "worker_request":
		var params struct {
			TaskID   string           `json:"task_id"`
			WorkerID string           `json:"worker_id"`
			Type     message.Type     `json:"type"`
			Category message.Category `json:"category"`
			Payload  json.RawMessage  `json:"payload"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil {
			response.Error = err.Error()
			return response
		}
		var payload any
		if err := json.Unmarshal(params.Payload, &payload); err != nil {
			response.Error = err.Error()
			return response
		}
		resolution, id, err := s.RequestMessage(ctx, params.TaskID, params.WorkerID, params.Type, params.Category, payload)
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
