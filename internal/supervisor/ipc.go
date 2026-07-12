package supervisor

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/vnai/subagent-broker/internal/message"
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
	if request.RunID != string(s.snapshot.Run.RunID) {
		response.Error = "request Run ID does not match Supervisor"
		return response
	}
	switch request.Method {
	case "ping", "status":
		response.OK = true
		response.Result = s.Snapshot()
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
		var params struct {
			Timeout time.Duration `json:"timeout"`
		}
		_ = json.Unmarshal(request.Params, &params)
		waitCtx := ctx
		cancel := func() {}
		if params.Timeout > 0 {
			waitCtx, cancel = context.WithTimeout(ctx, params.Timeout)
		}
		defer cancel()
		select {
		case <-s.terminal:
			response.OK = true
			response.Result = s.Snapshot()
		case <-waitCtx.Done():
			response.Error = waitCtx.Err().Error()
		}
	default:
		response.Error = fmt.Sprintf("unsupported IPC method %q", request.Method)
	}
	return response
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
