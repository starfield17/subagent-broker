package supervisor

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
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
		if err := s.RequestCancel(ctx); err != nil {
			response.Error = err.Error()
			return response
		}
		response.OK = true
		response.Result = s.Snapshot()
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
