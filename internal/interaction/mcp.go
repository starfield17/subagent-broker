package interaction

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/supervisor"
)

type WorkerServer struct {
	RunDir   string
	RunID    string
	TaskID   string
	WorkerID string
}

type AskInput struct {
	Question       string           `json:"question" jsonschema:"the single clear question for the Main Agent"`
	Reason         string           `json:"reason" jsonschema:"why work cannot continue without an answer"`
	Category       message.Category `json:"category" jsonschema:"decision, missing_information, conflict, environment, or validation_failure"`
	WorkspaceState string           `json:"workspace_state" jsonschema:"whether the workspace already contains partial changes"`
	RelatedTasks   []string         `json:"related_tasks,omitempty"`
	Suggestion     string           `json:"suggestion,omitempty"`
}

type ScopeInput struct {
	RequestedScope                []string `json:"requested_scope" jsonschema:"project-relative paths or globs requested for writing"`
	Reason                        string   `json:"reason"`
	Consequence                   string   `json:"consequence"`
	PartialModifications          string   `json:"partial_modifications"`
	RelatedTasks                  []string `json:"related_tasks,omitempty"`
	Recommendation                string   `json:"recommendation,omitempty"`
	RequiresPublicInterfaceChange bool     `json:"requires_public_interface_change,omitempty"`
}

type ToolOutput struct {
	MessageID string                  `json:"message_id"`
	Answer    string                  `json:"answer,omitempty"`
	Decision  message.DecisionPayload `json:"decision,omitempty"`
}

func (w WorkerServer) Run(ctx context.Context) error {
	return w.serve(ctx, os.Stdin, os.Stdout)
}

func (w WorkerServer) ask(ctx context.Context, input AskInput) (ToolOutput, error) {
	if input.Category == "" {
		input.Category = message.Decision
	}
	payload := message.QuestionEnvelope{SchemaVersion: supervisor.SchemaVersion, Question: input.Question, Reason: input.Reason, CurrentScope: []string{"See Task Contract"}, RelatedTasks: input.RelatedTasks, WorkspaceState: input.WorkspaceState, Suggestion: input.Suggestion}
	return w.call(ctx, message.Question, input.Category, payload)
}

func (w WorkerServer) requestScope(ctx context.Context, input ScopeInput) (ToolOutput, error) {
	payload := message.ScopeRequestPayload{RequestedScope: input.RequestedScope, Reason: input.Reason, Consequence: input.Consequence, PartialModifications: input.PartialModifications, RelatedTasks: input.RelatedTasks, Recommendation: input.Recommendation, RequiresPublicInterfaceChange: input.RequiresPublicInterfaceChange}
	return w.call(ctx, message.ScopeExpansionRequest, message.Scope, payload)
}

func (w WorkerServer) call(ctx context.Context, messageType message.Type, category message.Category, payload any) (ToolOutput, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return ToolOutput{}, err
	}
	response, err := callSupervisor(ctx, w.RunDir, w.RunID, "worker_request", map[string]any{"task_id": w.TaskID, "worker_id": w.WorkerID, "type": messageType, "category": category, "payload": json.RawMessage(raw)})
	if err != nil {
		return ToolOutput{}, err
	}
	if !response.OK {
		return ToolOutput{}, fmt.Errorf("Broker request failed: %s", response.Error)
	}
	data, err := json.Marshal(response.Result)
	if err != nil {
		return ToolOutput{}, err
	}
	var wrapper struct {
		MessageID  string             `json:"message_id"`
		Resolution message.Resolution `json:"resolution"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return ToolOutput{}, err
	}
	return ToolOutput{MessageID: wrapper.MessageID, Answer: wrapper.Resolution.Answer, Decision: wrapper.Resolution.Decision}, nil
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

func (w WorkerServer) serve(ctx context.Context, input io.Reader, output io.Writer) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
	encoder := json.NewEncoder(output)
	for scanner.Scan() {
		var request rpcRequest
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			continue
		}
		if len(request.ID) == 0 {
			continue
		}
		result, rpcErr := w.handleRPC(ctx, request)
		response := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(request.ID)}
		if rpcErr != nil {
			response["error"] = map[string]any{"code": -32000, "message": rpcErr.Error()}
		} else {
			response["result"] = result
		}
		if err := encoder.Encode(response); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (w WorkerServer) handleRPC(ctx context.Context, request rpcRequest) (any, error) {
	switch request.Method {
	case "initialize":
		return map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]string{"name": "subagent-broker", "version": supervisor.SchemaVersion}}, nil
	case "tools/list":
		return map[string]any{"tools": []any{
			map[string]any{"name": "ask_main_agent", "description": "Ask the Main Agent one blocking question and wait for its answer.", "inputSchema": askSchema()},
			map[string]any{"name": "request_scope_expansion", "description": "Request additional write scope before an out-of-scope edit.", "inputSchema": scopeSchema()},
		}}, nil
	case "tools/call":
		var params struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil {
			return nil, err
		}
		var output ToolOutput
		var err error
		switch params.Name {
		case "ask_main_agent":
			var input AskInput
			if err = json.Unmarshal(params.Arguments, &input); err == nil {
				output, err = w.ask(ctx, input)
			}
		case "request_scope_expansion":
			var input ScopeInput
			if err = json.Unmarshal(params.Arguments, &input); err == nil {
				output, err = w.requestScope(ctx, input)
			}
		default:
			err = fmt.Errorf("unknown tool %q", params.Name)
		}
		if err != nil {
			return map[string]any{"content": []any{map[string]string{"type": "text", "text": err.Error()}}, "isError": true}, nil
		}
		data, _ := json.Marshal(output)
		return map[string]any{"content": []any{map[string]string{"type": "text", "text": string(data)}}, "isError": false}, nil
	default:
		return nil, fmt.Errorf("unsupported method %q", request.Method)
	}
}

func askSchema() map[string]any {
	return map[string]any{"type": "object", "required": []string{"question", "reason", "category", "workspace_state"}, "properties": map[string]any{"question": map[string]string{"type": "string"}, "reason": map[string]string{"type": "string"}, "category": map[string]any{"type": "string", "enum": []string{"decision", "missing_information", "conflict", "environment", "validation_failure"}}, "workspace_state": map[string]string{"type": "string"}, "related_tasks": map[string]any{"type": "array", "items": map[string]string{"type": "string"}}, "suggestion": map[string]string{"type": "string"}}}
}

func scopeSchema() map[string]any {
	return map[string]any{"type": "object", "required": []string{"requested_scope", "reason", "consequence", "partial_modifications"}, "properties": map[string]any{"requested_scope": map[string]any{"type": "array", "items": map[string]string{"type": "string"}}, "reason": map[string]string{"type": "string"}, "consequence": map[string]string{"type": "string"}, "partial_modifications": map[string]string{"type": "string"}, "related_tasks": map[string]any{"type": "array", "items": map[string]string{"type": "string"}}, "recommendation": map[string]string{"type": "string"}, "requires_public_interface_change": map[string]string{"type": "boolean"}}}
}

func LoadRunID(runDir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(runDir, "run.json"))
	if err != nil {
		return "", err
	}
	var value domain.Run
	if err := json.Unmarshal(data, &value); err != nil {
		return "", err
	}
	return string(value.RunID), nil
}

func callSupervisor(ctx context.Context, runDir, runID, method string, params any) (supervisor.Response, error) {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(ctx, "unix", supervisor.SocketPath(runDir))
	if err != nil {
		return supervisor.Response{}, err
	}
	defer conn.Close()
	raw, err := json.Marshal(params)
	if err != nil {
		return supervisor.Response{}, err
	}
	request := supervisor.Request{SchemaVersion: supervisor.SchemaVersion, RequestID: fmt.Sprintf("worker-%d", time.Now().UnixNano()), RunID: runID, Method: method, Params: raw}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return supervisor.Response{}, err
	}
	var response supervisor.Response
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return response, err
	}
	if err := json.Unmarshal(line, &response); err != nil {
		return response, err
	}
	return response, nil
}
