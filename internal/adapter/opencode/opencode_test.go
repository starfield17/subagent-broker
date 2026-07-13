package opencode

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/protocol"
)

const validEnvelope = `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"succeeded","summary":"latest","work_completed":["done"],"files_changed":[],"no_files_changed_reason":"fixture","validation":[{"command":"fixture","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]}`

const olderEnvelope = `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"succeeded","summary":"older","work_completed":["done"],"files_changed":[],"no_files_changed_reason":"fixture","validation":[{"command":"fixture","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]}`

func TestCollectFinalFromSessionMessages(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/session/ses-fixture/message" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("content-type", "application/json")
		_, _ = writer.Write([]byte(`[{"info":{"role":"assistant","tokens":{"input":3,"output":4},"cost":0.01},"parts":[{"type":"text","text":` + jsonQuote(validEnvelope) + `}]}]`))
	}))
	defer server.Close()
	a := New("")
	state := &sessionState{baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-fixture"}
	if err := a.collectFinal(state); err != nil {
		t.Fatal(err)
	}
	if string(state.final) == "" || state.usage.InputTokens != 3 || state.usage.OutputTokens != 4 {
		t.Fatalf("unexpected final=%q usage=%+v", string(state.final), state.usage)
	}
	if _, err := a.NormalizeEvent(adapter.NativeEvent{Kind: "session.idle", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
}

func TestCollectFinalSelectsNewestValidEnvelope(t *testing.T) {
	// History: older valid envelope, then invalid assistant text, then newest valid.
	payload := `[
		{"info":{"role":"assistant","tokens":{"input":1,"output":1},"cost":0},"parts":[{"type":"text","text":` + jsonQuote(olderEnvelope) + `}]},
		{"info":{"role":"user","tokens":{"input":0,"output":0},"cost":0},"parts":[{"type":"text","text":"follow up"}]},
		{"info":{"role":"assistant","tokens":{"input":2,"output":2},"cost":0},"parts":[{"type":"text","text":"not an envelope"}]},
		{"info":{"role":"assistant","tokens":{"input":5,"output":6},"cost":0.02},"parts":[{"type":"text","text":` + jsonQuote(validEnvelope) + `}]}
	]`
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/message") {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("content-type", "application/json")
		_, _ = writer.Write([]byte(payload))
	}))
	defer server.Close()
	a := New("")
	state := &sessionState{baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-multi"}
	if err := a.collectFinal(state); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(state.final), `"summary":"latest"`) {
		t.Fatalf("expected newest envelope, got %s", state.final)
	}
	if strings.Contains(string(state.final), "older") {
		t.Fatal("historical envelope must not be concatenated into final")
	}
	if state.usage.InputTokens != 5 || state.usage.OutputTokens != 6 {
		t.Fatalf("usage should come from newest valid turn: %+v", state.usage)
	}
}

func TestSessionIdleDoesNotStopServer(t *testing.T) {
	a := New("")
	turn := &ocTurnResult{generation: 1, ready: make(chan struct{})}
	state := &sessionState{
		stream:      protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:    make(chan struct{}),
		resultReady: make(chan struct{}),
		sessionID:   "ses-idle",
		generation:  1, promptGen: 1, promptInFlight: true, currentTurn: turn,
	}
	// No process attached: stop would panic/fail if called. Idle must not call stop.
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: []byte(`{}`)})
	// Generation readiness should be closed for CollectFinalResult path.
	select {
	case <-turn.ready:
	default:
		t.Fatal("session.idle should signal generation ready without stopping server")
	}
	// Stream still accepting for next-turn (not aborted/closed by idle).
	if state.stream.Publish(adapter.NativeEvent{Kind: "opencode.ping"}) == protocol.PublishRejected {
		t.Fatal("session must remain open after idle for multi-turn delivery")
	}
}

func TestConcurrentPromptRejected(t *testing.T) {
	a := New("")
	state := &sessionState{
		baseURL:     "http://127.0.0.1:1",
		directory:   t.TempDir(),
		sessionID:   "ses-c",
		resultReady: make(chan struct{}),
	}
	state.promptInFlight = true
	err := a.promptAsync(context.Background(), state, adapter.StartRequest{Contract: "x"})
	if err == nil {
		t.Fatal("expected concurrent prompt rejection")
	}
}

func TestTwoTurnIdleFreezesSecondResult(t *testing.T) {
	env1 := `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"succeeded","summary":"first","work_completed":["a"],"files_changed":[],"no_files_changed_reason":"n","validation":[{"command":"c","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]}`
	env2 := `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"succeeded","summary":"second","work_completed":["b"],"files_changed":[],"no_files_changed_reason":"n","validation":[{"command":"c","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]}`
	call := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/message") {
			http.NotFound(w, r)
			return
		}
		call++
		w.Header().Set("content-type", "application/json")
		if call == 1 {
			_, _ = w.Write([]byte(`[{"info":{"role":"assistant","tokens":{"input":1,"output":1},"cost":0},"parts":[{"type":"text","text":` + jsonQuote(env1) + `}]}]`))
			return
		}
		_, _ = w.Write([]byte(`[{"info":{"role":"assistant","tokens":{"input":1,"output":1},"cost":0},"parts":[{"type":"text","text":` + jsonQuote(env1) + `}]},{"info":{"role":"assistant","tokens":{"input":2,"output":2},"cost":0},"parts":[{"type":"text","text":` + jsonQuote(env2) + `}]}]`))
	}))
	defer server.Close()
	a := New("")
	state := &sessionState{
		baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-2t",
		stream:      protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:    make(chan struct{}),
		resultReady: make(chan struct{}), generation: 1, promptGen: 1, promptInFlight: true,
	}
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: []byte(`{}`)})
	if string(state.final) == "" || !strings.Contains(string(state.final), "first") {
		t.Fatalf("first final=%s", state.final)
	}
	// Second prompt generation.
	if err := a.promptAsync(context.Background(), state, adapter.StartRequest{Contract: "next"}); err != nil {
		// prompt_async hits dead baseURL for POST — only collectFinal uses message route.
		// Manually re-arm like a successful accept for idle test.
		state.mu.Lock()
		state.generation = 2
		state.promptGen = 2
		state.promptInFlight = true
		if state.resultSignaled {
			state.resultReady = make(chan struct{})
			state.resultSignaled = false
		}
		state.mu.Unlock()
	}
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: []byte(`{}`)})
	if !strings.Contains(string(state.final), "second") {
		t.Fatalf("second final=%s", state.final)
	}
	if state.stream.Publish(adapter.NativeEvent{Kind: "opencode.ping"}) == protocol.PublishRejected {
		t.Fatal("server/session must stay open across idles")
	}
}

func jsonQuote(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}
