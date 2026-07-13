package opencode

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
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
	state := &sessionState{
		events:      make(chan adapter.NativeEvent, 8),
		shutdown:    make(chan struct{}),
		resultReady: make(chan struct{}),
		sessionID:   "ses-idle",
	}
	// No process attached: stop would panic/fail if called. Idle must not call stop.
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: []byte(`{}`)})
	// resultReady should be closed for CollectFinalResult path.
	select {
	case <-state.resultReady:
	default:
		t.Fatal("session.idle should signal resultReady without stopping server")
	}
	// Session still open for next-turn.
	if state.closed {
		t.Fatal("session must remain open after idle for multi-turn delivery")
	}
}

func jsonQuote(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}
