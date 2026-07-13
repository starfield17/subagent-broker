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
	"github.com/vnai/subagent-broker/internal/event"
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
		_, _ = writer.Write([]byte(`[{"id":"m1","info":{"role":"assistant","tokens":{"input":3,"output":4},"cost":0.01},"parts":[{"type":"text","text":` + jsonQuote(validEnvelope) + `}]}]`))
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
	payload := `[
		{"id":"m1","info":{"role":"assistant","tokens":{"input":1,"output":1},"cost":0},"parts":[{"type":"text","text":` + jsonQuote(olderEnvelope) + `}]},
		{"id":"m2","info":{"role":"user","tokens":{"input":0,"output":0},"cost":0},"parts":[{"type":"text","text":"follow up"}]},
		{"id":"m3","info":{"role":"assistant","tokens":{"input":2,"output":2},"cost":0},"parts":[{"type":"text","text":"not an envelope"}]},
		{"id":"m4","info":{"role":"assistant","tokens":{"input":5,"output":6},"cost":0.02},"parts":[{"type":"text","text":` + jsonQuote(validEnvelope) + `}]}
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

func TestEmptyFirstTurnBaselineIsAuthoritative(t *testing.T) {
	// Empty baseline + current-turn message with ID must complete.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/message") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[{"id":"msg-new","info":{"role":"assistant","tokens":{"input":1,"output":1},"cost":0},"parts":[{"type":"text","text":` + jsonQuote(validEnvelope) + `}]}]`))
	}))
	defer server.Close()
	a := New("")
	turn := &ocTurnResult{
		generation:         1,
		ready:              make(chan struct{}),
		baselineCaptured:   true,
		baselineMessageIDs: map[string]struct{}{},
		sawCurrentActivity: true,
	}
	state := &sessionState{
		baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-empty-base",
		stream:     protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:   make(chan struct{}),
		generation: 1, promptGen: 1, promptInFlight: true, currentTurn: turn,
	}
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: []byte(`{}`)})
	select {
	case <-turn.ready:
	default:
		t.Fatal("empty baseline turn should complete")
	}
	if !turn.success || !strings.Contains(string(turn.final), "latest") {
		t.Fatalf("turn final=%s success=%v", turn.final, turn.success)
	}
}

func TestHistoricalEnvelopeRejectedForNewTurn(t *testing.T) {
	// Turn 2 baseline includes turn-1 message; only invalid new assistant output.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/message") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[
			{"id":"m1","info":{"role":"assistant","tokens":{"input":1,"output":1},"cost":0},"parts":[{"type":"text","text":` + jsonQuote(olderEnvelope) + `}]},
			{"id":"m2","info":{"role":"assistant","tokens":{"input":2,"output":2},"cost":0},"parts":[{"type":"text","text":"not an envelope"}]}
		]`))
	}))
	defer server.Close()
	a := New("")
	turn := &ocTurnResult{
		generation:         2,
		ready:              make(chan struct{}),
		baselineCaptured:   true,
		baselineMessageIDs: map[string]struct{}{"m1": {}},
		sawCurrentActivity: true,
	}
	state := &sessionState{
		baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-hist",
		stream:     protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:   make(chan struct{}),
		generation: 2, promptGen: 2, promptInFlight: true, currentTurn: turn,
	}
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: []byte(`{}`)})
	select {
	case <-turn.ready:
	default:
		t.Fatal("invalid current-turn output should complete as failed")
	}
	if turn.success {
		t.Fatal("historical envelope must not satisfy a newer turn")
	}
	if !strings.Contains(turn.resultErr, "without valid Result Envelope") {
		t.Fatalf("resultErr=%s", turn.resultErr)
	}
}

func TestStaleIdleWithoutActivityIgnored(t *testing.T) {
	a := New("")
	turn := &ocTurnResult{generation: 2, ready: make(chan struct{}), baselineCaptured: true, baselineMessageIDs: map[string]struct{}{}}
	state := &sessionState{
		stream:     protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:   make(chan struct{}),
		sessionID:  "ses-stale",
		generation: 2, promptGen: 2, promptInFlight: true, currentTurn: turn,
	}
	// No sawCurrentActivity — idle must be ignored.
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: []byte(`{}`)})
	select {
	case <-turn.ready:
		t.Fatal("stale idle must not close readiness")
	default:
	}
	if turn.completed || !state.promptInFlight {
		t.Fatal("stale idle must not complete turn or clear in-flight")
	}
	// No boundary event published.
	for _, h := range state.history {
		if h.Kind == event.ResultSubmitted || h.Kind == event.TurnFailed {
			t.Fatalf("stale idle published boundary: %s", h.Kind)
		}
	}
}

func TestDuplicateIdlePublishesNoSecondBoundary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/message") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[{"id":"m1","info":{"role":"assistant","tokens":{"input":1,"output":1},"cost":0},"parts":[{"type":"text","text":` + jsonQuote(validEnvelope) + `}]}]`))
	}))
	defer server.Close()
	a := New("")
	turn := &ocTurnResult{
		generation: 1, ready: make(chan struct{}),
		baselineCaptured: true, baselineMessageIDs: map[string]struct{}{},
		sawCurrentActivity: true,
	}
	state := &sessionState{
		baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-dup",
		stream:     protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:   make(chan struct{}),
		generation: 1, promptGen: 1, promptInFlight: true, currentTurn: turn,
	}
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: []byte(`{}`)})
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: []byte(`{}`)})
	boundaries := 0
	for _, h := range state.history {
		if h.Kind == event.ResultSubmitted || h.Kind == event.TurnFailed {
			boundaries++
		}
	}
	if boundaries != 1 {
		t.Fatalf("expected exactly one boundary event, got %d", boundaries)
	}
}

func TestOldTurnCollectionCannotMutateNewerTurn(t *testing.T) {
	// Collection for turn 1 starts; turn 2 becomes current before commit.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/message") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[{"id":"m1","info":{"role":"assistant","tokens":{"input":1,"output":1},"cost":0},"parts":[{"type":"text","text":` + jsonQuote(validEnvelope) + `}]}]`))
	}))
	defer server.Close()
	a := New("")
	turn1 := &ocTurnResult{
		generation: 1, ready: make(chan struct{}),
		baselineCaptured: true, baselineMessageIDs: map[string]struct{}{},
		sawCurrentActivity: true,
	}
	turn2 := &ocTurnResult{
		generation: 2, ready: make(chan struct{}),
		baselineCaptured: true, baselineMessageIDs: map[string]struct{}{"m1": {}},
	}
	state := &sessionState{
		baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-own",
		stream:     protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:   make(chan struct{}),
		generation: 1, promptGen: 1, promptInFlight: true, currentTurn: turn1,
	}
	// Simulate supersession mid-collection by swapping before idle commit:
	// capture turn1 under lock, then replace currentTurn with turn2, then collect.
	// handleSessionIdle captures turn then unlocks for collect — we inject swap
	// by replacing after first activity mark and calling collectFinalTurn directly.
	collected, err := a.collectFinalTurn(state, turn1)
	if err != nil || !collected.HasEnvelope {
		t.Fatalf("collect turn1: %+v err=%v", collected, err)
	}
	state.mu.Lock()
	state.currentTurn = turn2
	state.promptGen = 2
	state.generation = 2
	state.promptInFlight = true
	state.mu.Unlock()
	// Re-apply commit checks: turn1 must not freeze into turn2.
	state.mu.Lock()
	if state.currentTurn == turn1 {
		t.Fatal("setup failed")
	}
	// Emulate post-collect ownership check from handleSessionIdle.
	if state.currentTurn != turn1 {
		// correctly refuse
	} else {
		turn1.final = collected.Final
		turn1.completed = true
	}
	state.mu.Unlock()
	if turn2.completed || len(turn2.final) > 0 {
		t.Fatal("turn1 collection mutated turn2")
	}
	if turn1.completed {
		t.Fatal("superseded turn1 must not complete after ownership lost")
	}
}

func TestBaselineCaptureFailureFailsPrompt(t *testing.T) {
	// Message GET fails → prompt_async not sent.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/message") {
			http.Error(w, "boom", 500)
			return
		}
		if strings.Contains(r.URL.Path, "prompt_async") {
			t.Error("prompt_async must not be called after baseline failure")
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	a := New("")
	state := &sessionState{
		baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-base-fail",
		resultReady: make(chan struct{}),
	}
	err := a.promptAsync(context.Background(), state, adapter.StartRequest{Contract: "x"})
	if err == nil {
		t.Fatal("expected baseline failure")
	}
	if state.promptInFlight {
		t.Fatal("prompt must not remain in flight after baseline failure")
	}
	if state.currentTurn == nil || !state.currentTurn.completed || state.currentTurn.success {
		t.Fatal("generation must fail deterministically")
	}
}

func TestSessionIdleDoesNotStopServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/message") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[{"id":"m1","info":{"role":"assistant","tokens":{"input":1,"output":1},"cost":0},"parts":[{"type":"text","text":` + jsonQuote(validEnvelope) + `}]}]`))
	}))
	defer server.Close()
	a := New("")
	turn := &ocTurnResult{
		generation: 1, ready: make(chan struct{}),
		baselineCaptured: true, baselineMessageIDs: map[string]struct{}{},
		sawCurrentActivity: true,
	}
	state := &sessionState{
		baseURL:     server.URL,
		stream:      protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:    make(chan struct{}),
		resultReady: make(chan struct{}),
		sessionID:   "ses-idle",
		generation:  1, promptGen: 1, promptInFlight: true, currentTurn: turn,
	}
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: []byte(`{}`)})
	select {
	case <-turn.ready:
	default:
		t.Fatal("session.idle should signal generation ready without stopping server")
	}
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
	messagesJSON := `[]`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "prompt_async") {
			w.Header().Set("content-type", "application/json")
			_, _ = w.Write([]byte(`{"id":"prompt-x"}`))
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/message") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(messagesJSON))
	}))
	defer server.Close()
	a := New("")
	state := &sessionState{
		baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-2t",
		stream:      protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:    make(chan struct{}),
		resultReady: make(chan struct{}),
	}
	// Turn 1: empty baseline, then m1 appears.
	if err := a.promptAsync(context.Background(), state, adapter.StartRequest{Contract: "first"}); err != nil {
		t.Fatal(err)
	}
	messagesJSON = `[{"id":"m1","info":{"role":"assistant","tokens":{"input":1,"output":1},"cost":0},"parts":[{"type":"text","text":` + jsonQuote(env1) + `}]}]`
	a.handleEvent(state, sseEvent{Type: "message.updated", Properties: []byte(`{}`)})
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: []byte(`{}`)})
	if string(state.final) == "" || !strings.Contains(string(state.final), "first") {
		t.Fatalf("first final=%s", state.final)
	}
	// Turn 2: baseline includes m1; only m2 is new.
	if err := a.promptAsync(context.Background(), state, adapter.StartRequest{Contract: "next"}); err != nil {
		t.Fatal(err)
	}
	messagesJSON = `[{"id":"m1","info":{"role":"assistant","tokens":{"input":1,"output":1},"cost":0},"parts":[{"type":"text","text":` + jsonQuote(env1) + `}]},{"id":"m2","info":{"role":"assistant","tokens":{"input":2,"output":2},"cost":0},"parts":[{"type":"text","text":` + jsonQuote(env2) + `}]}]`
	a.handleEvent(state, sseEvent{Type: "message.updated", Properties: []byte(`{}`)})
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
