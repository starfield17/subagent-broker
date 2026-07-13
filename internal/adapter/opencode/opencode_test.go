package opencode

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/protocol"
	"github.com/vnai/subagent-broker/internal/event"
)

const validEnvelope = `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"succeeded","summary":"latest","work_completed":["done"],"files_changed":[],"no_files_changed_reason":"fixture","validation":[{"command":"fixture","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]}`

const olderEnvelope = `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"succeeded","summary":"older","work_completed":["done"],"files_changed":[],"no_files_changed_reason":"fixture","validation":[{"command":"fixture","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]}`

// realMsg builds an official-shape OpenCode message fixture.
func realMsg(id, sessionID, role string, input, output int64, cost float64, text string) string {
	return `{"info":{"id":` + jsonQuote(id) + `,"sessionID":` + jsonQuote(sessionID) + `,"role":` + jsonQuote(role) + `,"tokens":{"input":` + itoa(input) + `,"output":` + itoa(output) + `},"cost":` + ftoa(cost) + `},"parts":[{"type":"text","text":` + jsonQuote(text) + `}]}`
}

func itoa(v int64) string { return strings.TrimSpace(strings.ReplaceAll(jsonNumber(v), "\n", "")) }
func ftoa(v float64) string {
	b, _ := json.Marshal(v)
	return string(b)
}
func jsonNumber(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func sessionProps(sessionID string) []byte {
	return []byte(`{"sessionID":` + jsonQuote(sessionID) + `}`)
}

func statusProps(sessionID, statusType string) []byte {
	return []byte(`{"sessionID":` + jsonQuote(sessionID) + `,"status":{"type":` + jsonQuote(statusType) + `}}`)
}

func messageUpdatedProps(sessionID string) []byte {
	return []byte(`{"sessionID":` + jsonQuote(sessionID) + `,"info":{}}`)
}

func TestCollectFinalFromSessionMessages(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/session/ses-fixture/message" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("content-type", "application/json")
		_, _ = writer.Write([]byte(`[` + realMsg("m1", "ses-fixture", "assistant", 3, 4, 0.01, validEnvelope) + `]`))
	}))
	defer server.Close()
	a := New("")
	state := &sessionState{baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-fixture"}
	if err := a.collectFinalTestOnly(state); err != nil {
		t.Fatal(err)
	}
	if string(state.final) == "" || state.usage.InputTokens != 3 || state.usage.OutputTokens != 4 {
		t.Fatalf("unexpected final=%q usage=%+v", string(state.final), state.usage)
	}
	if _, err := a.NormalizeEvent(adapter.NativeEvent{Kind: "session.idle", Payload: sessionProps("ses-fixture")}); err != nil {
		t.Fatal(err)
	}
}

func TestCollectFinalSelectsNewestValidEnvelope(t *testing.T) {
	payload := `[` +
		realMsg("m1", "ses-multi", "assistant", 1, 1, 0, olderEnvelope) + `,` +
		realMsg("m2", "ses-multi", "user", 0, 0, 0, "follow up") + `,` +
		realMsg("m3", "ses-multi", "assistant", 2, 2, 0, "not an envelope") + `,` +
		realMsg("m4", "ses-multi", "assistant", 5, 6, 0.02, validEnvelope) + `]`
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
	if err := a.collectFinalTestOnly(state); err != nil {
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

func TestRealSchemaFirstTurnShape(t *testing.T) {
	// Official first-turn shape: empty baseline is authoritative; info.id/sessionID/tokens.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/message") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[` + realMsg("msg-1", "ses-a", "assistant", 3, 4, 0.01, validEnvelope) + `]`))
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
		baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-a",
		stream:     protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:   make(chan struct{}),
		generation: 1, promptGen: 1, promptInFlight: true, currentTurn: turn,
	}
	// Empty first-turn baseline is authoritative (baselineCaptured + empty map).
	if !turn.baselineCaptured || len(turn.baselineMessageIDs) != 0 {
		t.Fatal("empty baseline must be authoritative")
	}
	// Also verify captureMessageIDs accepts empty HTTP arrays.
	emptySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer emptySrv.Close()
	baseline, err := a.captureMessageIDs(context.Background(), &sessionState{
		baseURL: emptySrv.URL, directory: t.TempDir(), sessionID: "ses-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(baseline) != 0 {
		t.Fatalf("empty baseline must be empty map, got %v", baseline)
	}
	a.handleEvent(state, sseEvent{Type: "session.status", Properties: statusProps("ses-a", "idle")})
	select {
	case <-turn.ready:
	default:
		t.Fatal("real-schema first turn should complete")
	}
	if !turn.success || !strings.Contains(string(turn.final), "latest") {
		t.Fatalf("turn final=%s success=%v", turn.final, turn.success)
	}
	if turn.usage.InputTokens != 3 || turn.usage.OutputTokens != 4 {
		t.Fatalf("usage from info.tokens: %+v", turn.usage)
	}
}

func TestEmptyFirstTurnBaselineIsAuthoritative(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/message") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[` + realMsg("msg-new", "ses-empty-base", "assistant", 1, 1, 0, validEnvelope) + `]`))
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
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: sessionProps("ses-empty-base")})
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
		_, _ = w.Write([]byte(`[` +
			realMsg("m1", "ses-hist", "assistant", 1, 1, 0, olderEnvelope) + `,` +
			realMsg("m2", "ses-hist", "assistant", 2, 2, 0, "not an envelope") + `]`))
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
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: sessionProps("ses-hist")})
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
	if strings.Contains(string(turn.final), "older") && turn.success {
		t.Fatal("must not reuse msg-old envelope")
	}
}

func TestRealSchemaHistoricalRejection(t *testing.T) {
	TestHistoricalEnvelopeRejectedForNewTurn(t)
}

func TestRealSchemaMalformedTopLevelOnlyID(t *testing.T) {
	// Deliberately wrong fixture: top-level id only — must be rejected.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[{"id":"legacy-id","info":{"sessionID":"ses-a","role":"assistant"},"parts":[]}]`))
	}))
	defer server.Close()
	a := New("")
	state := &sessionState{baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-a"}
	_, err := a.captureMessageIDs(context.Background(), state)
	if err == nil {
		t.Fatal("top-level-only id must fail contract")
	}
	if !strings.Contains(err.Error(), "info.id") {
		t.Fatalf("err=%v", err)
	}
	// collectFinalTurn must also reject.
	turn := &ocTurnResult{baselineCaptured: true, baselineMessageIDs: map[string]struct{}{}}
	_, err = a.collectFinalTurn(state, turn)
	if err == nil {
		t.Fatal("collectFinalTurn must reject top-level-only id")
	}
}

func TestRealSchemaMissingOrMismatchedMessageSession(t *testing.T) {
	a := New("")
	// Missing info.sessionID
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[{"info":{"id":"m1","role":"assistant","tokens":{"input":1,"output":1},"cost":0},"parts":[]}]`))
	}))
	defer server1.Close()
	_, err := a.captureMessageIDs(context.Background(), &sessionState{baseURL: server1.URL, directory: t.TempDir(), sessionID: "ses-a"})
	if err == nil || !strings.Contains(err.Error(), "sessionID") {
		t.Fatalf("missing sessionID: err=%v", err)
	}
	// Mismatched session
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[` + realMsg("m1", "ses-other", "assistant", 1, 1, 0, "x") + `]`))
	}))
	defer server2.Close()
	_, err = a.captureMessageIDs(context.Background(), &sessionState{baseURL: server2.URL, directory: t.TempDir(), sessionID: "ses-a"})
	if err == nil || !strings.Contains(err.Error(), "sessionID") {
		t.Fatalf("mismatched sessionID: err=%v", err)
	}
}

func TestSessionCorrelationWrongSessionActivity(t *testing.T) {
	a := New("")
	turn := &ocTurnResult{generation: 1, ready: make(chan struct{}), baselineCaptured: true, baselineMessageIDs: map[string]struct{}{}}
	state := &sessionState{
		stream:     protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:   make(chan struct{}),
		sessionID:  "ses-a",
		generation: 1, promptGen: 1, promptInFlight: true, currentTurn: turn,
	}
	a.handleEvent(state, sseEvent{Type: "message.updated", Properties: messageUpdatedProps("ses-b")})
	if turn.sawCurrentActivity {
		t.Fatal("wrong-session message.updated must not mark activity")
	}
	if len(state.history) != 0 {
		t.Fatalf("wrong-session event must not append history: %d", len(state.history))
	}
	// Drain stream — should be empty.
	select {
	case ev := <-state.stream.Events():
		t.Fatalf("wrong-session published event: %+v", ev)
	default:
	}
}

func TestSessionCorrelationWrongSessionIdle(t *testing.T) {
	a := New("")
	turn := &ocTurnResult{
		generation: 1, ready: make(chan struct{}),
		baselineCaptured: true, baselineMessageIDs: map[string]struct{}{},
		sawCurrentActivity: true,
	}
	state := &sessionState{
		stream:     protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:   make(chan struct{}),
		sessionID:  "ses-a",
		generation: 1, promptGen: 1, promptInFlight: true, currentTurn: turn,
	}
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: sessionProps("ses-b")})
	if turn.completed || !state.promptInFlight {
		t.Fatal("wrong-session idle must leave turn in flight")
	}
	select {
	case <-turn.ready:
		t.Fatal("wrong-session idle must not close readiness")
	default:
	}
}

func TestSessionStatusIdleCompletesTurn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/message") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[` + realMsg("m1", "ses-a", "assistant", 1, 1, 0, validEnvelope) + `]`))
	}))
	defer server.Close()
	a := New("")
	turn := &ocTurnResult{
		generation: 1, ready: make(chan struct{}),
		baselineCaptured: true, baselineMessageIDs: map[string]struct{}{},
		sawCurrentActivity: true,
	}
	state := &sessionState{
		baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-a",
		stream:     protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:   make(chan struct{}),
		generation: 1, promptGen: 1, promptInFlight: true, currentTurn: turn,
	}
	// busy marks activity (already true); idle collects exactly once.
	a.handleEvent(state, sseEvent{Type: "session.status", Properties: statusProps("ses-a", "busy")})
	a.handleEvent(state, sseEvent{Type: "session.status", Properties: statusProps("ses-a", "idle")})
	select {
	case <-turn.ready:
	default:
		t.Fatal("session.status idle must complete turn")
	}
	if !turn.success {
		t.Fatal("expected success")
	}
	// Second idle must not publish another boundary.
	a.handleEvent(state, sseEvent{Type: "session.status", Properties: statusProps("ses-a", "idle")})
	boundaries := 0
	for _, h := range state.history {
		if h.Kind == event.ResultSubmitted || h.Kind == event.TurnFailed {
			boundaries++
		}
	}
	if boundaries != 1 {
		t.Fatalf("expected one boundary, got %d", boundaries)
	}
}

func TestSessionStatusDeprecatedIdleCompatibility(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/message") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[` + realMsg("m1", "ses-a", "assistant", 1, 1, 0, validEnvelope) + `]`))
	}))
	defer server.Close()
	a := New("")
	turn := &ocTurnResult{
		generation: 1, ready: make(chan struct{}),
		baselineCaptured: true, baselineMessageIDs: map[string]struct{}{},
		sawCurrentActivity: true,
	}
	state := &sessionState{
		baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-a",
		stream:     protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:   make(chan struct{}),
		generation: 1, promptGen: 1, promptInFlight: true, currentTurn: turn,
	}
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: sessionProps("ses-a")})
	select {
	case <-turn.ready:
	default:
		t.Fatal("deprecated session.idle must complete through same handler")
	}
	if !turn.success {
		t.Fatal("expected success")
	}
}

func TestSessionErrorFailsOneGeneration(t *testing.T) {
	a := New("")
	turn := &ocTurnResult{
		generation: 1, ready: make(chan struct{}),
		baselineCaptured: true, baselineMessageIDs: map[string]struct{}{},
		sawCurrentActivity: true,
	}
	state := &sessionState{
		stream:     protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 8, ProgressQueueLimit: 8}),
		shutdown:   make(chan struct{}),
		sessionID:  "ses-a",
		generation: 1, promptGen: 1, promptInFlight: true, currentTurn: turn,
	}
	a.handleEvent(state, sseEvent{Type: "session.error", Properties: sessionProps("ses-a")})
	select {
	case <-turn.ready:
	default:
		t.Fatal("session.error must close readiness")
	}
	if turn.success || !turn.completed || state.promptInFlight {
		t.Fatalf("expected failed generation: success=%v completed=%v inFlight=%v", turn.success, turn.completed, state.promptInFlight)
	}
	if turn.resultErr != "OpenCode session error" {
		t.Fatalf("resultErr=%s", turn.resultErr)
	}
	boundaries := 0
	for _, h := range state.history {
		if h.Kind == event.TurnFailed {
			boundaries++
		}
	}
	if boundaries != 1 {
		t.Fatalf("expected one TurnFailed, got %d", boundaries)
	}
	// Later idle must not publish a second boundary.
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: sessionProps("ses-a")})
	boundaries = 0
	for _, h := range state.history {
		if h.Kind == event.ResultSubmitted || h.Kind == event.TurnFailed {
			boundaries++
		}
	}
	if boundaries != 1 {
		t.Fatalf("idle after error must not add boundary, got %d", boundaries)
	}
}

func TestSessionCorrelationCrossSessionThreeTurnIsolation(t *testing.T) {
	// Two logical sessions: events for ses-b must never alter ses-a turns.
	var mu sync.Mutex
	messagesA := `[]`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
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
		_, _ = w.Write([]byte(messagesA))
	}))
	defer server.Close()
	a := New("")
	stateA := &sessionState{
		baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-a",
		stream:      protocol.NewEventStream(protocol.EventStreamOptions{OutputBuffer: 16, ProgressQueueLimit: 16}),
		shutdown:    make(chan struct{}),
		resultReady: make(chan struct{}),
	}
	env1 := `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"succeeded","summary":"a1","work_completed":["a"],"files_changed":[],"no_files_changed_reason":"n","validation":[{"command":"c","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]}`
	env2 := `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"succeeded","summary":"a2","work_completed":["b"],"files_changed":[],"no_files_changed_reason":"n","validation":[{"command":"c","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]}`
	env3 := `{"schema_version":"v1alpha1","task_id":"t","worker_id":"w","status":"succeeded","summary":"a3","work_completed":["c"],"files_changed":[],"no_files_changed_reason":"n","validation":[{"command":"c","passed":true}],"remaining_work":[],"blocking_issues":[],"risks":[],"handoff_notes":[]}`

	// Turn 1
	if err := a.promptAsync(context.Background(), stateA, adapter.StartRequest{Contract: "t1"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	messagesA = `[` + realMsg("m1", "ses-a", "assistant", 1, 1, 0, env1) + `]`
	mu.Unlock()
	// Noise from ses-b must not mark activity or complete.
	a.handleEvent(stateA, sseEvent{Type: "message.updated", Properties: messageUpdatedProps("ses-b")})
	a.handleEvent(stateA, sseEvent{Type: "session.idle", Properties: sessionProps("ses-b")})
	if stateA.currentTurn == nil || stateA.currentTurn.completed {
		t.Fatal("ses-b events must not complete ses-a turn")
	}
	a.handleEvent(stateA, sseEvent{Type: "message.updated", Properties: messageUpdatedProps("ses-a")})
	a.handleEvent(stateA, sseEvent{Type: "session.status", Properties: statusProps("ses-a", "idle")})
	if !strings.Contains(string(stateA.final), "a1") {
		t.Fatalf("turn1 final=%s", stateA.final)
	}

	// Turn 2: baseline has m1; only m2 is new. Historical must not be reused if m2 invalid.
	if err := a.promptAsync(context.Background(), stateA, adapter.StartRequest{Contract: "t2"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	messagesA = `[` + realMsg("m1", "ses-a", "assistant", 1, 1, 0, env1) + `,` + realMsg("m2", "ses-a", "assistant", 2, 2, 0, env2) + `]`
	mu.Unlock()
	a.handleEvent(stateA, sseEvent{Type: "message.updated", Properties: messageUpdatedProps("ses-a")})
	a.handleEvent(stateA, sseEvent{Type: "session.idle", Properties: sessionProps("ses-a")})
	if !strings.Contains(string(stateA.final), "a2") {
		t.Fatalf("turn2 final=%s", stateA.final)
	}

	// Turn 3
	if err := a.promptAsync(context.Background(), stateA, adapter.StartRequest{Contract: "t3"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	messagesA = `[` +
		realMsg("m1", "ses-a", "assistant", 1, 1, 0, env1) + `,` +
		realMsg("m2", "ses-a", "assistant", 2, 2, 0, env2) + `,` +
		realMsg("m3", "ses-a", "assistant", 3, 3, 0, env3) + `]`
	mu.Unlock()
	a.handleEvent(stateA, sseEvent{Type: "message.updated", Properties: messageUpdatedProps("ses-b")})
	a.handleEvent(stateA, sseEvent{Type: "session.error", Properties: sessionProps("ses-b")})
	if stateA.currentTurn.completed {
		t.Fatal("ses-b error must not fail ses-a")
	}
	a.handleEvent(stateA, sseEvent{Type: "message.updated", Properties: messageUpdatedProps("ses-a")})
	a.handleEvent(stateA, sseEvent{Type: "session.status", Properties: statusProps("ses-a", "idle")})
	if !strings.Contains(string(stateA.final), "a3") {
		t.Fatalf("turn3 final=%s", stateA.final)
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
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: sessionProps("ses-stale")})
	select {
	case <-turn.ready:
		t.Fatal("stale idle must not close readiness")
	default:
	}
	if turn.completed || !state.promptInFlight {
		t.Fatal("stale idle must not complete turn or clear in-flight")
	}
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
		_, _ = w.Write([]byte(`[` + realMsg("m1", "ses-dup", "assistant", 1, 1, 0, validEnvelope) + `]`))
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
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: sessionProps("ses-dup")})
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: sessionProps("ses-dup")})
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
	// Production path: turn1 idle begins HTTP collection → block → supersede → release.
	// turn1 collector must not mutate the newer turn.
	var calls atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/message") {
			http.NotFound(w, r)
			return
		}
		n := calls.Add(1)
		if n == 1 {
			close(entered)
			<-release
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[` + realMsg("m1", "ses-own", "assistant", 1, 1, 0, validEnvelope) + `]`))
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
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.handleEvent(state, sseEvent{Type: "session.status", Properties: statusProps("ses-own", "idle")})
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("collection did not start")
	}
	// Supersede while turn1 is blocked in HTTP collect.
	state.mu.Lock()
	state.currentTurn = turn2
	state.promptGen = 2
	state.generation = 2
	state.promptInFlight = true
	state.mu.Unlock()
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("idle handler did not return")
	}
	if turn2.completed || len(turn2.final) > 0 {
		t.Fatal("turn1 collection mutated turn2")
	}
	if turn1.completed {
		t.Fatal("superseded turn1 must not complete after ownership lost")
	}
	if !state.promptInFlight {
		t.Fatal("newer turn must remain in flight")
	}
}

func TestGenerationBaselineCaptureFailureFailsPrompt(t *testing.T) {
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

func TestBaselineCaptureFailureFailsPrompt(t *testing.T) {
	TestGenerationBaselineCaptureFailureFailsPrompt(t)
}

func TestSessionIdleDoesNotStopServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/message") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`[` + realMsg("m1", "ses-idle", "assistant", 1, 1, 0, validEnvelope) + `]`))
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
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: sessionProps("ses-idle")})
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
	if err := a.promptAsync(context.Background(), state, adapter.StartRequest{Contract: "first"}); err != nil {
		t.Fatal(err)
	}
	messagesJSON = `[` + realMsg("m1", "ses-2t", "assistant", 1, 1, 0, env1) + `]`
	a.handleEvent(state, sseEvent{Type: "message.updated", Properties: messageUpdatedProps("ses-2t")})
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: sessionProps("ses-2t")})
	if string(state.final) == "" || !strings.Contains(string(state.final), "first") {
		t.Fatalf("first final=%s", state.final)
	}
	if err := a.promptAsync(context.Background(), state, adapter.StartRequest{Contract: "next"}); err != nil {
		t.Fatal(err)
	}
	messagesJSON = `[` + realMsg("m1", "ses-2t", "assistant", 1, 1, 0, env1) + `,` + realMsg("m2", "ses-2t", "assistant", 2, 2, 0, env2) + `]`
	a.handleEvent(state, sseEvent{Type: "message.updated", Properties: messageUpdatedProps("ses-2t")})
	a.handleEvent(state, sseEvent{Type: "session.idle", Properties: sessionProps("ses-2t")})
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
