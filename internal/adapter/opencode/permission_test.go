package opencode

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/event"
)

func TestRespondPermissionOpenCode11715AllowRoute(t *testing.T) {
	var mu sync.Mutex
	var seenMethod, seenPath, seenBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		seenMethod = r.Method
		seenPath = r.URL.Path
		seenBody = string(body)
		mu.Unlock()
		if strings.Contains(r.URL.Path, "/session/") && strings.Contains(r.URL.Path, "/permissions/") {
			http.Error(w, "old route must not be used", http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/permission/") && strings.HasSuffix(r.URL.Path, "/reply") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	a := New("")
	state := &sessionState{
		baseURL:   server.URL,
		directory: t.TempDir(),
		sessionID: "ses-1",
	}
	a.mu.Lock()
	a.sessions["ses-1"] = state
	a.mu.Unlock()

	if err := a.RespondPermission(context.Background(), "ses-1", adapter.PermissionDecision{
		RequestID: "perm-abc",
		Allowed:   true,
	}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if seenMethod != http.MethodPost {
		t.Fatalf("method=%s", seenMethod)
	}
	if seenPath != "/permission/perm-abc/reply" {
		t.Fatalf("path=%s", seenPath)
	}
	if strings.Contains(seenPath, "/session/") {
		t.Fatal("session id must not appear in permission reply route")
	}
	var body map[string]string
	if err := json.Unmarshal([]byte(seenBody), &body); err != nil {
		t.Fatal(err)
	}
	if body["reply"] != "once" {
		t.Fatalf("body=%v want reply=once", body)
	}
	if _, ok := body["response"]; ok {
		t.Fatal("must not use response field")
	}
}

func TestRespondPermissionOpenCode11715DenyRoute(t *testing.T) {
	var seenPath, seenBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		seenPath = r.URL.Path
		seenBody = string(body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	a := New("")
	state := &sessionState{baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-2"}
	a.mu.Lock()
	a.sessions["ses-2"] = state
	a.mu.Unlock()
	if err := a.RespondPermission(context.Background(), "ses-2", adapter.PermissionDecision{
		RequestID: "perm-deny", Allowed: false,
	}); err != nil {
		t.Fatal(err)
	}
	if seenPath != "/permission/perm-deny/reply" {
		t.Fatalf("path=%s", seenPath)
	}
	if !strings.Contains(seenBody, `"reply":"reject"`) {
		t.Fatalf("body=%s", seenBody)
	}
	if strings.Contains(seenBody, `"response"`) {
		t.Fatal("must not use response field")
	}
}

func TestRespondPermissionOpenCodeHTTPFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer server.Close()
	a := New("")
	state := &sessionState{baseURL: server.URL, directory: t.TempDir(), sessionID: "ses-3"}
	a.mu.Lock()
	a.sessions["ses-3"] = state
	a.mu.Unlock()
	err := a.RespondPermission(context.Background(), "ses-3", adapter.PermissionDecision{
		RequestID: "perm-x", Allowed: true,
	})
	if err == nil {
		t.Fatal("expected HTTP failure")
	}
}

func TestHandleEventPermissionAskedObjectTool(t *testing.T) {
	// Realistic OpenCode permission.asked with object-valued tool.
	props := json.RawMessage(`{
		"id": "perm-obj-1",
		"tool": {"messageID": "msg-1", "callID": "call-9"},
		"permission": "edit",
		"patterns": ["**/*.go"]
	}`)
	a := New("")
	state := &sessionState{
		events:   make(chan adapter.NativeEvent, 4),
		shutdown: make(chan struct{}),
	}
	a.handleEvent(state, sseEvent{Type: "permission.asked", Properties: props})
	select {
	case native := <-state.events:
		if native.Kind != event.PermissionRequested {
			t.Fatalf("kind=%s", native.Kind)
		}
		// Payload must still carry the id for the supervisor parser.
		var m map[string]json.RawMessage
		if err := json.Unmarshal(native.Payload, &m); err != nil {
			t.Fatal(err)
		}
		var id string
		if err := json.Unmarshal(m["id"], &id); err != nil || id != "perm-obj-1" {
			t.Fatalf("id=%q err=%v payload=%s", id, err, native.Payload)
		}
	default:
		t.Fatal("expected permission event")
	}
}
