package supervisor

import (
	"path/filepath"
	"testing"
)

func TestAuthorizeRoles(t *testing.T) {
	if err := Authorize(CallerControl, "cancel"); err != nil {
		t.Fatal(err)
	}
	if err := Authorize(CallerWorker, "cancel"); err == nil {
		t.Fatal("worker must not cancel")
	}
	if err := Authorize(CallerWorker, "resolve_message"); err == nil {
		t.Fatal("worker must not resolve_message")
	}
	if err := Authorize(CallerWorker, "worker_request"); err != nil {
		t.Fatal(err)
	}
	if err := Authorize(CallerControl, "worker_request"); err == nil {
		t.Fatal("control socket policy rejects worker_request")
	}
}

func TestControlCredentialRoundTrip(t *testing.T) {
	auth := newAuthState()
	dir := t.TempDir()
	tok, err := auth.InitControlCredential(dir)
	if err != nil || tok == "" {
		t.Fatal(err)
	}
	if !auth.AuthenticateControl(tok) {
		t.Fatal("valid control token rejected")
	}
	if auth.AuthenticateControl("wrong") {
		t.Fatal("invalid token accepted")
	}
	if auth.AuthenticateControl("") {
		t.Fatal("empty token accepted")
	}
	loaded, err := LoadControlCredential(dir)
	if err != nil || loaded != tok {
		t.Fatalf("load=%q err=%v", loaded, err)
	}
}

func TestWorkerCredentialBinding(t *testing.T) {
	auth := newAuthState()
	tok, err := auth.IssueWorkerCredential("run1", "task-a", "w1", 1, "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	b, ok := auth.AuthenticateWorker(tok)
	if !ok || b.TaskID != "task-a" || b.AttemptNumber != 1 {
		t.Fatalf("%+v ok=%v", b, ok)
	}
	if !b.Bound || b.NativeSessionID != "sess-1" {
		t.Fatalf("expected pre-bound credential, got bound=%v session=%q", b.Bound, b.NativeSessionID)
	}
	if _, ok := auth.AuthenticateWorker("nope"); ok {
		t.Fatal("bad worker token")
	}
	// Other task must not authenticate as this binding (token is unique).
	tok2, _ := auth.IssueWorkerCredential("run1", "task-b", "w2", 1, "sess-2")
	b2, _ := auth.AuthenticateWorker(tok2)
	if b2.TaskID == "task-a" {
		t.Fatal("cross-task binding")
	}
	auth.RevokeWorkerAttempt("task-a", "w1", 1)
	if _, ok := auth.AuthenticateWorker(tok); ok {
		t.Fatal("revoked token still valid")
	}
}

func TestUnboundCredentialRejected(t *testing.T) {
	auth := newAuthState()
	tok, err := auth.IssueWorkerCredential("run1", "task-a", "w1", 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := auth.AuthenticateWorker(tok); ok {
		t.Fatal("unbound credential must not authenticate")
	}
	if err := auth.BindWorkerSession(tok, "sess-1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := auth.AuthenticateWorker(tok); !ok {
		t.Fatal("bound credential should authenticate")
	}
}

func TestBindWorkerSessionLifecycle(t *testing.T) {
	auth := newAuthState()
	tok, err := auth.IssueWorkerCredential("run1", "task-a", "w1", 1, "")
	if err != nil {
		t.Fatal(err)
	}

	// Unbound cannot authenticate.
	if _, ok := auth.AuthenticateWorker(tok); ok {
		t.Fatal("unbound must be rejected")
	}

	// Bind to a native session.
	if err := auth.BindWorkerSession(tok, "sess-1"); err != nil {
		t.Fatalf("initial bind failed: %v", err)
	}
	if _, ok := auth.AuthenticateWorker(tok); !ok {
		t.Fatal("bound credential should authenticate")
	}

	// Same session idempotent rebind must succeed.
	if err := auth.BindWorkerSession(tok, "sess-1"); err != nil {
		t.Fatalf("idempotent rebind failed: %v", err)
	}

	// Rebind to different session must fail.
	if err := auth.BindWorkerSession(tok, "sess-2"); err == nil {
		t.Fatal("rebind to different session should fail")
	}

	// Revoked credential cannot be bound.
	auth.RevokeWorkerAttempt("task-a", "w1", 1)
	if err := auth.BindWorkerSession(tok, "sess-1"); err == nil {
		t.Fatal("binding revoked credential should fail")
	}
	if _, ok := auth.AuthenticateWorker(tok); ok {
		t.Fatal("revoked token still valid")
	}
}

func TestBindWorkerSessionRejections(t *testing.T) {
	auth := newAuthState()
	tok, _ := auth.IssueWorkerCredential("run1", "task-a", "w1", 1, "")

	// Empty session ID.
	if err := auth.BindWorkerSession(tok, ""); err == nil {
		t.Fatal("empty session id should be rejected")
	}

	// Empty token.
	if err := auth.BindWorkerSession("", "sess-1"); err == nil {
		t.Fatal("empty token should be rejected")
	}

	// Unknown token.
	if err := auth.BindWorkerSession("deadbeef", "sess-1"); err == nil {
		t.Fatal("unknown token should be rejected")
	}

	// Wrong attempt cannot bind (token is unique per issuance).
	tok2, _ := auth.IssueWorkerCredential("run1", "task-a", "w1", 2, "")
	if err := auth.BindWorkerSession(tok2, "sess-a"); err != nil {
		t.Fatal(err)
	}
	// tok (attempt 1) remains unbound and cannot authenticate.
	if _, ok := auth.AuthenticateWorker(tok); ok {
		t.Fatal("unbound attempt-1 credential must not authenticate")
	}
	// After binding attempt-1 it authenticates as attempt 1.
	if err := auth.BindWorkerSession(tok, "sess-1"); err != nil {
		t.Fatal(err)
	}
	b, ok := auth.AuthenticateWorker(tok)
	if !ok || b.AttemptNumber != 1 {
		t.Fatal("attempt-1 credential should still map to attempt 1")
	}
}

func TestControlTokenPathNotUnderWorkerArgs(t *testing.T) {
	// Sanity: path lives under control/.
	p := ControlTokenPath(filepath.Join("runs", "r1"))
	if filepath.Base(filepath.Dir(p)) != "control" {
		t.Fatalf("path=%s", p)
	}
}
