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

func TestControlTokenPathNotUnderWorkerArgs(t *testing.T) {
	// Sanity: path lives under control/.
	p := ControlTokenPath(filepath.Join("runs", "r1"))
	if filepath.Base(filepath.Dir(p)) != "control" {
		t.Fatalf("path=%s", p)
	}
}
