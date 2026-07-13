package supervisor

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// sha256 and hex are used for WorkerSocketPath shortening and token hashing.

// CallerRole is the authenticated IPC authority for a request.
type CallerRole string

const (
	CallerControl CallerRole = "control"
	CallerWorker  CallerRole = "worker"
	CallerNone    CallerRole = ""
)

// controlMethods are operator / Main Agent methods. Workers must never invoke them.
var controlMethods = map[string]bool{
	"ping": true, "status": true, "events": true, "wait": true,
	"cancel": true, "inbox": true, "send": true, "resolve_message": true,
	"barrier.accept": true, "barrier_accept": true, "accept_barrier_warnings": true,
	"barrier.reject": true, "barrier_reject": true, "reject_barrier_warnings": true,
	"final.accept": true, "final_accept": true, "accept_final_warnings": true,
	"final.reject": true, "final_reject": true, "reject_final_warnings": true,
}

// Authorize fails closed when role/method is not permitted.
func Authorize(role CallerRole, method string) error {
	method = strings.TrimSpace(method)
	if method == "" {
		return fmt.Errorf("unauthorized")
	}
	switch role {
	case CallerControl:
		if controlMethods[method] {
			return nil
		}
		// Control may also call worker_request for tests/tools if needed — no, fail closed.
		return fmt.Errorf("unauthorized")
	case CallerWorker:
		if method == "worker_request" {
			return nil
		}
		return fmt.Errorf("unauthorized")
	default:
		return fmt.Errorf("unauthorized")
	}
}

// WorkerCredentialBinding is the authoritative identity for a Worker IPC credential.
// The raw token is never logged or returned in errors.
type WorkerCredentialBinding struct {
	TokenHash       string
	RunID           string
	TaskID          string
	WorkerID        string
	AttemptNumber   int
	NativeSessionID string
	Revoked         bool
	Bound           bool // true when activated against a real native session
}

// AuthState holds control and worker credentials for one Supervisor process.
type AuthState struct {
	mu           sync.RWMutex
	controlToken string // raw; never logged
	controlHash  string
	workers      map[string]*WorkerCredentialBinding // token hash -> binding
	byAttempt    map[string]string                   // task|worker|attempt -> token hash
}

func newAuthState() *AuthState {
	return &AuthState{
		workers:   map[string]*WorkerCredentialBinding{},
		byAttempt: map[string]string{},
	}
}

// GenerateToken returns a 32-byte cryptographically random token as hex (256 bits).
func GenerateToken() (string, error) {
	var buf [32]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("generate credential: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}

func hashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		// Compare against a to avoid timing on length alone of secrets when possible.
		return subtle.ConstantTimeCompare([]byte(a), []byte(a)) == 1 && false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// InitControlCredential generates a control token and writes it to control/auth.token (0600).
// Never place this token in Worker env, argv, hooks, or MCP config.
func (a *AuthState) InitControlCredential(runDir string) (string, error) {
	token, err := GenerateToken()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(runDir, "control")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	path := ControlTokenPath(runDir)
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		return "", err
	}
	a.mu.Lock()
	a.controlToken = token
	a.controlHash = hashToken(token)
	a.mu.Unlock()
	return token, nil
}

// LoadControlCredential loads an existing control token from disk (recovery/CLI).
func LoadControlCredential(runDir string) (string, error) {
	data, err := os.ReadFile(ControlTokenPath(runDir))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// ControlTokenPath is the operator-only credential file (mode 0600).
func ControlTokenPath(runDir string) string {
	return filepath.Join(runDir, "control", "auth.token")
}

// AuthenticateControl returns CallerControl when the token matches.
func (a *AuthState) AuthenticateControl(token string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.controlToken == "" || token == "" {
		return false
	}
	return constantTimeEqual(a.controlToken, token)
}

// IssueWorkerCredential creates a per-attempt Worker token. Returns the raw token
// for injection into Worker env only (never argv).
func (a *AuthState) IssueWorkerCredential(runID, taskID, workerID string, attempt int, nativeSessionID string) (string, error) {
	if attempt <= 0 || taskID == "" || workerID == "" {
		return "", fmt.Errorf("incomplete worker identity for credential")
	}
	token, err := GenerateToken()
	if err != nil {
		return "", err
	}
	h := hashToken(token)
	key := attemptKey(taskID, workerID, attempt)
	a.mu.Lock()
	// Revoke any prior credential for this attempt key.
	if old, ok := a.byAttempt[key]; ok {
		if b := a.workers[old]; b != nil {
			b.Revoked = true
		}
	}
	a.workers[h] = &WorkerCredentialBinding{
		TokenHash: h, RunID: runID, TaskID: taskID, WorkerID: workerID,
		AttemptNumber: attempt, NativeSessionID: nativeSessionID,
	}
	a.byAttempt[key] = h
	a.mu.Unlock()
	return token, nil
}

// BindWorkerSession activates a provisional Worker credential against a real
// native session. The session ID must be non-empty and cannot change once bound.
// Revoked credentials cannot be rebound. Wrong task/worker/attempt is rejected.
func (a *AuthState) BindWorkerSession(token, nativeSessionID string) error {
	if strings.TrimSpace(nativeSessionID) == "" {
		return fmt.Errorf("native session id is required for credential binding")
	}
	if token == "" {
		return fmt.Errorf("token is required for credential binding")
	}
	h := hashToken(token)
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.workers[h]
	if b == nil {
		return fmt.Errorf("credential not found")
	}
	if b.Revoked {
		return fmt.Errorf("credential has been revoked")
	}
	if b.Bound {
		if b.NativeSessionID != nativeSessionID {
			return fmt.Errorf("credential already bound to session %q; cannot rebind to %q",
				b.NativeSessionID, nativeSessionID)
		}
		return nil // already bound to same session
	}
	b.NativeSessionID = nativeSessionID
	b.Bound = true
	return nil
}

// RevokeWorkerAttempt invalidates credentials for a Task/Worker/attempt.
func (a *AuthState) RevokeWorkerAttempt(taskID, workerID string, attempt int) {
	key := attemptKey(taskID, workerID, attempt)
	a.mu.Lock()
	defer a.mu.Unlock()
	if h, ok := a.byAttempt[key]; ok {
		if b := a.workers[h]; b != nil {
			b.Revoked = true
		}
		delete(a.byAttempt, key)
	}
}

// AuthenticateWorker validates a Worker token and returns its binding.
func (a *AuthState) AuthenticateWorker(token string) (*WorkerCredentialBinding, bool) {
	if token == "" {
		return nil, false
	}
	h := hashToken(token)
	a.mu.RLock()
	defer a.mu.RUnlock()
	b := a.workers[h]
	if b == nil || b.Revoked {
		return nil, false
	}
	// Copy to avoid races.
	cp := *b
	return &cp, true
}

func attemptKey(taskID, workerID string, attempt int) string {
	return fmt.Sprintf("%s|%s|%d", taskID, workerID, attempt)
}

// WorkerSocketPath is the Worker-plane Unix socket (worker_request only).
// Shortened when the path would exceed Linux abstract-path limits (same as SocketPath).
func WorkerSocketPath(runDir string) string {
	path := filepath.Join(runDir, "control", "worker.sock")
	if len(path) < 100 {
		return path
	}
	sum := sha256.Sum256([]byte(path))
	return filepath.Join(os.TempDir(), "broker-worker-"+hex.EncodeToString(sum[:8])+".sock")
}
