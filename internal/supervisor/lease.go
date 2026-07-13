package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/vnai/subagent-broker/internal/process"
)

// SupervisorLease is the exclusive ownership handle for a Run's mutable Supervisor
// lifetime. The owning *os.File is strongly referenced for the full lease lifetime
// so GC finalizers cannot release the flock while the Supervisor is alive.
//
// The stable path <runDir>/control/supervisor.lock is never unlinked on release.
// Only close (which releases the kernel lock) is performed. The kernel lock is
// authoritative; file contents are diagnostic metadata only.
type SupervisorLease struct {
	mu   sync.Mutex
	file *os.File
	path string
}

// LeasePath is the run-scoped supervisor lock file (flock).
func LeasePath(runDir string) string {
	return filepath.Join(runDir, "control", "supervisor.lock")
}

// AcquireSupervisorLease opens (or creates) the stable lock file and acquires
// LOCK_EX | LOCK_NB. Diagnostic metadata is rewritten only after the lock succeeds.
// A stale metadata file without a held kernel lock never blocks startup.
func AcquireSupervisorLease(runDir string) (*SupervisorLease, error) {
	lockPath := LeasePath(runDir)
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
		return nil, fmt.Errorf("create lock directory: %w", err)
	}
	// Open existing file when present so the inode stays stable across owners.
	fd, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = fd.Close()
		return nil, fmt.Errorf("supervisor lock already held by another process: %w", err)
	}

	// Real process start token when available; never use PID as a stand-in.
	startToken := ""
	if identity, inspectErr := process.Inspect(context.Background(), os.Getpid()); inspectErr == nil {
		startToken = identity.StartToken
	}

	_ = fd.Truncate(0)
	_, _ = fd.Seek(0, 0)
	meta := map[string]any{
		"pid":                 os.Getpid(),
		"process_start_token": startToken,
		"acquired_at":         time.Now().UTC().Format(time.RFC3339),
		"run_id":              "",
	}
	// Best-effort run_id from run.json (diagnostic only).
	if data, readErr := os.ReadFile(filepath.Join(runDir, "run.json")); readErr == nil {
		var partial struct {
			RunID string `json:"run_id"`
		}
		if json.Unmarshal(data, &partial) == nil {
			meta["run_id"] = partial.RunID
		}
	}
	metaJSON, _ := json.Marshal(meta)
	_, _ = fd.Write(append(metaJSON, '\n'))

	return &SupervisorLease{file: fd, path: lockPath}, nil
}

// Path returns the stable lock file path.
func (l *SupervisorLease) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}

// File returns the owning *os.File (strongly referenced). Tests may inspect it;
// production code must not close it except via Close.
func (l *SupervisorLease) File() *os.File {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file
}

// Close releases the kernel lock by closing the owned file descriptor.
// It is idempotent. It never unlinks the stable lock file path.
func (l *SupervisorLease) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	// Deliberately do NOT os.Remove(l.path). The stable path must remain so
	// subsequent owners lock the same inode rather than creating a new one.
	return err
}

// Held reports whether the lease still owns an open file descriptor.
func (l *SupervisorLease) Held() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file != nil
}
