package supervisor

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// TestSupervisorLeaseSubprocessContention verifies exclusive ownership across OS
// processes, GC resilience, stable lock path (no unlink), and that a loser
// cannot acquire while the winner holds the lease.
func TestSupervisorLeaseSubprocessContention(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("flock semantics differ on windows")
	}
	runDir := t.TempDir()
	lockPath := LeasePath(runDir)
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
		t.Fatal(err)
	}

	// Process A: acquire lease, force GC, hold, then exit (kill without unlink).
	holdScript := `
package main
import (
  "fmt"
  "os"
  "runtime"
  "syscall"
  "time"
)
func main() {
  path := os.Args[1]
  ready := os.Args[2]
  fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
  if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(2) }
  if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
    fmt.Fprintln(os.Stderr, "lock failed:", err); os.Exit(3)
  }
  // Keep fd strongly referenced; force GC.
  runtime.GC(); runtime.GC()
  if err := os.WriteFile(ready, []byte("ready"), 0600); err != nil {
    fmt.Fprintln(os.Stderr, err); os.Exit(4)
  }
  // Hold until killed or timeout.
  time.Sleep(30 * time.Second)
  _ = fd.Close()
}
`
	srcDir := t.TempDir()
	src := filepath.Join(srcDir, "holder.go")
	if err := os.WriteFile(src, []byte(holdScript), 0o600); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(srcDir, "holder")
	build := exec.Command("go", "build", "-o", bin, src)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build holder: %v\n%s", err, out)
	}

	ready := filepath.Join(runDir, "ready")
	cmdA := exec.Command(bin, lockPath, ready)
	if err := cmdA.Start(); err != nil {
		t.Fatal(err)
	}
	// Wait for ready.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := os.Stat(ready); err != nil {
		_ = cmdA.Process.Kill()
		t.Fatal("holder never became ready")
	}

	// Process B: cannot acquire while A holds.
	fdB, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		_ = cmdA.Process.Kill()
		t.Fatal(err)
	}
	if err := syscall.Flock(int(fdB.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		_ = fdB.Close()
		_ = cmdA.Process.Kill()
		t.Fatal("process B acquired lock while A held it")
	}
	_ = fdB.Close()

	// Lock file still present (A must not unlink).
	if _, err := os.Stat(lockPath); err != nil {
		_ = cmdA.Process.Kill()
		t.Fatalf("lock file missing while held: %v", err)
	}

	// Kill A without graceful cleanup.
	_ = cmdA.Process.Kill()
	_, _ = cmdA.Process.Wait()

	// B can now acquire the same stable path.
	lease, err := AcquireSupervisorLease(runDir)
	if err != nil {
		t.Fatalf("B could not acquire after A killed: %v", err)
	}
	if lease.Path() != lockPath {
		t.Fatalf("path changed: %s vs %s", lease.Path(), lockPath)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	// Stable path remains after graceful release.
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("lock file unlinked on Close: %v", err)
	}
}
