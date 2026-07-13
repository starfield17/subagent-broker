package supervisor

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
)

// TestSupervisorLeaseAcquireAndRelease verifies basic lease lifecycle.
func TestSupervisorLeaseAcquireAndRelease(t *testing.T) {
	runDir, _ := writeFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{})); err != nil {
		t.Fatal(err)
	}
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	if service.lease == nil || !service.lease.Held() {
		t.Fatal("Load must hold a strongly referenced lease")
	}
	if service.lease.File() == nil {
		t.Fatal("lease must retain *os.File ownership, not only Fd()")
	}

	// Verify lock file exists and is never unlinked on release.
	lockPath := LeasePath(runDir)
	infoBefore, err := os.Stat(lockPath)
	if err != nil {
		t.Fatalf("lock file missing: %v", err)
	}

	// Force GC while lease is held — finalizers must not release the flock.
	runtime.GC()
	runtime.GC()
	if !service.lease.Held() {
		t.Fatal("GC released live Supervisor lease")
	}

	// Release via Close — must not unlink.
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	infoAfter, err := os.Stat(lockPath)
	if err != nil {
		t.Fatalf("lock file must remain after release: %v", err)
	}
	// Same path; inode may match on systems that preserve it.
	_ = infoBefore
	_ = infoAfter

	// After release, new service should be able to acquire.
	service2, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatalf("second Load after release failed: %v", err)
	}
	if !service2.lease.Held() {
		t.Fatal("second lease not held")
	}
	_ = service2.Close()
}

// TestSupervisorLeaseSecondStartFails verifies a second process cannot acquire
// while the first holds the lease (using flock LOCK_NB).
func TestSupervisorLeaseSecondStartFails(t *testing.T) {
	runDir, _ := writeFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{})); err != nil {
		t.Fatal(err)
	}
	service1, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	defer service1.Close()

	// Second Load in same process should fail at lease acquisition before any
	// mutable recovery write.
	_, err = Load(runDir, registry, false)
	if err == nil {
		t.Fatal("second Load should fail while first holds lock")
	}
}

// TestLeasePathConsistency verifies the lock file path structure.
func TestLeasePathConsistency(t *testing.T) {
	path := LeasePath(filepath.Join("runs", "r1"))
	if filepath.Base(path) != "supervisor.lock" {
		t.Fatalf("unexpected lock filename: %s", path)
	}
	if filepath.Base(filepath.Dir(path)) != "control" {
		t.Fatalf("lock file not under control/: %s", path)
	}
}

// TestLeaseNeverUnlinked verifies graceful release keeps the stable path.
func TestLeaseNeverUnlinked(t *testing.T) {
	runDir := t.TempDir()
	lease1, err := AcquireSupervisorLease(runDir)
	if err != nil {
		t.Fatal(err)
	}
	path := lease1.Path()
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if err := lease1.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stable lock file unlinked on release: %v", err)
	}
	// Stale metadata without held lock must not block.
	lease2, err := AcquireSupervisorLease(runDir)
	if err != nil {
		t.Fatalf("stale metadata blocked acquisition: %v", err)
	}
	if err := lease2.Close(); err != nil {
		t.Fatal(err)
	}
	// Idempotent close.
	if err := lease2.Close(); err != nil {
		t.Fatal(err)
	}
}
