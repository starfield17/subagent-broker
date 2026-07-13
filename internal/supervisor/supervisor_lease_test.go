package supervisor

import (
	"os"
	"path/filepath"
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

	// First acquisition should succeed.
	if err := service.acquireLease(); err != nil {
		t.Fatalf("first acquireLease failed: %v", err)
	}
	if service.leaseFD <= 0 {
		t.Fatal("leaseFD not set")
	}

	// Verify lock file exists.
	lockPath := LeasePath(runDir)
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("lock file missing: %v", err)
	}

	// Release should clean up.
	service.releaseLease()

	// After release, new service should be able to acquire.
	service2, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := service2.acquireLease(); err != nil {
		t.Fatalf("second acquireLease after release failed: %v", err)
	}
	service2.releaseLease()
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
	if err := service1.acquireLease(); err != nil {
		t.Fatalf("first acquireLease failed: %v", err)
	}
	defer service1.releaseLease()

	// Second service in same process should fail to acquire (LOCK_NB).
	service2, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := service2.acquireLease(); err == nil {
		service2.releaseLease()
		t.Fatal("second acquireLease should fail while first holds lock")
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
