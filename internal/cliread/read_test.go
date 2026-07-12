package cliread

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/supervisor"
)

func TestLoadSnapshotPrefersIPCOverOlderDiskSnapshot(t *testing.T) {
	runDir := t.TempDir()
	identity, err := process.Inspect(context.Background(), os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	runValue := domain.Run{RunID: "run-1", ProjectID: "project-1", TaskIDs: []domain.TaskID{"task-1"}, SupervisorIdentity: &domain.SupervisorIdentity{PID: identity.PID, ProcessStartToken: identity.StartToken}}
	old := supervisor.Snapshot{SchemaVersion: supervisor.SchemaVersion, Run: runValue, UpdatedAt: time.Unix(1, 0).UTC()}
	if err := writeJSON(filepath.Join(runDir, "run.json"), runValue); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(runDir, "state.json"), old); err != nil {
		t.Fatal(err)
	}
	listener, endpoint := listenSocket(t, runDir)
	runValue.SupervisorIdentity.IPCEndpoint = endpoint
	if err := writeJSON(filepath.Join(runDir, "run.json"), runValue); err != nil {
		t.Fatal(err)
	}
	live := old
	live.UpdatedAt = time.Now().UTC()
	serveOne(t, listener, supervisor.Response{SchemaVersion: supervisor.SchemaVersion, OK: true, Result: live})

	view, err := LoadSnapshot(runDir)
	if err != nil {
		t.Fatal(err)
	}
	if view.Meta.Source != ReadSourceIPC || view.Meta.Degraded || view.Snapshot.UpdatedAt.Equal(old.UpdatedAt) {
		t.Fatalf("expected live IPC view, got %+v", view)
	}
}

func TestLoadSnapshotPIDReuseNeverUsesSocket(t *testing.T) {
	runDir := t.TempDir()
	runValue := domain.Run{RunID: "run-1", ProjectID: "project-1", Status: domain.RunRunning, SupervisorIdentity: &domain.SupervisorIdentity{PID: os.Getpid(), ProcessStartToken: "not-the-current-start-token"}}
	snapshot := supervisor.Snapshot{SchemaVersion: supervisor.SchemaVersion, Run: runValue, UpdatedAt: time.Now().UTC()}
	if err := writeJSON(filepath.Join(runDir, "run.json"), runValue); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(runDir, "state.json"), snapshot); err != nil {
		t.Fatal(err)
	}
	view, err := LoadSnapshot(runDir)
	if err != nil {
		t.Fatal(err)
	}
	if view.Meta.SupervisorIdentity != IdentityMismatched || view.Meta.IdentityValid || view.Meta.Source != ReadSourceDisk || !view.Meta.Degraded {
		t.Fatalf("unexpected PID reuse metadata: %+v", view.Meta)
	}
}

func TestLoadEventsRepairsDiskTailInDegradedMode(t *testing.T) {
	runDir := t.TempDir()
	store := event.NewStore(filepath.Join(runDir, "events.jsonl"), "run-1", 0)
	if _, err := store.Append(event.Input{Source: "test", Type: event.RunStarted}); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(runDir, "events.jsonl"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString(`{"schema_version":"v1alpha1"`)
	_ = file.Close()

	view, err := LoadEvents(runDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if view.Meta.Source != ReadSourceDisk || !view.Meta.Degraded || !view.Meta.TailRepaired || len(view.Events) != 1 {
		t.Fatalf("unexpected repaired event view: %+v", view)
	}
}

func TestWaitReturnsCommunicationForNonTerminalRealRunWithoutSupervisor(t *testing.T) {
	runDir := t.TempDir()
	runValue := domain.Run{RunID: "run-1", ProjectID: "project-1", Status: domain.RunRunning, SupervisorIdentity: &domain.SupervisorIdentity{PID: 99999999, ProcessStartToken: "dead"}}
	snapshot := supervisor.Snapshot{SchemaVersion: supervisor.SchemaVersion, Run: runValue, UpdatedAt: time.Now().UTC()}
	if err := writeJSON(filepath.Join(runDir, "run.json"), runValue); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(runDir, "state.json"), snapshot); err != nil {
		t.Fatal(err)
	}
	view, err := Wait(runDir, supervisor.WaitParams{For: "run", Timeout: time.Millisecond})
	if !errors.Is(err, ErrSupervisorUnavailable) || !view.Meta.Degraded || view.Snapshot.Run.Status != domain.RunRunning {
		t.Fatalf("expected degraded communication result, view=%+v err=%v", view, err)
	}
}

func TestWaitReturnsTerminalDiskResultWithoutSupervisor(t *testing.T) {
	runDir := t.TempDir()
	runValue := domain.Run{RunID: "run-1", ProjectID: "project-1", Status: domain.RunCompleted, SupervisorIdentity: &domain.SupervisorIdentity{PID: 99999999, ProcessStartToken: "dead"}}
	snapshot := supervisor.Snapshot{SchemaVersion: supervisor.SchemaVersion, Run: runValue, UpdatedAt: time.Now().UTC()}
	if err := writeJSON(filepath.Join(runDir, "run.json"), runValue); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(runDir, "state.json"), snapshot); err != nil {
		t.Fatal(err)
	}
	view, err := Wait(runDir, supervisor.WaitParams{For: "run"})
	if err != nil || !view.Matched || view.Meta.Source != ReadSourceDisk || !view.Meta.Degraded {
		t.Fatalf("expected terminal disk result, view=%+v err=%v", view, err)
	}
}

func writeJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func listenSocket(t *testing.T, runDir string) (net.Listener, string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(supervisor.SocketPath(runDir)), 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", supervisor.SocketPath(runDir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close(); _ = os.Remove(supervisor.SocketPath(runDir)) })
	return listener, supervisor.SocketPath(runDir)
}

func serveOne(t *testing.T, listener net.Listener, response supervisor.Response) {
	t.Helper()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = bufio.NewReader(conn).ReadBytes('\n')
		_ = json.NewEncoder(conn).Encode(response)
	}()
}
