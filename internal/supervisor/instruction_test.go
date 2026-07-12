package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/message"
)

func newInstructionService(t *testing.T, caps adapter.Capabilities, withSession bool) (*Service, *fake.Adapter, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	harness := fake.New(caps)
	service := &Service{
		snapshot: Snapshot{
			SchemaVersion: SchemaVersion,
			Run:           domain.Run{RunID: "run-1", ProjectID: "p1", Status: domain.RunRunning},
		},
		messages:         store,
		messageIndex:     map[string]message.Message{},
		router:           router,
		active:           map[string]activeWorker{},
		acceptingWork:    true,
		fatalPersistence: make(chan error, 1),
	}
	if withSession {
		session, err := harness.StartSession(context.Background(), adapter.StartRequest{
			RunID: "run-1", TaskID: "task-a", WorkerID: "worker-a",
			ProjectRoot: t.TempDir(), Contract: "contract", Scenario: "active_steer",
		})
		if err != nil {
			t.Fatal(err)
		}
		service.active["task-a"] = activeWorker{
			adapter:   harness,
			sessionID: session.NativeSessionID,
			cancel:    func() {},
		}
	}
	return service, harness, path
}

func journalStatuses(t *testing.T, path string) []message.Status {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var statuses []message.Status
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var value message.Message
		if err := json.Unmarshal([]byte(line), &value); err != nil {
			t.Fatal(err)
		}
		statuses = append(statuses, value.Status)
	}
	return statuses
}

func TestSendInstructionImmediateSuccessQueuedThenDelivered(t *testing.T) {
	service, _, path := newInstructionService(t, adapter.Capabilities{
		StructuredStream: true, SteerActiveTurn: true, BidirectionalStream: true,
	}, true)
	result, err := service.SendInstruction(context.Background(), "task-a", "steer now")
	if err != nil {
		t.Fatal(err)
	}
	if result.Mode != adapter.DeliveryImmediate || result.MessageID == "" {
		t.Fatalf("result=%+v", result)
	}
	statuses := journalStatuses(t, path)
	if len(statuses) < 2 || statuses[0] != message.Queued || statuses[1] != message.Delivered {
		t.Fatalf("journal statuses=%v", statuses)
	}
	got, ok := service.router.Get(result.MessageID)
	if !ok || got.Status != message.Delivered {
		t.Fatalf("router state=%+v ok=%v", got, ok)
	}
}

type failingSteerAdapter struct {
	*fake.Adapter
}

func (a failingSteerAdapter) SteerActiveTurn(context.Context, string, string) (adapter.DeliveryResult, error) {
	return adapter.DeliveryResult{Mode: adapter.DeliveryImmediate}, errors.New("steer exploded")
}

func TestSendInstructionImmediateFailureQueuedThenFailed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	inner := fake.New(adapter.Capabilities{SteerActiveTurn: true, BidirectionalStream: true, StructuredStream: true})
	session, err := inner.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-1", TaskID: "task-a", WorkerID: "worker-a",
		ProjectRoot: t.TempDir(), Contract: "contract", Scenario: "active_steer",
	})
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{
		snapshot:         Snapshot{Run: domain.Run{RunID: "run-1"}},
		messages:         store,
		messageIndex:     map[string]message.Message{},
		router:           router,
		active:           map[string]activeWorker{"task-a": {adapter: failingSteerAdapter{Adapter: inner}, sessionID: session.NativeSessionID, cancel: func() {}}},
		acceptingWork:    true,
		fatalPersistence: make(chan error, 1),
	}
	result, err := service.SendInstruction(context.Background(), "task-a", "fail please")
	if err == nil || !strings.Contains(err.Error(), "steer exploded") {
		t.Fatalf("expected steer error, got %v", err)
	}
	if result.MessageID == "" {
		t.Fatal("broker message id required on failure")
	}
	statuses := journalStatuses(t, path)
	if len(statuses) < 2 || statuses[0] != message.Queued || statuses[1] != message.Failed {
		t.Fatalf("journal statuses=%v", statuses)
	}
	got, _ := router.Get(result.MessageID)
	if got.Status != message.Failed || got.Error == "" {
		t.Fatalf("failed message not persisted: %+v", got)
	}
}

func TestSendInstructionNoActiveWorkerPersistsUnsupported(t *testing.T) {
	service, _, path := newInstructionService(t, adapter.Capabilities{}, false)
	result, err := service.SendInstruction(context.Background(), "task-a", "hello")
	if !errors.Is(err, adapter.ErrUnsupported) {
		t.Fatalf("expected unsupported, got %v", err)
	}
	if result.Mode != adapter.DeliveryUnsupported || result.MessageID == "" {
		t.Fatalf("result=%+v", result)
	}
	statuses := journalStatuses(t, path)
	if len(statuses) < 2 || statuses[0] != message.Queued || statuses[1] != message.Failed {
		t.Fatalf("journal statuses=%v", statuses)
	}
	got, _ := service.router.Get(result.MessageID)
	if got.Status != message.Failed || got.DeliveryMode != message.DeliveryUnsupported {
		t.Fatalf("router=%+v", got)
	}
}

func TestSendInstructionNextTurnAndResumeStayQueued(t *testing.T) {
	nextService, _, nextPath := newInstructionService(t, adapter.Capabilities{
		BidirectionalStream: true, StructuredStream: true,
	}, true)
	nextResult, err := nextService.SendInstruction(context.Background(), "task-a", "later")
	if err != nil {
		t.Fatal(err)
	}
	if nextResult.Mode != adapter.DeliveryNextTurn {
		t.Fatalf("mode=%s", nextResult.Mode)
	}
	if statuses := journalStatuses(t, nextPath); len(statuses) != 1 || statuses[0] != message.Queued {
		t.Fatalf("next_turn statuses=%v", statuses)
	}
	got, _ := nextService.router.Get(nextResult.MessageID)
	if got.Status != message.Queued || got.DeliveryMode != message.DeliveryNextTurn {
		t.Fatalf("next_turn router=%+v", got)
	}

	resumeService, _, resumePath := newInstructionService(t, adapter.Capabilities{
		ResumeSession: true, StructuredStream: true,
	}, true)
	resumeResult, err := resumeService.SendInstruction(context.Background(), "task-a", "on resume")
	if err != nil {
		t.Fatal(err)
	}
	if resumeResult.Mode != adapter.DeliveryResume {
		t.Fatalf("mode=%s", resumeResult.Mode)
	}
	if statuses := journalStatuses(t, resumePath); len(statuses) != 1 || statuses[0] != message.Queued {
		t.Fatalf("resume statuses=%v", statuses)
	}
}

type panicOnSendAdapter struct {
	*fake.Adapter
	called *bool
}

func (a panicOnSendAdapter) SteerActiveTurn(context.Context, string, string) (adapter.DeliveryResult, error) {
	*a.called = true
	panic("adapter must not be called when enqueue fails")
}

func (a panicOnSendAdapter) SendMessage(context.Context, string, string) (adapter.DeliveryResult, error) {
	*a.called = true
	panic("adapter must not be called when enqueue fails")
}

func TestSendInstructionStoreFailureDoesNotCallAdapter(t *testing.T) {
	dir := t.TempDir()
	blocked := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(blocked, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := message.NewStore(filepath.Join(blocked, "messages.jsonl"))
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	inner := fake.New(adapter.Capabilities{SteerActiveTurn: true, StructuredStream: true})
	session, err := inner.StartSession(context.Background(), adapter.StartRequest{
		RunID: "run-1", TaskID: "task-a", WorkerID: "worker-a",
		ProjectRoot: t.TempDir(), Contract: "contract", Scenario: "active_steer",
	})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	service := &Service{
		snapshot:         Snapshot{Run: domain.Run{RunID: "run-1"}},
		messages:         store,
		messageIndex:     map[string]message.Message{},
		router:           router,
		active:           map[string]activeWorker{"task-a": {adapter: panicOnSendAdapter{Adapter: inner, called: &called}, sessionID: session.NativeSessionID, cancel: func() {}}},
		acceptingWork:    true,
		fatalPersistence: make(chan error, 1),
	}
	if _, err := service.SendInstruction(context.Background(), "task-a", "never"); err == nil {
		t.Fatal("expected store failure")
	}
	if called {
		t.Fatal("adapter was called despite store failure")
	}
	time.Sleep(time.Millisecond)
}
