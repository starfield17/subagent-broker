package supervisor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/storage"
)

func TestResolveOneQuestionKeepsBlockedWhenAnotherPending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{
		paths: storagePaths(t),
		snapshot: Snapshot{
			Run: domain.Run{RunID: "run-1"},
			Tasks: []TaskState{{
				Task:       domain.Task{TaskID: "task-a", WriteScope: []string{"a/**"}},
				Worker:     &domain.WorkerSession{WorkerID: "w1", TaskID: "task-a"},
				Dimensions: state.Dimensions{Task: state.TaskRunning, Process: state.ProcessAlive},
			}},
		},
		messages:         store,
		messageIndex:     map[string]message.Message{},
		router:           router,
		pending:          map[string]*pendingWaiter{},
		acceptingWork:    true,
		fatalPersistence: make(chan error, 1),
	}

	// Enqueue two decisions directly (avoids concurrent RequestMessage races in the test harness).
	raw1, _ := json.Marshal(message.QuestionEnvelope{SchemaVersion: SchemaVersion, Question: "Q1", Reason: "r1", CurrentScope: []string{"a/**"}, WorkspaceState: "ok"})
	raw2, _ := json.Marshal(message.QuestionEnvelope{SchemaVersion: SchemaVersion, Question: "Q2", Reason: "r2", CurrentScope: []string{"a/**"}, WorkspaceState: "ok"})
	m1, err := router.EnqueueDecision("task-a", "w1", message.Question, message.Decision, raw1)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := router.EnqueueDecision("task-a", "w1", message.Question, message.Decision, raw2)
	if err != nil {
		t.Fatal(err)
	}
	taskDir := filepath.Join(service.paths.Tasks, "task-a")
	_ = message.PublishQuestionProjection(taskDir, m1.MessageID, "task-a", service.questionEnvelopeFor(m1), true)
	_ = message.PublishQuestionProjection(taskDir, m2.MessageID, "task-a", service.questionEnvelopeFor(m2), false)
	_ = service.setTaskWaiting("task-a", message.Question)
	service.syncMessageProjection(m1)
	service.syncMessageProjection(m2)

	if err := service.ResolveMessage(m1.MessageID, message.Resolution{Answer: "A1"}); err != nil {
		t.Fatal(err)
	}
	if !service.router.HasPendingDecisions("task-a") {
		t.Fatal("should still have pending after one answer")
	}
	if service.Snapshot().Tasks[0].Task.Status != state.TaskBlocked {
		t.Fatalf("status=%s want blocked", service.Snapshot().Tasks[0].Task.Status)
	}
	// Top-level projection still present for remaining question.
	if _, err := os.Stat(filepath.Join(taskDir, "question.md")); err != nil {
		t.Fatalf("top-level projection missing: %v", err)
	}
	// Historical archive for m1 must remain.
	if _, err := os.Stat(filepath.Join(taskDir, "questions", m1.MessageID, "question.md")); err != nil {
		t.Fatalf("archive missing: %v", err)
	}
	// Resolve last → clear waiting.
	if err := service.ResolveMessage(m2.MessageID, message.Resolution{Answer: "A2"}); err != nil {
		t.Fatal(err)
	}
	if service.router.HasPendingDecisions("task-a") {
		t.Fatal("no pending expected")
	}
	if _, err := os.Stat(filepath.Join(taskDir, "question.md")); !os.IsNotExist(err) {
		t.Fatalf("top-level should be cleared, err=%v", err)
	}
	// Archives remain.
	if _, err := os.Stat(filepath.Join(taskDir, "questions", m2.MessageID, "question.md")); err != nil {
		t.Fatal(err)
	}
}

func TestExpireTaskMessagesOnTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{
		paths: storagePaths(t),
		snapshot: Snapshot{
			Run: domain.Run{RunID: "run-1"},
			Tasks: []TaskState{{
				Task: domain.Task{TaskID: "task-a", WriteScope: []string{"a/**"}, Status: state.TaskRunning},
			}},
		},
		messages: store, messageIndex: map[string]message.Message{}, router: router,
		pending: map[string]*pendingWaiter{}, acceptingWork: true, fatalPersistence: make(chan error, 1),
	}
	_, err = router.EnqueueDecision("task-a", "w1", message.Question, message.Decision, json.RawMessage(`{"schema_version":"v1alpha1","question":"q","reason":"r","current_scope":["a/**"],"workspace_state":"ok"}`))
	if err != nil {
		t.Fatal(err)
	}
	expired, err := service.expireTaskMessages("task-a", "task failed")
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 || expired[0].Status != message.Expired {
		t.Fatalf("%+v", expired)
	}
	if service.router.HasPendingDecisions("task-a") {
		t.Fatal("pending should be empty")
	}
}

func TestQuestionEnvelopeUsesWriteScope(t *testing.T) {
	service := &Service{
		snapshot: Snapshot{Tasks: []TaskState{{
			Task: domain.Task{TaskID: "task-a", WriteScope: []string{"internal/x/**", "go.mod"}},
		}}},
	}
	env := service.questionEnvelopeFor(message.Message{
		Type: message.Question, TaskID: "task-a",
		Payload: json.RawMessage(`{"schema_version":"v1alpha1","question":"q","reason":"r","current_scope":["See Task Contract"],"workspace_state":"ok"}`),
	})
	if len(env.CurrentScope) != 2 || env.CurrentScope[0] != "internal/x/**" {
		t.Fatalf("scope=%v", env.CurrentScope)
	}
}

func TestInstructionOutboxOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.jsonl")
	store := message.NewStore(path)
	router, err := message.NewRouter(message.NewRouterOptions{RunID: "run-1", Store: store})
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{router: router, messages: store, messageIndex: map[string]message.Message{}, acceptingWork: true, fatalPersistence: make(chan error, 1)}
	a, _ := router.EnqueueInstruction("task-a", "", "one", message.DeliveryNextTurn)
	b, _ := router.EnqueueInstruction("task-a", "", "two", message.DeliveryNextTurn)
	pending := service.router.PendingInstructions("task-a", message.DeliveryNextTurn)
	if len(pending) != 2 || pending[0].MessageID != a.MessageID || pending[1].MessageID != b.MessageID {
		t.Fatalf("%+v", pending)
	}
}

func storagePaths(t *testing.T) storage.RunPaths {
	t.Helper()
	root := t.TempDir()
	tasks := filepath.Join(root, "tasks")
	_ = os.MkdirAll(filepath.Join(tasks, "task-a", "questions"), 0o700)
	return storage.RunPaths{Root: root, Tasks: tasks}
}

// silence unused import if time removed
var _ = time.Second
var _ = context.Background
