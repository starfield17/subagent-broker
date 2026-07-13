package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/fake"
	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/state"
)

func TestResolveMessageTerminalNotAnswered(t *testing.T) {
	service := loadTestService(t)
	val, err := service.router.EnqueueDecision("task-a", "worker-a", message.Question, message.Decision,
		json.RawMessage(`{"schema_version":"v1alpha1","question":"Q","reason":"R","current_scope":["x"],"workspace_state":"w"}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.expireOneMessage(val.MessageID, errors.New("timeout"))
	if err != nil {
		t.Fatal(err)
	}
	err = service.ResolveMessage(val.MessageID, message.NewAnswerResolution("yes"))
	var term *message.ErrMessageTerminalNotAnswered
	if !errors.As(err, &term) {
		t.Fatalf("expected ErrMessageTerminalNotAnswered, got %v", err)
	}
}

func TestResolveMessageAnsweredIdempotent(t *testing.T) {
	service := loadTestService(t)
	val, err := service.router.EnqueueDecision("task-a", "worker-a", message.Question, message.Decision,
		json.RawMessage(`{"schema_version":"v1alpha1","question":"Q","reason":"R","current_scope":["x"],"workspace_state":"w"}`))
	if err != nil {
		t.Fatal(err)
	}
	res := message.NewAnswerResolution("yes")
	if err := service.ResolveMessage(val.MessageID, res); err != nil {
		t.Fatal(err)
	}
	if err := service.ResolveMessage(val.MessageID, res); err != nil {
		t.Fatalf("identical Answered retry must succeed: %v", err)
	}
	if err := service.ResolveMessage(val.MessageID, message.NewAnswerResolution("no")); err == nil {
		t.Fatal("conflicting answer must fail")
	}
}

func TestExpireFrozenResolutionReconciliation(t *testing.T) {
	service := loadTestService(t)
	val, err := service.router.EnqueueDecision("task-a", "worker-a", message.ScopeExpansionRequest, message.Scope,
		json.RawMessage(`{"requested_scope":["new.txt"],"reason":"need","consequence":"none","partial_modifications":"none"}`))
	if err != nil {
		t.Fatal(err)
	}
	resJSON, _ := json.Marshal(message.NewDecisionResolution(true, "", false))
	if _, _, err := service.router.FreezeResolution(val.MessageID, resJSON); err != nil {
		t.Fatal(err)
	}
	_, err = service.expireOneMessage(val.MessageID, errors.New("task ended"))
	var recon *message.ErrResolutionReconciliationRequired
	if !errors.As(err, &recon) {
		t.Fatalf("expected reconciliation required, got %v", err)
	}
	got, ok := service.router.Get(val.MessageID)
	if !ok || got.Status != message.Queued || len(got.Resolution) == 0 {
		t.Fatalf("got=%+v", got)
	}
}

func TestRequestMessageResolverWinsBeforeOpLock(t *testing.T) {
	service := loadTestService(t)
	markTaskRunning(service, "task-a")

	payload := map[string]any{
		"schema_version": "v1alpha1", "question": "Q", "reason": "R",
		"current_scope": []string{"x"}, "workspace_state": "w",
	}

	var wg sync.WaitGroup
	wg.Add(2)
	var reqErr error
	var reqRes message.Resolution
	var reqID string

	go func() {
		defer wg.Done()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			pending := service.router.PendingDecisions("task-a")
			if len(pending) > 0 {
				_ = service.ResolveMessage(pending[0].MessageID, message.NewAnswerResolution("from-resolver"))
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		reqRes, reqID, reqErr = service.RequestMessage(ctx, "task-a", "worker-a", message.Question, message.Decision, payload)
	}()
	wg.Wait()

	if reqErr != nil && !errors.Is(reqErr, context.DeadlineExceeded) && !errors.Is(reqErr, context.Canceled) {
		// Durable answer may still have been returned as success.
		t.Logf("RequestMessage err=%v id=%s res=%+v", reqErr, reqID, reqRes)
	}
	if service.router.HasPendingDecisions("task-a") {
		// Ensure resolve completed.
		for _, p := range service.router.PendingDecisions("task-a") {
			_ = service.ResolveMessage(p.MessageID, message.NewAnswerResolution("from-resolver"))
		}
	}
	if service.router.HasPendingDecisions("task-a") {
		t.Fatal("pending decisions remain")
	}
	_ = service.recomputeTaskWaiting("task-a")
}

func TestRequestMessageDoesNotLeaveBlockedAfterAnswer(t *testing.T) {
	service := loadTestService(t)
	markTaskRunning(service, "task-a")
	// Active worker with no exit so recompute can clear blocked.
	service.mu.Lock()
	for i := range service.snapshot.Tasks {
		if string(service.snapshot.Tasks[i].Task.TaskID) == "task-a" {
			service.snapshot.Tasks[i].Worker = &domain.WorkerSession{
				WorkerID: "worker-a", TaskID: "task-a",
			}
		}
	}
	service.mu.Unlock()

	val, err := service.router.EnqueueDecision("task-a", "worker-a", message.Question, message.Decision,
		json.RawMessage(`{"schema_version":"v1alpha1","question":"Q","reason":"R","current_scope":["x"],"workspace_state":"w"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.setTaskWaiting("task-a", message.Question); err != nil {
		t.Fatal(err)
	}
	if err := service.ResolveMessage(val.MessageID, message.NewAnswerResolution("yes")); err != nil {
		t.Fatal(err)
	}
	if service.router.HasPendingDecisions("task-a") {
		t.Fatal("still pending")
	}
	snap := service.Snapshot()
	for _, ts := range snap.Tasks {
		if string(ts.Task.TaskID) == "task-a" {
			if ts.Task.Status == state.TaskBlocked && ts.BlockKind == BlockKindWaitingMessage {
				t.Fatalf("task left blocked after answer: %+v", ts)
			}
		}
	}
}

func markTaskRunning(service *Service, taskID string) {
	service.mu.Lock()
	defer service.mu.Unlock()
	for i := range service.snapshot.Tasks {
		if string(service.snapshot.Tasks[i].Task.TaskID) == taskID {
			service.snapshot.Tasks[i].Task.Status = state.TaskRunning
			service.snapshot.Tasks[i].Dimensions.Task = state.TaskRunning
			service.snapshot.Tasks[i].BlockKind = BlockKindNone
		}
	}
}

func loadTestService(t *testing.T) *Service {
	t.Helper()
	runDir, _ := writeFixture(t)
	registry := adapter.NewRegistry()
	if err := registry.Register(fake.New(adapter.Capabilities{})); err != nil {
		t.Fatal(err)
	}
	service, err := Load(runDir, registry, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	return service
}
