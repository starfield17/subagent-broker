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

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/wave"
)

// TestApproveScopeExpansionDoesNotDeadlock exercises the full Service.ResolveMessage
// production path for an allowed ScopeExpansionRequest. A hang here means the
// mutation callback re-entered s.mu (Go mutex is non-reentrant).
func TestApproveScopeExpansionDoesNotDeadlock(t *testing.T) {
	service := loadTestService(t)

	// Put the task into a running worker projection so waiting/recompute is meaningful.
	service.mu.Lock()
	for i := range service.snapshot.Tasks {
		if string(service.snapshot.Tasks[i].Task.TaskID) != "task-a" {
			continue
		}
		service.snapshot.Tasks[i].Task.Status = state.TaskRunning
		service.snapshot.Tasks[i].Dimensions.Task = state.TaskRunning
		service.snapshot.Tasks[i].BlockKind = BlockKindNone
		service.snapshot.Tasks[i].Worker = &domain.WorkerSession{
			WorkerID: "worker-a", Attempt: 1,
		}
	}
	originalScope := append([]string(nil), service.snapshot.Tasks[0].Task.WriteScope...)
	service.mu.Unlock()
	if len(originalScope) == 0 {
		t.Fatal("fixture must declare an original WriteScope")
	}

	requested := "expanded-output.txt"
	payload := message.ScopeRequestPayload{
		RequestedScope:       []string{requested},
		Reason:               "need write access for expanded deliverable",
		Consequence:          "cannot finish without the extra path",
		PartialModifications: "none",
	}

	type waitResult struct {
		res message.Resolution
		id  string
		err error
	}
	waited := make(chan waitResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		res, id, err := service.RequestMessage(ctx, "task-a", "worker-a",
			message.ScopeExpansionRequest, message.Scope, payload)
		waited <- waitResult{res: res, id: id, err: err}
	}()

	msgID := waitPendingDecision(t, service)
	resolution := message.Resolution{Decision: message.DecisionPayload{Allowed: true}}

	// Bounded deadline: deadlock regression must fail promptly, not hang the suite.
	done := make(chan error, 1)
	go func() {
		done <- service.ResolveMessage(msgID, resolution)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ResolveMessage: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ResolveMessage hung — likely approveScope mutex re-entry deadlock")
	}

	select {
	case got := <-waited:
		if got.err != nil {
			t.Fatalf("RequestMessage: %v", got.err)
		}
		if !got.res.Decision.Allowed {
			t.Fatalf("expected Allowed=true, got %+v", got.res)
		}
		if got.id != msgID {
			t.Fatalf("message id mismatch: waiter=%s resolve=%s", got.id, msgID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RequestMessage did not resume after approve")
	}

	// Message is exactly Answered.
	item, ok := service.router.Get(msgID)
	if !ok {
		t.Fatal("resolved message missing from router")
	}
	if item.Status != message.Answered {
		t.Fatalf("status=%s want Answered", item.Status)
	}

	// Requested scope appears exactly once; original scope remains.
	snap := service.Snapshot()
	var task TaskState
	found := false
	for _, ts := range snap.Tasks {
		if string(ts.Task.TaskID) == "task-a" {
			task = ts
			found = true
			break
		}
	}
	if !found {
		t.Fatal("task-a missing from snapshot")
	}
	assertScopeOnce(t, task.Task.WriteScope, requested)
	for _, orig := range originalScope {
		assertScopeOnce(t, task.Task.WriteScope, orig)
	}

	// Durable Task contract contains the expanded scope.
	contractPath := filepath.Join(service.paths.Tasks, "task-a", "contract.md")
	contractRaw, err := os.ReadFile(contractPath)
	if err != nil {
		t.Fatalf("read contract: %v", err)
	}
	if !strings.Contains(string(contractRaw), requested) {
		t.Fatalf("contract missing expanded scope %q:\n%s", requested, contractRaw)
	}

	// Wave preflight artifact was updated successfully.
	preflightPath := filepath.Join(service.paths.Waves, string(task.Task.WaveID), "preflight.json")
	preflightRaw, err := os.ReadFile(preflightPath)
	if err != nil {
		t.Fatalf("read preflight: %v", err)
	}
	var preflight wave.PreflightResult
	if err := json.Unmarshal(preflightRaw, &preflight); err != nil {
		t.Fatalf("decode preflight: %v", err)
	}
	if !preflight.Allowed {
		t.Fatalf("preflight not allowed: %+v", preflight)
	}

	// Task is not left blocked waiting on a decision.
	if task.Task.Status == state.TaskBlocked && task.BlockKind == BlockKindWaitingMessage {
		t.Fatalf("task left blocked on message: %+v", task)
	}

	// Identical resolution is idempotent and must not duplicate scope entries.
	if err := service.ResolveMessage(msgID, resolution); err != nil {
		t.Fatalf("identical resolve retry: %v", err)
	}
	afterRetry := service.Snapshot()
	for _, ts := range afterRetry.Tasks {
		if string(ts.Task.TaskID) != "task-a" {
			continue
		}
		assertScopeOnce(t, ts.Task.WriteScope, requested)
		for _, orig := range originalScope {
			assertScopeOnce(t, ts.Task.WriteScope, orig)
		}
	}

	// Conflicting deny after allow must reject without rolling back scope.
	denyErr := service.ResolveMessage(msgID, message.Resolution{
		Decision: message.DecisionPayload{Allowed: false, Reason: "conflict"},
	})
	if denyErr == nil {
		t.Fatal("conflicting deny must fail")
	}
	var conflict *message.ErrResolutionConflict
	if !errors.As(denyErr, &conflict) {
		t.Fatalf("expected ErrResolutionConflict, got %v", denyErr)
	}
	final := service.Snapshot()
	for _, ts := range final.Tasks {
		if string(ts.Task.TaskID) != "task-a" {
			continue
		}
		assertScopeOnce(t, ts.Task.WriteScope, requested)
		for _, orig := range originalScope {
			assertScopeOnce(t, ts.Task.WriteScope, orig)
		}
	}
	// Message remains Answered (no rollback of terminal resolution).
	if latest, ok := service.router.Get(msgID); !ok || latest.Status != message.Answered {
		t.Fatalf("message must remain Answered after conflict, got %+v ok=%v", latest, ok)
	}
}

func waitPendingDecision(t *testing.T, service *Service) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		items := service.Inbox(false)
		if len(items) > 0 {
			return items[0].MessageID
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("pending decision did not appear")
	return ""
}

func assertScopeOnce(t *testing.T, writeScope []string, want string) {
	t.Helper()
	count := 0
	for _, entry := range writeScope {
		if entry == want {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("scope %q count=%d in %v (want exactly 1)", want, count, writeScope)
	}
}
