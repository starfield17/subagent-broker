package supervisor

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/state"
	workerpkg "github.com/vnai/subagent-broker/internal/worker"
)

func TestClassifyRecoveryVariants(t *testing.T) {
	base := TaskState{
		Task: domain.Task{TaskID: "task-a", Status: state.TaskRunning},
		Worker: &domain.WorkerSession{
			WorkerID: "w1", TaskID: "task-a", PID: 42, ProcessStartToken: "tok",
			NativeSessionID: "sess-1",
		},
		Dimensions: state.Dimensions{Process: state.ProcessAlive, Task: state.TaskRunning},
	}

	// Terminal task.
	term := base
	term.Task.Status = state.TaskFailed
	if d := ClassifyRecovery(term, process.Identity{}, nil, true); d.Class != RecoveryAlreadyTerminal {
		t.Fatalf("terminal=%s", d.Class)
	}

	// Alive orphaned.
	if d := ClassifyRecovery(base, process.Identity{PID: 42, StartToken: "tok", ProcessGroupToken: "9"}, nil, true); d.Class != RecoveryAliveOrphaned {
		t.Fatalf("orphan=%s", d.Class)
	}

	// PID reuse — must not look like same process.
	if d := ClassifyRecovery(base, process.Identity{PID: 42, StartToken: "other", ProcessGroupToken: "9"}, nil, true); d.Class != RecoveryPIDReused {
		t.Fatalf("reuse=%s", d.Class)
	}

	// Exited resumable.
	if d := ClassifyRecovery(base, process.Identity{}, os.ErrNotExist, true); d.Class != RecoveryExitedResumable || d.ResumeSessionID != "sess-1" {
		t.Fatalf("resumable=%+v", d)
	}

	// Exited unresumable (no resume capability).
	if d := ClassifyRecovery(base, process.Identity{}, os.ErrNotExist, false); d.Class != RecoveryExitedUnresumable {
		t.Fatalf("unresumable=%s", d.Class)
	}

	// Missing worker.
	missing := TaskState{Task: domain.Task{TaskID: "task-b", Status: state.TaskRunning}}
	if d := ClassifyRecovery(missing, process.Identity{}, nil, true); d.Class != RecoveryMissingIdentity {
		t.Fatalf("missing=%s", d.Class)
	}

	// Final blocked is terminal.
	blocked := base
	blocked.Task.Status = state.TaskBlocked
	blocked.BlockKind = BlockKindFinal
	if d := ClassifyRecovery(blocked, process.Identity{}, nil, true); d.Class != RecoveryAlreadyTerminal {
		t.Fatalf("final blocked=%s", d.Class)
	}
}

func TestAttemptHistoryPreservedOnSecondBegin(t *testing.T) {
	task := domain.Task{TaskID: "task-a"}
	first, err := workerpkg.Begin(task, nil, workerpkg.AttemptFresh, domain.WorkerSession{WorkerID: "w1"}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	finished, err := workerpkg.Finish(first, workerpkg.AttemptExited, "done", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	second, err := workerpkg.Begin(task, []workerpkg.Attempt{finished}, workerpkg.AttemptRecoveryResume, domain.WorkerSession{WorkerID: "w2", NativeSessionID: "s1"}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if second.Number != 2 {
		t.Fatalf("number=%d", second.Number)
	}
	// History slice is independent.
	history := []workerpkg.Attempt{finished, second}
	if history[0].Worker.WorkerID != "w1" || history[1].Worker.WorkerID != "w2" {
		t.Fatalf("history clobbered: %+v", history)
	}
}

func TestApplyResultEnvelopeDoesNotMapFailedToPartial(t *testing.T) {
	appender := &fakeEventAppender{}
	service := newCommitService(appender, func(Snapshot) error { return nil })
	service.snapshot.Tasks = []TaskState{{
		Task:       domain.Task{TaskID: "task-a", Status: state.TaskRunning},
		Worker:     &domain.WorkerSession{WorkerID: "w1", TaskID: "task-a"},
		Dimensions: state.Dimensions{Task: state.TaskRunning, Process: state.ProcessExited},
	}}
	runtime := service.snapshot.Tasks[0]
	if err := service.applyResultEnvelope(&runtime, report.Envelope{
		SchemaVersion: report.SchemaVersion, TaskID: "task-a", WorkerID: "w1",
		Status: report.StatusFailed, Summary: "failed",
	}, "w1"); err != nil {
		t.Fatal(err)
	}
	snap := service.Snapshot()
	if snap.Tasks[0].Task.Status != state.TaskFailed {
		t.Fatalf("status=%s want failed", snap.Tasks[0].Task.Status)
	}
	if snap.Tasks[0].Task.Status == state.TaskVerifiedPartial {
		t.Fatal("failed must not become verified_partial")
	}
}

func TestApplyResultEnvelopeBlockedIsFinal(t *testing.T) {
	appender := &fakeEventAppender{}
	service := newCommitService(appender, func(Snapshot) error { return nil })
	service.snapshot.Tasks = []TaskState{{
		Task:       domain.Task{TaskID: "task-a", Status: state.TaskRunning},
		Worker:     &domain.WorkerSession{WorkerID: "w1", TaskID: "task-a"},
		Dimensions: state.Dimensions{Task: state.TaskRunning},
	}}
	runtime := service.snapshot.Tasks[0]
	if err := service.applyResultEnvelope(&runtime, report.Envelope{
		SchemaVersion: report.SchemaVersion, TaskID: "task-a", WorkerID: "w1",
		Status: report.StatusBlocked, Summary: "blocked",
	}, "w1"); err != nil {
		t.Fatal(err)
	}
	snap := service.Snapshot()
	if snap.Tasks[0].Task.Status != state.TaskBlocked || snap.Tasks[0].BlockKind != BlockKindFinal {
		t.Fatalf("got status=%s block=%s", snap.Tasks[0].Task.Status, snap.Tasks[0].BlockKind)
	}
}

func TestMigrateAttemptsFromLegacyWorker(t *testing.T) {
	ts := TaskState{
		Worker:     &domain.WorkerSession{WorkerID: "w1", Attempt: 1, AttemptMode: string(workerpkg.AttemptFresh)},
		Dimensions: state.Dimensions{Process: state.ProcessExited},
	}
	migrateAttempts(&ts)
	if len(ts.Attempts) != 1 || ts.Attempts[0].Worker.WorkerID != "w1" {
		t.Fatalf("migrate failed: %+v", ts.Attempts)
	}
	if ts.Attempts[0].Outcome != workerpkg.AttemptExited {
		t.Fatalf("outcome=%s", ts.Attempts[0].Outcome)
	}
}

func TestClassifyDoesNotSignalOnPIDReuse(t *testing.T) {
	// Documentation-level: classification is pure and never signals.
	// Ensure reuse path never returns alive_orphaned (which would imply process ownership).
	runtime := TaskState{
		Task:   domain.Task{TaskID: "t", Status: state.TaskRunning},
		Worker: &domain.WorkerSession{PID: 7, ProcessStartToken: "a", NativeSessionID: "s"},
	}
	d := ClassifyRecovery(runtime, process.Identity{PID: 7, StartToken: "b", ProcessGroupToken: "1"}, nil, true)
	if d.Class != RecoveryPIDReused {
		t.Fatal(d.Class)
	}
}

func TestFinishAttemptIsIdempotentError(t *testing.T) {
	a, err := workerpkg.Begin(domain.Task{TaskID: "t"}, nil, workerpkg.AttemptFresh, domain.WorkerSession{WorkerID: "w"}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	done, err := workerpkg.Finish(a, workerpkg.AttemptExited, "x", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workerpkg.Finish(done, workerpkg.AttemptExited, "y", time.Now().UTC()); err == nil {
		t.Fatal("expected error")
	}
}

func TestRecoveryDoesNotAutoRetryTerminal(t *testing.T) {
	// executeWave skip condition
	runtime := TaskState{Task: domain.Task{TaskID: "t", Status: state.TaskFailed}}
	if !recoveryTaskTerminal(runtime) {
		t.Fatal("failed should be terminal")
	}
	runtime.Task.Status = state.TaskVerifiedSuccess
	if !recoveryTaskTerminal(runtime) {
		t.Fatal("success should be terminal")
	}
}

func TestDefaultCancelPolicyPositive(t *testing.T) {
	p := defaultCancelPolicy(0)
	if p.InterruptGrace <= 0 || p.TermGrace <= 0 || p.KillGrace <= 0 {
		t.Fatalf("%+v", p)
	}
}

func TestProcessMissingHelpers(t *testing.T) {
	if !process.IsProcessNotFound(os.ErrNotExist) {
		t.Fatal("expected missing for os.ErrNotExist")
	}
	if !process.IsProcessNotFound(process.ErrProcessNotFound) {
		t.Fatal("expected missing for ErrProcessNotFound")
	}
	if process.IsProcessNotFound(errors.New("permission denied")) {
		t.Fatal("permission is not missing")
	}
	// Natural-language strings must NOT count as process-not-found.
	if process.IsProcessNotFound(errors.New("no such process")) {
		t.Fatal("string-only errors must not prove process missing")
	}
	if process.IsProcessNotFound(errors.New("no such file or directory")) {
		t.Fatal("generic no-such-file must not prove process missing")
	}
	_ = context.Background()
}
