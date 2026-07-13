package process

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

type fakeTree struct {
	mu sync.Mutex

	// pid -> identity currently alive
	live map[int]Identity
	// group token -> member pids
	groups map[string][]int

	interruptErr error
	termErr      error
	killErr      error

	interruptCalls int
	termCalls      int
	killCalls      int

	// When set, the corresponding signal removes all members of the identity's group.
	exitOnInterrupt bool
	exitOnTerm      bool
	exitOnKill      bool

	// When set, Inspect reports a different StartToken (PID reuse).
	reuseAfter int // become reused after this many Inspect calls (>0)
	inspects   int
}

func newFakeTree(root Identity, members ...Identity) *fakeTree {
	f := &fakeTree{
		live:   map[int]Identity{},
		groups: map[string][]int{},
	}
	all := append([]Identity{root}, members...)
	for _, member := range all {
		f.live[member.PID] = member
		f.groups[member.ProcessGroupToken] = append(f.groups[member.ProcessGroupToken], member.PID)
	}
	return f
}

func (f *fakeTree) Inspect(_ context.Context, pid int) (Identity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inspects++
	if f.reuseAfter > 0 && f.inspects >= f.reuseAfter {
		if id, ok := f.live[pid]; ok {
			reused := id
			reused.StartToken = id.StartToken + "-reused"
			return reused, nil
		}
		return Identity{PID: pid, StartToken: "reused", ProcessGroupToken: "1"}, nil
	}
	id, ok := f.live[pid]
	if !ok {
		return Identity{}, os.ErrNotExist
	}
	return id, nil
}

func (f *fakeTree) GroupMembers(_ context.Context, identity Identity) ([]Identity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pids := f.groups[identity.ProcessGroupToken]
	var members []Identity
	for _, pid := range pids {
		if id, ok := f.live[pid]; ok {
			members = append(members, id)
		}
	}
	return members, nil
}

func (f *fakeTree) Interrupt(_ context.Context, identity Identity) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.interruptCalls++
	if f.interruptErr != nil {
		return f.interruptErr
	}
	if f.exitOnInterrupt {
		f.clearGroupLocked(identity.ProcessGroupToken)
	}
	return nil
}

func (f *fakeTree) TerminateGracefully(_ context.Context, identity Identity) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.termCalls++
	if f.termErr != nil {
		return f.termErr
	}
	if f.exitOnTerm {
		f.clearGroupLocked(identity.ProcessGroupToken)
	}
	return nil
}

func (f *fakeTree) KillTree(_ context.Context, identity Identity) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.killCalls++
	if f.killErr != nil {
		return f.killErr
	}
	if f.exitOnKill {
		f.clearGroupLocked(identity.ProcessGroupToken)
	}
	return nil
}

func (f *fakeTree) clearGroupLocked(group string) {
	for _, pid := range f.groups[group] {
		delete(f.live, pid)
	}
	delete(f.groups, group)
}

func (f *fakeTree) counts() (interrupt, term, kill int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.interruptCalls, f.termCalls, f.killCalls
}

func rootIdentity() Identity {
	return Identity{PID: 100, StartToken: "start-100", ProcessGroupToken: "900"}
}

func childIdentity() Identity {
	return Identity{PID: 101, StartToken: "start-101", ProcessGroupToken: "900"}
}

func policy() TerminationPolicy {
	return TerminationPolicy{
		InterruptGrace: time.Millisecond,
		TermGrace:      time.Millisecond,
		KillGrace:      time.Millisecond,
		PollInterval:   time.Millisecond,
	}
}

func TestTerminateTreeExitsAfterInterrupt(t *testing.T) {
	root := rootIdentity()
	fake := newFakeTree(root, childIdentity())
	fake.exitOnInterrupt = true
	controller := Controller{Manager: fake}

	result, err := controller.TerminateTree(context.Background(), root, policy())
	if err != nil {
		t.Fatal(err)
	}
	if !result.InterruptSent || result.TermSent || result.KillSent {
		t.Fatalf("unexpected signals: %+v", result)
	}
	if !result.TreeExited || result.PIDReused {
		t.Fatalf("unexpected result: %+v", result)
	}
	if !result.TerminationRequested || result.TerminationPhase != "interrupt" {
		t.Fatalf("termination provenance=%+v", result)
	}
	interrupt, term, kill := fake.counts()
	if interrupt != 1 || term != 0 || kill != 0 {
		t.Fatalf("counts interrupt=%d term=%d kill=%d", interrupt, term, kill)
	}
}

func TestTerminateTreeEscalatesToTerm(t *testing.T) {
	root := rootIdentity()
	fake := newFakeTree(root)
	fake.exitOnTerm = true
	controller := Controller{Manager: fake}

	result, err := controller.TerminateTree(context.Background(), root, policy())
	if err != nil {
		t.Fatal(err)
	}
	if !result.InterruptSent || !result.TermSent || result.KillSent || !result.TreeExited {
		t.Fatalf("unexpected result: %+v", result)
	}
	if !result.TerminationRequested || result.TerminationPhase != "term" {
		t.Fatalf("termination provenance=%+v", result)
	}
	interrupt, term, kill := fake.counts()
	if interrupt != 1 || term != 1 || kill != 0 {
		t.Fatalf("counts interrupt=%d term=%d kill=%d", interrupt, term, kill)
	}
}

func TestTerminateTreeEscalatesToKill(t *testing.T) {
	root := rootIdentity()
	fake := newFakeTree(root, childIdentity())
	fake.exitOnKill = true
	controller := Controller{Manager: fake}

	result, err := controller.TerminateTree(context.Background(), root, policy())
	if err != nil {
		t.Fatal(err)
	}
	if !result.InterruptSent || !result.TermSent || !result.KillSent || !result.TreeExited {
		t.Fatalf("unexpected result: %+v", result)
	}
	if !result.TerminationRequested || result.TerminationPhase != "kill_tree" {
		t.Fatalf("termination provenance=%+v", result)
	}
	interrupt, term, kill := fake.counts()
	if interrupt != 1 || term != 1 || kill != 1 {
		t.Fatalf("counts interrupt=%d term=%d kill=%d", interrupt, term, kill)
	}
}

func TestTerminateTreeReportsRemainingPIDs(t *testing.T) {
	root := rootIdentity()
	child := childIdentity()
	fake := newFakeTree(root, child)
	controller := Controller{Manager: fake}

	result, err := controller.TerminateTree(context.Background(), root, policy())
	if err != nil {
		t.Fatal(err)
	}
	if result.TreeExited {
		t.Fatalf("expected tree still present: %+v", result)
	}
	if len(result.RemainingPIDs) != 2 || result.RemainingPIDs[0] != 100 || result.RemainingPIDs[1] != 101 {
		t.Fatalf("remaining=%v", result.RemainingPIDs)
	}
	if !result.InterruptSent || !result.TermSent || !result.KillSent {
		t.Fatalf("expected full escalation: %+v", result)
	}
}

func TestTerminateTreePIDReuseSendsNoSignals(t *testing.T) {
	root := rootIdentity()
	fake := newFakeTree(root)
	// First Inspect in TerminateTree sees a different StartToken.
	fake.reuseAfter = 1
	controller := Controller{Manager: fake}

	result, err := controller.TerminateTree(context.Background(), root, policy())
	if err != nil {
		t.Fatal(err)
	}
	if !result.PIDReused || !result.TreeExited {
		t.Fatalf("expected pid reuse exit: %+v", result)
	}
	interrupt, term, kill := fake.counts()
	if interrupt != 0 || term != 0 || kill != 0 {
		t.Fatalf("signals must not be sent on pid reuse: interrupt=%d term=%d kill=%d", interrupt, term, kill)
	}
}

func TestTerminateTreeMissingPIDIsExited(t *testing.T) {
	root := rootIdentity()
	fake := &fakeTree{live: map[int]Identity{}, groups: map[string][]int{}}
	controller := Controller{Manager: fake}

	result, err := controller.TerminateTree(context.Background(), root, policy())
	if err != nil {
		t.Fatal(err)
	}
	if !result.TreeExited || result.InterruptSent {
		t.Fatalf("expected immediate exit without signals: %+v", result)
	}
	if result.TerminationRequested || result.TerminationInitiator != "" || result.TerminationPhase != "" {
		t.Fatalf("must not invent termination provenance: %+v", result)
	}
}

func TestTerminateTreeContextCancel(t *testing.T) {
	root := rootIdentity()
	fake := newFakeTree(root)
	controller := Controller{Manager: fake}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := controller.TerminateTree(ctx, root, TerminationPolicy{
		InterruptGrace: time.Second,
		TermGrace:      time.Second,
		KillGrace:      time.Second,
		PollInterval:   time.Millisecond,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestTerminateTreeContinuesAfterSignalErrors(t *testing.T) {
	root := rootIdentity()
	fake := newFakeTree(root)
	fake.interruptErr = errors.New("interrupt failed")
	fake.termErr = errors.New("term failed")
	fake.exitOnKill = true
	controller := Controller{Manager: fake}

	result, err := controller.TerminateTree(context.Background(), root, policy())
	if err != nil {
		t.Fatal(err)
	}
	if !result.KillSent || !result.TreeExited {
		t.Fatalf("expected kill success after earlier errors: %+v", result)
	}
	if len(result.Errors) < 2 {
		t.Fatalf("expected recorded signal errors: %+v", result.Errors)
	}
	interrupt, term, kill := fake.counts()
	if interrupt != 1 || term != 1 || kill != 1 {
		t.Fatalf("counts interrupt=%d term=%d kill=%d", interrupt, term, kill)
	}
}

func TestIdentityCompleteAndSameGroup(t *testing.T) {
	a := Identity{PID: 1, StartToken: "a", ProcessGroupToken: "g1"}
	b := Identity{PID: 2, StartToken: "b", ProcessGroupToken: "g1"}
	c := Identity{PID: 3, StartToken: "c", ProcessGroupToken: "g2"}
	if !a.Complete() {
		t.Fatal("expected complete identity")
	}
	if (Identity{PID: 1, StartToken: "a"}).Complete() {
		t.Fatal("missing group token is incomplete")
	}
	if !a.SameGroup(b) || a.SameGroup(c) {
		t.Fatal("SameGroup mismatch")
	}
	if a.SameProcess(b) {
		t.Fatal("different PIDs must not be SameProcess")
	}
}

func TestWaitTreeGonePIDReuse(t *testing.T) {
	root := rootIdentity()
	fake := newFakeTree(root)
	fake.reuseAfter = 1
	controller := Controller{Manager: fake}
	gone, reused, remaining, err := controller.WaitTreeGone(context.Background(), root, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !gone || !reused || len(remaining) != 0 {
		t.Fatalf("gone=%v reused=%v remaining=%v", gone, reused, remaining)
	}
}
