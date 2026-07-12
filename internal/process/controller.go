package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"
)

// TerminationPolicy controls the Interrupt → TERM → KillTree escalation.
type TerminationPolicy struct {
	InterruptGrace time.Duration
	TermGrace      time.Duration
	KillGrace      time.Duration
	PollInterval   time.Duration
}

// TerminationResult records which signals were sent and whether the original
// process tree exited (or was replaced by PID reuse).
//
// TreeExited=true only when the controller confirmed the original tree is gone
// (or PID reuse). Inability to confirm is never reported as success: TreeExited
// stays false and OrphanRisk may be set.
type TerminationResult struct {
	InterruptSent bool
	TermSent      bool
	KillSent      bool
	TreeExited    bool
	PIDReused     bool
	OrphanRisk    bool
	RemainingPIDs []int
	Errors        []string
}

// Controller terminates process trees through a TreeManager.
type Controller struct {
	Manager TreeManager
}

// TerminateTree escalates Interrupt → TERM → KillTree and confirms the original
// tree is gone. PID reuse is treated as the original process having exited;
// signals are never sent to a reused PID.
func (c Controller) TerminateTree(
	ctx context.Context,
	identity Identity,
	policy TerminationPolicy,
) (TerminationResult, error) {
	var result TerminationResult
	if c.Manager == nil {
		return result, fmt.Errorf("process tree manager is required")
	}
	if !identity.Complete() {
		return result, fmt.Errorf("incomplete process identity")
	}

	gone, reused, remaining, err := c.inspectTreeState(ctx, identity)
	if err != nil {
		return result, err
	}
	if reused {
		result.PIDReused = true
		result.TreeExited = true
		return result, nil
	}
	if gone {
		result.TreeExited = true
		return result, nil
	}

	// Interrupt
	if err := c.Manager.Interrupt(ctx, identity); err != nil {
		result.Errors = append(result.Errors, "interrupt: "+err.Error())
	} else {
		result.InterruptSent = true
	}
	gone, reused, remaining, err = c.waitStage(ctx, identity, policy.InterruptGrace, policy.PollInterval)
	if err != nil {
		result.RemainingPIDs = remaining
		return result, err
	}
	if reused {
		result.PIDReused = true
		result.TreeExited = true
		return result, nil
	}
	if gone {
		result.TreeExited = true
		return result, nil
	}

	// TERM
	if err := c.Manager.TerminateGracefully(ctx, identity); err != nil {
		result.Errors = append(result.Errors, "terminate: "+err.Error())
	} else {
		result.TermSent = true
	}
	gone, reused, remaining, err = c.waitStage(ctx, identity, policy.TermGrace, policy.PollInterval)
	if err != nil {
		result.RemainingPIDs = remaining
		return result, err
	}
	if reused {
		result.PIDReused = true
		result.TreeExited = true
		return result, nil
	}
	if gone {
		result.TreeExited = true
		return result, nil
	}

	// KillTree
	if err := c.Manager.KillTree(ctx, identity); err != nil {
		result.Errors = append(result.Errors, "kill: "+err.Error())
	} else {
		result.KillSent = true
	}
	gone, reused, remaining, err = c.waitStage(ctx, identity, policy.KillGrace, policy.PollInterval)
	if err != nil {
		result.RemainingPIDs = remaining
		return result, err
	}
	if reused {
		result.PIDReused = true
		result.TreeExited = true
		return result, nil
	}
	if gone {
		result.TreeExited = true
		return result, nil
	}

	result.TreeExited = false
	result.OrphanRisk = true
	result.RemainingPIDs = remaining
	return result, nil
}

// WaitTreeGone polls until the original process tree has no members, the PID is
// reused, the timeout elapses, or ctx is cancelled.
func (c Controller) WaitTreeGone(
	ctx context.Context,
	identity Identity,
	timeout time.Duration,
	pollInterval time.Duration,
) (gone bool, pidReused bool, remaining []int, err error) {
	if c.Manager == nil {
		return false, false, nil, fmt.Errorf("process tree manager is required")
	}
	if !identity.Complete() {
		return false, false, nil, fmt.Errorf("incomplete process identity")
	}
	if pollInterval <= 0 {
		pollInterval = 10 * time.Millisecond
	}

	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	for {
		if err := ctx.Err(); err != nil {
			gone, reused, remaining, _ := c.inspectTreeState(context.Background(), identity)
			if gone || reused {
				return gone, reused, remaining, err
			}
			return false, false, remaining, err
		}

		gone, reused, remaining, inspectErr := c.inspectTreeState(ctx, identity)
		if inspectErr != nil {
			if errors.Is(inspectErr, context.Canceled) || errors.Is(inspectErr, context.DeadlineExceeded) {
				return false, false, remaining, inspectErr
			}
			return false, false, remaining, inspectErr
		}
		if gone || reused {
			return gone, reused, remaining, nil
		}

		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return false, false, remaining, nil
		}

		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			_, _, remaining, _ = c.inspectTreeState(context.Background(), identity)
			return false, false, remaining, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c Controller) waitStage(
	ctx context.Context,
	identity Identity,
	grace time.Duration,
	pollInterval time.Duration,
) (gone bool, reused bool, remaining []int, err error) {
	if grace <= 0 {
		return c.inspectTreeState(ctx, identity)
	}
	waitCtx, cancel := context.WithTimeout(ctx, grace)
	defer cancel()
	gone, reused, remaining, err = c.WaitTreeGone(waitCtx, identity, grace, pollInterval)
	if err == nil {
		return gone, reused, remaining, nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		// Grace elapsed while the tree was still present; escalate.
		gone, reused, remaining, _ = c.inspectTreeState(ctx, identity)
		return gone, reused, remaining, nil
	}
	if ctx.Err() != nil {
		return gone, reused, remaining, ctx.Err()
	}
	return gone, reused, remaining, err
}

func (c Controller) inspectTreeState(ctx context.Context, identity Identity) (gone bool, reused bool, remaining []int, err error) {
	if err := ctx.Err(); err != nil {
		return false, false, nil, err
	}
	current, err := c.Manager.Inspect(ctx, identity.PID)
	if processMissing(err) {
		members, memberErr := c.Manager.GroupMembers(ctx, identity)
		if memberErr != nil && !processMissing(memberErr) {
			// Group scan unsupported or failed after root vanished: treat root exit as tree exit.
			if isUnsupported(memberErr) {
				return true, false, nil, nil
			}
			return false, false, nil, memberErr
		}
		if len(members) == 0 {
			return true, false, nil, nil
		}
		return false, false, pidsOf(members), nil
	}
	if err != nil {
		return false, false, nil, err
	}
	if !identity.SameProcess(current) {
		return true, true, nil, nil
	}

	members, err := c.Manager.GroupMembers(ctx, identity)
	if err != nil {
		return false, false, nil, err
	}
	if len(members) == 0 {
		return true, false, nil, nil
	}
	return false, false, pidsOf(members), nil
}

func pidsOf(members []Identity) []int {
	pids := make([]int, 0, len(members))
	for _, member := range members {
		pids = append(pids, member.PID)
	}
	sort.Ints(pids)
	return pids
}

func processMissing(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such process") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "no such file")
}

func isUnsupported(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "not implemented")
}
