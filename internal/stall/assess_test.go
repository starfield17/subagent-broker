package stall

import (
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/state"
)

func TestWaitingNotStall(t *testing.T) {
	now := time.Now().UTC()
	a := Assess(Input{
		Protocol: state.ProtocolWaitingUser, Progress: state.ProgressQuiet,
		Process: state.ProcessAlive, LastProgressAt: now.Add(-time.Hour),
		QuietAfter: 30 * time.Second, StallAfter: 2 * time.Minute, Now: now,
		HasPendingMessage: true,
	})
	if a.Confidence != "none" || a.State != "none" {
		t.Fatalf("%+v", a)
	}
}

func TestQuietOnlyIsLow(t *testing.T) {
	now := time.Now().UTC()
	a := Assess(Input{
		Protocol: state.ProtocolThinking, Process: state.ProcessAlive,
		LastProgressAt: now.Add(-45 * time.Second), LastEventAt: now.Add(-10 * time.Second),
		QuietAfter: 30 * time.Second, StallAfter: 2 * time.Minute, Now: now,
	})
	if a.State != string(state.ProgressQuiet) || a.Confidence != "low" {
		t.Fatalf("%+v", a)
	}
}

func TestHighConfidenceStall(t *testing.T) {
	now := time.Now().UTC()
	a := Assess(Input{
		Protocol: state.ProtocolToolRunning, Process: state.ProcessAlive,
		LastProgressAt: now.Add(-10 * time.Minute), LastEventAt: now.Add(-10 * time.Minute),
		QuietAfter: 30 * time.Second, StallAfter: 2 * time.Minute, Now: now,
	})
	if a.Confidence != "high" || a.State != string(state.ProgressStalled) {
		t.Fatalf("%+v", a)
	}
	if len(a.Evidence) == 0 {
		t.Fatal("expected evidence")
	}
}

func TestQuietOnlyAtStallThresholdIsLowSuspected(t *testing.T) {
	now := time.Now().UTC()
	a := Assess(Input{
		Protocol: state.ProtocolThinking, Process: state.ProcessAlive,
		LastProgressAt: now.Add(-3 * time.Minute), StallAfter: 2 * time.Minute,
		QuietAfter: 30 * time.Second, Now: now,
	})
	if a.State != string(state.ProgressSuspectedStall) || a.Confidence != "low" || a.Reason != "quiet timeout only" {
		t.Fatalf("%+v", a)
	}
}

func TestIndependentSignalsPromoteMediumConfidence(t *testing.T) {
	now := time.Now().UTC()
	a := Assess(Input{
		Protocol: state.ProtocolToolRunning, Process: state.ProcessAlive,
		LastProgressAt: now.Add(-3 * time.Minute), LastProtocolEventAt: now.Add(-3 * time.Minute),
		LastStdoutAt: now.Add(-3 * time.Minute), StallAfter: 2 * time.Minute,
		QuietAfter: 30 * time.Second, Now: now,
	})
	if a.State != string(state.ProgressSuspectedStall) || a.Confidence != "medium" || len(a.Evidence) < 3 {
		t.Fatalf("%+v", a)
	}
}

func TestProcessExitIsNotStall(t *testing.T) {
	now := time.Now().UTC()
	a := Assess(Input{
		Protocol: state.ProtocolThinking, Process: state.ProcessExited,
		LastProgressAt: now.Add(-time.Hour), StallAfter: time.Minute, Now: now,
	})
	if a.State != "none" || a.Confidence != "none" {
		t.Fatalf("%+v", a)
	}
}
