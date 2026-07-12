package clioutcome

import (
	"errors"
	"testing"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/state"
)

func TestCodeOfNilTypedAndPlain(t *testing.T) {
	if CodeOf(nil) != ExitOK {
		t.Fatal("nil must be ExitOK")
	}
	if CodeOf(New(ExitTimeout, "wait", "timed out", nil)) != ExitTimeout {
		t.Fatal("typed error code")
	}
	if CodeOf(errors.New("boom")) != ExitInternal {
		t.Fatal("plain error must be ExitInternal")
	}
}

func TestErrorAsAndUnwrap(t *testing.T) {
	inner := errors.New("disk")
	err := New(ExitCommunication, "ipc", "dial failed", inner)
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatal("errors.As failed")
	}
	if typed.Code != ExitCommunication || typed.Op != "ipc" {
		t.Fatalf("typed=%+v", typed)
	}
	if !errors.Is(err, inner) {
		t.Fatal("unwrap/Is failed")
	}
}

func TestFromRun(t *testing.T) {
	cases := []struct {
		status domain.RunStatus
		code   ExitCode
		term   bool
		ok     bool
	}{
		{domain.RunCompleted, ExitOK, true, true},
		{domain.RunFailed, ExitFailed, true, false},
		{domain.RunDegraded, ExitFailed, true, false},
		{domain.RunCancelled, ExitCancelled, true, false},
		{domain.RunRunning, ExitOK, false, false},
	}
	for _, tc := range cases {
		out := FromRun(tc.status)
		if out.Code != tc.code || out.Terminal != tc.term || out.Successful != tc.ok {
			t.Fatalf("%s => %+v want code=%d term=%v ok=%v", tc.status, out, tc.code, tc.term, tc.ok)
		}
	}
}

func TestDegradedRunUsesDurableReason(t *testing.T) {
	cases := []struct {
		reason string
		code   ExitCode
	}{
		{"partial task result", ExitPartial},
		{"Wave barrier blocked: pending decision", ExitBlocked},
		{"Supervisor communication lost", ExitCommunication},
		{"hard timeout", ExitTimeout},
		{"cancel completed with residual worker processes", ExitCancelled},
	}
	for _, tc := range cases {
		if got := FromRunDetailed(domain.RunDegraded, tc.reason).Code; got != tc.code {
			t.Fatalf("reason %q => %d, want %d", tc.reason, got, tc.code)
		}
	}
}

func TestFromWave(t *testing.T) {
	passed := FromWave(domain.Wave{WaveID: "w1", Status: domain.WaveVerified, BarrierResult: domain.BarrierPassed})
	if !passed.Successful || passed.Code != ExitOK || !passed.Terminal {
		t.Fatalf("passed=%+v", passed)
	}
	warning := FromWave(domain.Wave{WaveID: "w1", Status: domain.WaveVerified, BarrierResult: domain.BarrierPassedWithWarnings})
	if warning.Code != ExitBlocked || warning.Successful {
		t.Fatalf("unaccepted warning=%+v", warning)
	}
	accepted := FromWave(domain.Wave{WaveID: "w1", Status: domain.WaveVerified, BarrierResult: domain.BarrierPassedWithWarnings, BarrierAccepted: true})
	if accepted.Code != ExitOK || !accepted.Successful {
		t.Fatalf("accepted warning=%+v", accepted)
	}
	if FromWave(domain.Wave{Status: domain.WaveBlocked}).Code != ExitBlocked {
		t.Fatal("blocked")
	}
	if FromWave(domain.Wave{Status: domain.WaveFailed}).Code != ExitFailed {
		t.Fatal("failed")
	}
	if FromWave(domain.Wave{Status: domain.WaveCancelled}).Code != ExitCancelled {
		t.Fatal("cancelled")
	}
}

func TestFromTask(t *testing.T) {
	if out := FromTask("t1", state.TaskVerifiedSuccess, false); out.Code != ExitOK || !out.Successful || !out.Terminal {
		t.Fatalf("success=%+v", out)
	}
	if out := FromTask("t1", state.TaskVerifiedPartial, false); out.Code != ExitPartial || !out.Terminal {
		t.Fatalf("partial=%+v", out)
	}
	if out := FromTask("t1", state.TaskFailed, false); out.Code != ExitFailed {
		t.Fatalf("failed=%+v", out)
	}
	if out := FromTask("t1", state.TaskVerificationFailed, false); out.Code != ExitFailed {
		t.Fatalf("verification_failed=%+v", out)
	}
	if out := FromTask("t1", state.TaskCancelled, false); out.Code != ExitCancelled {
		t.Fatalf("cancelled=%+v", out)
	}
	if out := FromTask("t1", state.TaskBlocked, false); out.Terminal || out.Code != ExitOK {
		t.Fatalf("non-terminal blocked=%+v", out)
	}
	if out := FromTask("t1", state.TaskBlocked, true); !out.Terminal || out.Code != ExitBlocked {
		t.Fatalf("terminal blocked=%+v", out)
	}
}
