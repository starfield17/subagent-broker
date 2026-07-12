package state

import "testing"

func TestReportedCompleteRequiresVerification(t *testing.T) {
	if err := ValidateTaskTransition(TaskReportedComplete, TaskVerifiedSuccess); err == nil {
		t.Fatal("reported_complete must not jump directly to verified_success")
	}
	if err := ValidateTaskTransition(TaskReportedComplete, TaskVerifying); err != nil {
		t.Fatalf("expected verification transition: %v", err)
	}
	if err := ValidateTaskTransition(TaskVerifying, TaskVerifiedSuccess); err != nil {
		t.Fatalf("expected verified success transition: %v", err)
	}
}

func TestWaitingIsNotStall(t *testing.T) {
	for _, p := range []Protocol{ProtocolWaitingPermission, ProtocolWaitingUser, ProtocolWaitingScope} {
		if MayEscalateToStall(p) {
			t.Fatalf("%s must not escalate to stall", p)
		}
		err := ValidateDimensions(Dimensions{Process: ProcessAlive, Protocol: p, Progress: ProgressSuspectedStall, Task: TaskBlocked})
		if err == nil {
			t.Fatalf("%s with suspected stall should be invalid", p)
		}
	}
}

func TestProgressCanRecover(t *testing.T) {
	if err := ValidateProgressTransition(ProgressSuspectedStall, ProgressActive); err != nil {
		t.Fatalf("progress should recover after new evidence: %v", err)
	}
}
