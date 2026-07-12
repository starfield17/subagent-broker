package process

import "testing"

func TestPIDReuseRequiresStartTokenMatch(t *testing.T) {
	old := Identity{PID: 42, StartToken: "old", ProcessGroupToken: "7"}
	reused := Identity{PID: 42, StartToken: "new", ProcessGroupToken: "7"}
	if old.SameProcess(reused) {
		t.Fatal("PID equality alone must not identify a process")
	}
	if !old.SameGroup(reused) {
		t.Fatal("same process group token should match")
	}
	if !old.Complete() {
		t.Fatal("expected complete identity")
	}
	if (Identity{PID: 42, StartToken: "old"}).Complete() {
		t.Fatal("identity without process group is incomplete")
	}
}
