package process

import "testing"

func TestPIDReuseRequiresStartTokenMatch(t *testing.T) {
	old := Identity{PID: 42, StartToken: "old"}
	reused := Identity{PID: 42, StartToken: "new"}
	if old.SameProcess(reused) {
		t.Fatal("PID equality alone must not identify a process")
	}
}
