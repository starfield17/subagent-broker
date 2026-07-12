//go:build linux

package process

import (
	"context"
	"os"
	"testing"
)

func TestGroupMembersIncludesSelf(t *testing.T) {
	identity, err := Inspect(context.Background(), os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if !identity.Complete() {
		t.Fatalf("incomplete identity: %+v", identity)
	}
	members, err := GroupMembers(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, member := range members {
		if member.PID == identity.PID && member.SameProcess(identity) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("self pid %d not found in group members: %+v", identity.PID, members)
	}
}
