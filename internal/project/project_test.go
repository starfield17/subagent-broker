package project

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestKeyPartsStableAndReadable(t *testing.T) {
	slugA, hashA := KeyParts("/Users/example/My Project")
	slugB, hashB := KeyParts("/Users/example/My Project")
	if slugA != slugB || hashA != hashB {
		t.Fatal("project key must be stable")
	}
	if !strings.Contains(slugA, "My-Project") {
		t.Fatalf("slug should remain human-readable: %s", slugA)
	}
	_, otherHash := KeyParts("/Users/example/Other Project")
	if hashA == otherHash {
		t.Fatal("different canonical paths should not share short hashes in this test")
	}
}

func TestUUIDv7VersionAndRunIDSortability(t *testing.T) {
	now := time.Date(2026, 7, 12, 5, 22, 18, 123000000, time.UTC)
	uuid, err := UUIDv7(now, bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(uuid, "-")
	if len(parts) != 5 || parts[2][0] != '7' {
		t.Fatalf("not UUIDv7: %s", uuid)
	}
	id, err := NewRunID(now)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(id), "20260712T052218.123Z-") {
		t.Fatalf("run id must be time-sortable: %s", id)
	}
}

func TestResolveUsesCurrentDirectoryWhenExplicitPathMissing(t *testing.T) {
	project, err := Resolve(context.Background(), "", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if project.CanonicalPath == "" || project.ProjectID == "" {
		t.Fatalf("unexpected project: %+v", project)
	}
}
