package verify

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunLevelScopeAudit(t *testing.T) {
	audit, err := AuditScopes([]string{"internal/auth/a.go", "README.md"}, map[string][]string{"auth": {"internal/auth/**"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(audit.Authorized) != 1 || len(audit.Unauthorized) != 1 || audit.Unauthorized[0] != "README.md" {
		t.Fatalf("unexpected audit: %+v", audit)
	}
}

func TestWorkspaceSnapshotDetectsChangesAfterBaseline(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "existing.txt")
	if err := os.WriteFile(path, []byte("before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := CaptureWorkspace(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("after\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "new.txt"), []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	after, err := CaptureWorkspace(root)
	if err != nil {
		t.Fatal(err)
	}
	changed := ChangedFiles(before, after)
	if len(changed) != 2 || changed[0] != "existing.txt" || changed[1] != "new.txt" {
		t.Fatalf("unexpected changed files: %v", changed)
	}
}
