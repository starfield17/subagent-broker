package verify

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunLevelScopeAudit(t *testing.T) {
	audit, err := AuditScopes([]string{"internal/auth/a.go", "README.md"}, map[string][]string{"auth": {"internal/auth/**"}}, AuditPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if len(audit.Authorized) != 1 || len(audit.Unauthorized) != 1 || audit.Unauthorized[0] != "README.md" {
		t.Fatalf("unexpected audit: %+v", audit)
	}
}

func TestDefaultEphemeralAuditCoversRootAndNestedCaches(t *testing.T) {
	policy, err := NormalizeAuditPolicy(DefaultAuditPolicy())
	if err != nil {
		t.Fatal(err)
	}
	audit, err := AuditScopes([]string{
		".pytest_cache/CACHEDIR.TAG",
		"pkg/.pytest_cache/v/cache/lastfailed",
		"__pycache__/root.pyc",
		"pkg/__pycache__/nested.pyc",
		"root.pyc",
		"pkg/root.pyc",
	}, nil, policy)
	if err != nil {
		t.Fatal(err)
	}
	if len(audit.Ephemeral) != 6 || len(audit.Authorized) != 0 || len(audit.Unauthorized) != 0 || len(audit.OwnerUncertain) != 0 {
		t.Fatalf("unexpected ephemeral audit: %+v", audit)
	}
	for _, item := range audit.Ephemeral {
		if item.Pattern == "" {
			t.Fatalf("missing pattern attribution: %+v", item)
		}
	}
}

func TestAuditScopesClassifiesEachPathExactlyOnce(t *testing.T) {
	audit, err := AuditScopes(
		[]string{"owned.go", "other.go", "shared.go", "owned.go", "__pycache__/x.pyc"},
		map[string][]string{
			"one": {"owned.go", "shared.go"},
			"two": {"shared.go"},
		},
		DefaultAuditPolicy(),
	)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, item := range audit.Authorized {
		counts[item.Path]++
	}
	for _, item := range audit.Ephemeral {
		counts[item.Path]++
	}
	for _, item := range audit.Unauthorized {
		counts[item]++
	}
	for _, item := range audit.OwnerUncertain {
		counts[item.Path]++
	}
	for _, path := range []string{"owned.go", "other.go", "shared.go", "__pycache__/x.pyc"} {
		if counts[path] != 1 {
			t.Fatalf("path %q count=%d audit=%+v", path, counts[path], audit)
		}
	}
	if len(audit.Authorized) != 1 || audit.Authorized[0].Path != "owned.go" {
		t.Fatalf("authorized=%+v", audit.Authorized)
	}
	if len(audit.Unauthorized) != 1 || audit.Unauthorized[0] != "other.go" {
		t.Fatalf("unauthorized=%v", audit.Unauthorized)
	}
	if len(audit.OwnerUncertain) != 1 || audit.OwnerUncertain[0].Path != "shared.go" {
		t.Fatalf("owner uncertain=%+v", audit.OwnerUncertain)
	}
}

func TestAuditScopesRejectsInvalidEphemeralPattern(t *testing.T) {
	if _, err := NormalizeAuditPolicy(AuditPolicy{EphemeralPaths: []string{"../outside/**"}}); err == nil {
		t.Fatal("expected project-escaping pattern to be rejected")
	}
	if _, err := AuditScopes([]string{"file.txt"}, nil, AuditPolicy{EphemeralPaths: []string{"/absolute/**"}}); err == nil {
		t.Fatal("expected invalid project-relative glob to be rejected")
	}
}

func TestAuditPolicyNormalizesDeduplicatesAndSorts(t *testing.T) {
	policy, err := NewAuditPolicy("./z-cache/**", "z-cache/**", "a-cache/**")
	if err != nil {
		t.Fatal(err)
	}
	if len(policy.EphemeralPaths) != len(DefaultEphemeralPaths)+2 {
		t.Fatalf("unexpected normalized policy: %+v", policy)
	}
	for index := 1; index < len(policy.EphemeralPaths); index++ {
		if policy.EphemeralPaths[index-1] > policy.EphemeralPaths[index] {
			t.Fatalf("policy is not sorted: %v", policy.EphemeralPaths)
		}
	}
	if count := countString(policy.EphemeralPaths, "z-cache/**"); count != 1 {
		t.Fatalf("deduplication failed: %v", policy.EphemeralPaths)
	}
}

func countString(values []string, target string) int {
	count := 0
	for _, value := range values {
		if value == target {
			count++
		}
	}
	return count
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

func TestWorkspaceSnapshotCapturesEphemeralFiles(t *testing.T) {
	root := t.TempDir()
	paths := []string{
		filepath.Join(root, ".pytest_cache", "CACHEDIR.TAG"),
		filepath.Join(root, "pkg", "__pycache__", "module.pyc"),
		filepath.Join(root, "pkg", "module.pyc"),
	}
	for _, path := range paths {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("cache"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := CaptureWorkspace(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{".pytest_cache/CACHEDIR.TAG", "pkg/__pycache__/module.pyc", "pkg/module.pyc"} {
		if _, ok := snapshot.Files[path]; !ok {
			t.Fatalf("ephemeral file %q was not captured: %+v", path, snapshot.Files)
		}
	}
}
