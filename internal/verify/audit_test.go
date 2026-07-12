package verify

import "testing"

func TestRunLevelScopeAudit(t *testing.T) {
	audit, err := AuditScopes([]string{"internal/auth/a.go", "README.md"}, map[string][]string{"auth": {"internal/auth/**"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(audit.Authorized) != 1 || len(audit.Unauthorized) != 1 || audit.Unauthorized[0] != "README.md" {
		t.Fatalf("unexpected audit: %+v", audit)
	}
}
