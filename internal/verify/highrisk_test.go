package verify

import "testing"

func TestClassifyHighRiskFindsGoMod(t *testing.T) {
	matches := ClassifyHighRisk([]string{"internal/a.go", "go.mod", "README.md"})
	if len(matches) != 1 || matches[0].Path != "go.mod" {
		t.Fatalf("%+v", matches)
	}
}
