package verify

import (
	"path/filepath"
	"strings"
)

// HighRiskPattern describes a high-risk workspace path class.
type HighRiskPattern struct {
	Pattern string
	Reason  string
}

// DefaultHighRiskPatterns covers common global/shared objects.
var DefaultHighRiskPatterns = []HighRiskPattern{
	{Pattern: "go.mod", Reason: "module dependencies"},
	{Pattern: "go.sum", Reason: "module lockfile"},
	{Pattern: "package.json", Reason: "package dependencies"},
	{Pattern: "package-lock.json", Reason: "package lockfile"},
	{Pattern: "pnpm-lock.yaml", Reason: "package lockfile"},
	{Pattern: "yarn.lock", Reason: "package lockfile"},
	{Pattern: "Cargo.toml", Reason: "crate dependencies"},
	{Pattern: "Cargo.lock", Reason: "crate lockfile"},
	{Pattern: ".github/workflows/**", Reason: "CI workflow"},
	{Pattern: ".gitlab-ci.yml", Reason: "CI configuration"},
	{Pattern: "Dockerfile", Reason: "container build"},
	{Pattern: "docker-compose*.yml", Reason: "container orchestration"},
	{Pattern: "Makefile", Reason: "build entrypoint"},
	{Pattern: "migrations/**", Reason: "schema migration"},
	{Pattern: "schema/**", Reason: "schema definition"},
	{Pattern: "**/*_test.go", Reason: ""}, // not high-risk by default — excluded below
}

// ClassifyHighRisk returns high-risk changed paths with reasons.
// Paths matching test-only globs are not classified as high-risk unless they
// also match a global dependency/CI pattern.
func ClassifyHighRisk(changed []string) []HighRiskMatch {
	var matches []HighRiskMatch
	for _, path := range changed {
		clean := filepath.ToSlash(path)
		for _, rule := range DefaultHighRiskPatterns {
			if rule.Reason == "" {
				continue
			}
			if matchHighRisk(clean, rule.Pattern) {
				matches = append(matches, HighRiskMatch{Path: clean, Reason: rule.Reason})
				break
			}
		}
	}
	return matches
}

// HighRiskMatch is a changed path classified as high-risk.
type HighRiskMatch struct {
	Path   string
	Reason string
}

func matchHighRisk(path, pattern string) bool {
	pattern = filepath.ToSlash(pattern)
	if strings.HasPrefix(pattern, "**/") {
		suffix := strings.TrimPrefix(pattern, "**/")
		return strings.HasSuffix(path, strings.TrimPrefix(suffix, "*")) || path == strings.TrimPrefix(suffix, "*")
	}
	if strings.HasSuffix(pattern, "/**") {
		prefix := strings.TrimSuffix(pattern, "/**")
		return path == prefix || strings.HasPrefix(path, prefix+"/")
	}
	if strings.Contains(pattern, "*") {
		// simple prefix*suffix
		parts := strings.Split(pattern, "*")
		if len(parts) == 2 {
			return strings.HasPrefix(path, parts[0]) && strings.HasSuffix(path, parts[1])
		}
	}
	return path == pattern || filepath.Base(path) == pattern
}
