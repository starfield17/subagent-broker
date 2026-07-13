package verify

import (
	"fmt"
	"sort"

	"github.com/vnai/subagent-broker/internal/scope"
)

// DefaultEphemeralPaths deliberately covers only runtime/test caches that are
// known not to be Task deliverables in Python projects. Broader build/output
// directories are intentionally not defaults because they may contain real
// deliverables.
var DefaultEphemeralPaths = []string{
	"**/__pycache__/**",
	"**/.pytest_cache/**",
	"**/*.pyc",
}

// AuditPolicy is frozen into a Run configuration at dispatch. Patterns use
// the same project-relative scope language as Task write leases.
type AuditPolicy struct {
	EphemeralPaths []string `json:"ephemeral_paths,omitempty"`
}

// DefaultAuditPolicy returns a fresh policy containing the narrow built-in
// cache patterns.
func DefaultAuditPolicy() AuditPolicy {
	return AuditPolicy{EphemeralPaths: append([]string(nil), DefaultEphemeralPaths...)}
}

// NormalizeAuditPolicy validates patterns with the repository's scope
// compiler, stores their normalized forms, and makes policy persistence
// deterministic.
func NormalizeAuditPolicy(policy AuditPolicy) (AuditPolicy, error) {
	seen := make(map[string]struct{}, len(policy.EphemeralPaths))
	patterns := make([]string, 0, len(policy.EphemeralPaths))
	for _, raw := range policy.EphemeralPaths {
		compiled, err := scope.Compile(raw)
		if err != nil {
			return AuditPolicy{}, fmt.Errorf("invalid ephemeral path %q: %w", raw, err)
		}
		if _, ok := seen[compiled.Normalized]; ok {
			continue
		}
		seen[compiled.Normalized] = struct{}{}
		patterns = append(patterns, compiled.Normalized)
	}
	sort.Strings(patterns)
	return AuditPolicy{EphemeralPaths: patterns}, nil
}

// NewAuditPolicy combines the narrow defaults with user-supplied patterns.
func NewAuditPolicy(extra ...string) (AuditPolicy, error) {
	patterns := append(DefaultAuditPolicy().EphemeralPaths, extra...)
	return NormalizeAuditPolicy(AuditPolicy{EphemeralPaths: patterns})
}

type FileAttribution struct {
	Path   string   `json:"path"`
	Owners []string `json:"owners"`
}

type EphemeralAttribution struct {
	Path    string `json:"path"`
	Pattern string `json:"pattern"`
}

type ScopeAudit struct {
	Authorized     []FileAttribution      `json:"authorized"`
	Ephemeral      []EphemeralAttribution `json:"ephemeral"`
	Unauthorized   []string               `json:"unauthorized"`
	OwnerUncertain []FileAttribution      `json:"owner_uncertain"`
}

func AuditScopes(changedFiles []string, leases map[string][]string, policy AuditPolicy) (ScopeAudit, error) {
	var result ScopeAudit
	normalized, err := NormalizeAuditPolicy(policy)
	if err != nil {
		return ScopeAudit{}, err
	}
	compiledEphemeral := make([]scope.Pattern, 0, len(normalized.EphemeralPaths))
	for _, raw := range normalized.EphemeralPaths {
		compiled, err := scope.Compile(raw)
		if err != nil {
			return ScopeAudit{}, err
		}
		compiledEphemeral = append(compiledEphemeral, compiled)
	}
	files := uniqueSorted(changedFiles)
	sort.Strings(files)
	for _, file := range files {
		if pattern := matchingEphemeralPattern(file, compiledEphemeral); pattern != "" {
			result.Ephemeral = append(result.Ephemeral, EphemeralAttribution{Path: file, Pattern: pattern})
			continue
		}
		owners, err := scope.CoveringOwners(file, leases)
		if err != nil {
			return ScopeAudit{}, err
		}
		switch len(owners) {
		case 0:
			result.Unauthorized = append(result.Unauthorized, file)
		case 1:
			result.Authorized = append(result.Authorized, FileAttribution{Path: file, Owners: owners})
		default:
			result.OwnerUncertain = append(result.OwnerUncertain, FileAttribution{Path: file, Owners: owners})
		}
	}
	return result, nil
}

func matchingEphemeralPattern(file string, patterns []scope.Pattern) string {
	var match string
	for _, pattern := range patterns {
		if !pattern.Match(file) {
			continue
		}
		// Prefer the most specific matching pattern so a cache-specific rule is
		// retained when a broad *.pyc rule also matches the same file.
		if len(pattern.Normalized) > len(match) || (len(pattern.Normalized) == len(match) && pattern.Normalized < match) {
			match = pattern.Normalized
		}
	}
	return match
}

func uniqueSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
