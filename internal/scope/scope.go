package scope

import (
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
)

type Pattern struct {
	Raw          string
	Normalized   string
	re           *regexp.Regexp
	staticPrefix []string
	hasWildcard  bool
}

func Compile(raw string) (Pattern, error) {
	normalized, err := Normalize(raw)
	if err != nil {
		return Pattern{}, err
	}
	re, err := regexp.Compile(globRegex(normalized))
	if err != nil {
		return Pattern{}, fmt.Errorf("compile scope %q: %w", raw, err)
	}
	return Pattern{
		Raw:          raw,
		Normalized:   normalized,
		re:           re,
		staticPrefix: staticPrefix(normalized),
		hasWildcard:  strings.ContainsAny(normalized, "*?[") || strings.Contains(normalized, "**"),
	}, nil
}

func Normalize(raw string) (string, error) {
	raw = strings.TrimSpace(strings.ReplaceAll(raw, "\\", "/"))
	raw = strings.TrimPrefix(raw, "./")
	if raw == "" {
		return "", fmt.Errorf("scope pattern is empty")
	}
	if strings.HasPrefix(raw, "/") || regexp.MustCompile(`^[A-Za-z]:/`).MatchString(raw) {
		return "", fmt.Errorf("scope pattern must be project-relative: %q", raw)
	}
	clean := path.Clean(raw)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("scope pattern escapes project root: %q", raw)
	}
	return clean, nil
}

func (p Pattern) Match(candidate string) bool {
	candidate = strings.TrimPrefix(strings.ReplaceAll(candidate, "\\", "/"), "./")
	candidate = path.Clean(candidate)
	return p.re.MatchString(candidate)
}

func Match(rawPattern, candidate string) (bool, error) {
	p, err := Compile(rawPattern)
	if err != nil {
		return false, err
	}
	return p.Match(candidate), nil
}

func MayOverlap(a, b string) (bool, error) {
	pa, err := Compile(a)
	if err != nil {
		return false, err
	}
	pb, err := Compile(b)
	if err != nil {
		return false, err
	}
	if !pa.hasWildcard && !pb.hasWildcard {
		return pa.Normalized == pb.Normalized, nil
	}
	if !pa.hasWildcard {
		return pb.Match(pa.Normalized), nil
	}
	if !pb.hasWildcard {
		return pa.Match(pb.Normalized), nil
	}
	min := len(pa.staticPrefix)
	if len(pb.staticPrefix) < min {
		min = len(pb.staticPrefix)
	}
	for i := 0; i < min; i++ {
		if pa.staticPrefix[i] != pb.staticPrefix[i] {
			return false, nil
		}
	}
	// Conservative by design: once static prefixes are compatible, reject the
	// pair rather than risk a false-negative write/write conflict.
	return true, nil
}

type Overlap struct {
	LeftOwner    string `json:"left_owner"`
	LeftPattern  string `json:"left_pattern"`
	RightOwner   string `json:"right_owner"`
	RightPattern string `json:"right_pattern"`
}

func FindOverlaps(leases map[string][]string) ([]Overlap, error) {
	owners := make([]string, 0, len(leases))
	for owner := range leases {
		owners = append(owners, owner)
	}
	sort.Strings(owners)
	var overlaps []Overlap
	for i := 0; i < len(owners); i++ {
		for j := i + 1; j < len(owners); j++ {
			for _, left := range leases[owners[i]] {
				for _, right := range leases[owners[j]] {
					overlap, err := MayOverlap(left, right)
					if err != nil {
						return nil, err
					}
					if overlap {
						overlaps = append(overlaps, Overlap{LeftOwner: owners[i], LeftPattern: left, RightOwner: owners[j], RightPattern: right})
					}
				}
			}
		}
	}
	return overlaps, nil
}

func CoveringOwners(candidate string, leases map[string][]string) ([]string, error) {
	var owners []string
	for owner, patterns := range leases {
		for _, raw := range patterns {
			p, err := Compile(raw)
			if err != nil {
				return nil, err
			}
			if p.Match(candidate) {
				owners = append(owners, owner)
				break
			}
		}
	}
	sort.Strings(owners)
	return owners, nil
}

func staticPrefix(pattern string) []string {
	parts := strings.Split(pattern, "/")
	prefix := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.ContainsAny(part, "*?[") {
			break
		}
		prefix = append(prefix, part)
	}
	return prefix
}

func globRegex(glob string) string {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(glob); {
		if glob[i] == '*' {
			if i+1 < len(glob) && glob[i+1] == '*' {
				if i+2 < len(glob) && glob[i+2] == '/' {
					b.WriteString("(?:.*/)?")
					i += 3
				} else {
					b.WriteString(".*")
					i += 2
				}
			} else {
				b.WriteString("[^/]*")
				i++
			}
			continue
		}
		switch glob[i] {
		case '?':
			b.WriteString("[^/]")
		case '.', '+', '(', ')', '|', '^', '$', '{', '}', '[', ']', '\\':
			b.WriteByte('\\')
			b.WriteByte(glob[i])
		default:
			b.WriteByte(glob[i])
		}
		i++
	}
	b.WriteString("$")
	return b.String()
}
