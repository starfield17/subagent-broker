package project

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
)

var slugInvalid = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

func CanonicalPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("project path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("absolute project path: %w", err)
	}
	abs = filepath.Clean(abs)
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	}
	return abs, nil
}

func Resolve(ctx context.Context, explicitPath string, now time.Time) (domain.Project, error) {
	if strings.TrimSpace(explicitPath) == "" {
		workingDir, err := os.Getwd()
		if err != nil {
			return domain.Project{}, fmt.Errorf("resolve current directory: %w", err)
		}
		explicitPath = workingDir
	}
	canonical, err := CanonicalPath(explicitPath)
	if err != nil {
		return domain.Project{}, err
	}
	gitRoot := gitValue(ctx, canonical, "rev-parse", "--show-toplevel")
	gitCommonDir := gitValue(ctx, canonical, "rev-parse", "--git-common-dir")
	if gitCommonDir != "" && !filepath.IsAbs(gitCommonDir) {
		gitCommonDir = filepath.Clean(filepath.Join(canonical, gitCommonDir))
	}
	identityPath := canonical
	if gitRoot != "" {
		if rooted, err := CanonicalPath(gitRoot); err == nil {
			gitRoot = rooted
			identityPath = rooted
		}
	}
	slug, hash := KeyParts(identityPath)
	return domain.Project{
		ProjectID:     domain.ProjectID(slug + "--" + hash),
		CanonicalPath: canonical,
		GitRoot:       gitRoot,
		GitCommonDir:  gitCommonDir,
		CreatedAt:     now.UTC(),
		LastSeenAt:    now.UTC(),
		PathSlug:      slug,
		PathHash:      hash,
	}, nil
}

func gitValue(ctx context.Context, dir string, args ...string) string {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func KeyParts(canonicalPath string) (slug, shortHash string) {
	normalized := filepath.ToSlash(filepath.Clean(canonicalPath))
	slug = strings.Trim(slugInvalid.ReplaceAllString(normalized, "-"), "-.")
	if slug == "" {
		slug = "project"
	}
	const maxSlug = 72
	if len(slug) > maxSlug {
		slug = slug[len(slug)-maxSlug:]
	}
	sum := sha256.Sum256([]byte(normalized))
	shortHash = hex.EncodeToString(sum[:6])
	return slug, shortHash
}

func NewRunID(now time.Time) (domain.RunID, error) {
	uuid, err := UUIDv7(now, rand.Reader)
	if err != nil {
		return "", err
	}
	return domain.RunID(now.UTC().Format("20060102T150405.000Z") + "-" + uuid), nil
}

func UUIDv7(now time.Time, source io.Reader) (string, error) {
	var b [16]byte
	if _, err := io.ReadFull(source, b[:]); err != nil {
		return "", fmt.Errorf("uuidv7 randomness: %w", err)
	}
	ms := uint64(now.UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
