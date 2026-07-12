package verify

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type FileState struct {
	Kind   string      `json:"kind"`
	Mode   fs.FileMode `json:"mode"`
	Digest string      `json:"digest"`
}

type WorkspaceSnapshot struct {
	Files map[string]FileState `json:"files"`
}

func CaptureWorkspace(root string, excludedRoots ...string) (WorkspaceSnapshot, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return WorkspaceSnapshot{}, err
	}
	excluded := make([]string, 0, len(excludedRoots))
	for _, candidate := range excludedRoots {
		if candidate == "" {
			continue
		}
		if absolute, absoluteErr := filepath.Abs(candidate); absoluteErr == nil {
			excluded = append(excluded, filepath.Clean(absolute))
		}
	}
	result := WorkspaceSnapshot{Files: map[string]FileState{}}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		clean := filepath.Clean(path)
		if entry.IsDir() {
			if entry.Name() == ".git" || containsPath(excluded, clean) {
				return filepath.SkipDir
			}
			return nil
		}
		if containsPath(excluded, clean) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		state := FileState{Mode: info.Mode().Perm()}
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			state.Kind = "symlink"
			target, readErr := os.Readlink(path)
			if readErr != nil {
				return readErr
			}
			state.Digest = digestString(target)
		case info.Mode().IsRegular():
			state.Kind = "file"
			digest, digestErr := digestFile(path)
			if digestErr != nil {
				return digestErr
			}
			state.Digest = digest
		default:
			return nil
		}
		result.Files[relative] = state
		return nil
	})
	if err != nil {
		return WorkspaceSnapshot{}, fmt.Errorf("capture workspace: %w", err)
	}
	return result, nil
}

func ChangedFiles(before, after WorkspaceSnapshot) []string {
	seen := map[string]bool{}
	for path := range before.Files {
		seen[path] = true
	}
	for path := range after.Files {
		seen[path] = true
	}
	changed := make([]string, 0)
	for path := range seen {
		if before.Files[path] != after.Files[path] {
			changed = append(changed, path)
		}
	}
	sort.Strings(changed)
	return changed
}

func containsPath(roots []string, path string) bool {
	for _, root := range roots {
		if path == root || strings.HasPrefix(path, root+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func digestFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func digestString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
