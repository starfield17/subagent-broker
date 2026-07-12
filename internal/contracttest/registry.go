package contracttest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Result is a durable record of a harness contract verification.
type Result struct {
	Harness        string    `json:"harness"`
	Version        string    `json:"version"`
	Contract       string    `json:"contract"` // steer_active_turn | permission_routing | resume_session
	Status         string    `json:"status"`   // passed | failed | skipped | unverified
	PermissionMode string    `json:"permission_mode,omitempty"`
	Evidence       string    `json:"evidence,omitempty"`
	VerifiedAt     time.Time `json:"verified_at"`
	Reason         string    `json:"reason,omitempty"`
}

var (
	mu        sync.RWMutex
	results   []Result
	storePath string
)

func init() {
	// Optional on-disk cache for cross-process probe/preflight.
	if dir := os.Getenv("BROKER_CONTRACT_DIR"); dir != "" {
		storePath = filepath.Join(dir, "contract-results.json")
		_ = load()
	}
}

// Record appends a contract result and persists when a store path is configured.
func Record(r Result) {
	if r.VerifiedAt.IsZero() {
		r.VerifiedAt = time.Now().UTC()
	}
	mu.Lock()
	defer mu.Unlock()
	// Replace same harness/version/contract entry.
	out := results[:0]
	for _, existing := range results {
		if existing.Harness == r.Harness && existing.Version == r.Version && existing.Contract == r.Contract && existing.PermissionMode == r.PermissionMode {
			continue
		}
		out = append(out, existing)
	}
	results = append(out, r)
	_ = saveLocked()
}

// Latest returns the newest matching result, if any.
func Latest(harness, version, contract string) (Result, bool) {
	mu.RLock()
	defer mu.RUnlock()
	for i := len(results) - 1; i >= 0; i-- {
		r := results[i]
		if r.Harness == harness && r.Contract == contract {
			if version == "" || r.Version == version {
				return r, true
			}
		}
	}
	return Result{}, false
}

// SteerVerified reports whether a passed steer contract exists for harness/version.
func SteerVerified(harness, version string) bool {
	r, ok := Latest(harness, version, "steer_active_turn")
	return ok && r.Status == "passed"
}

func saveLocked() error {
	if storePath == "" {
		return nil
	}
	_ = os.MkdirAll(filepath.Dir(storePath), 0o700)
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(storePath, append(data, '\n'), 0o600)
}

func load() error {
	data, err := os.ReadFile(storePath)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &results)
}
