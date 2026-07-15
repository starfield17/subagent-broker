package doctor

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/vnai/subagent-broker/internal/storage"
)

// PersistRun writes the compact machine record and Markdown summary atomically
// with mode 0600. Raw protocol payloads, environments, and credentials are not
// included; event and stderr artifacts are deliberately redacted projections.
func PersistRun(evidenceDir string, result RunResult) error {
	result = sanitizeRunResult(result)
	if err := storage.AtomicWriteJSON(filepath.Join(evidenceDir, "doctor.json"), result, 0o600); err != nil {
		return fmt.Errorf("write Doctor JSON: %w", err)
	}
	if err := storage.AtomicWriteFile(filepath.Join(evidenceDir, "summary.md"), []byte(RenderSummary(result)), 0o600); err != nil {
		return fmt.Errorf("write Doctor summary: %w", err)
	}
	for _, item := range result.Harnesses {
		harnessDir := item.Artifacts.HarnessDir
		if harnessDir == "" {
			harnessDir = filepath.Join(evidenceDir, safeSegment(string(item.Harness)))
		}
		if err := os.MkdirAll(harnessDir, 0o700); err != nil {
			return fmt.Errorf("create Harness evidence directory: %w", err)
		}
		value := item
		if value.Artifacts.EvidenceJSON == "" {
			value.Artifacts.HarnessDir = harnessDir
			value.Artifacts.EvidenceJSON = filepath.Join(harnessDir, "evidence.json")
			value.Artifacts.EvidenceMarkdown = filepath.Join(harnessDir, "evidence.md")
		}
		if err := storage.AtomicWriteJSON(value.Artifacts.EvidenceJSON, value, 0o600); err != nil {
			return fmt.Errorf("write Harness evidence JSON: %w", err)
		}
		if err := storage.AtomicWriteFile(value.Artifacts.EvidenceMarkdown, []byte(RenderHarnessMarkdown(value)), 0o600); err != nil {
			return fmt.Errorf("write Harness evidence Markdown: %w", err)
		}
		if value.Artifacts.NormalizedEvents != "" {
			if err := storage.AtomicWriteFile(value.Artifacts.NormalizedEvents, []byte(value.normalizedEventsLog), 0o600); err != nil {
				return fmt.Errorf("write normalized event journal: %w", err)
			}
		}
		if value.Artifacts.Stderr != "" {
			if err := storage.AtomicWriteFile(value.Artifacts.Stderr, []byte(value.stderrLog), 0o600); err != nil {
				return fmt.Errorf("write sanitized stderr log: %w", err)
			}
		}
	}
	return nil
}
