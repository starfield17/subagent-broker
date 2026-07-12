package report

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/storage"
)

const SchemaVersion = "v1alpha1"

type Status string

const (
	StatusSucceeded Status = "succeeded"
	StatusPartial   Status = "partial"
	StatusBlocked   Status = "blocked"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

type Validation struct {
	Command string `json:"command"`
	Passed  bool   `json:"passed"`
	Details string `json:"details,omitempty"`
}

func (v *Validation) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if strings.HasPrefix(trimmed, "\"") {
		var text string
		if err := json.Unmarshal(data, &text); err != nil {
			return err
		}
		lower := strings.ToLower(text)
		passed := !strings.Contains(lower, "failed") &&
			!strings.Contains(lower, "failure") &&
			!strings.Contains(lower, "not run") &&
			!strings.Contains(lower, "error")
		*v = Validation{Command: text, Passed: passed}
		return nil
	}

	type validationAlias Validation
	var decoded validationAlias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*v = Validation(decoded)
	return nil
}

type ScopeExpansion struct {
	Paths               []string `json:"paths"`
	Reason              string   `json:"reason"`
	Consequence         string   `json:"consequence"`
	PartialWriteMade    bool     `json:"partial_write_made"`
	RelatedTasks        []string `json:"related_tasks,omitempty"`
	SuggestedResolution string   `json:"suggested_resolution,omitempty"`
}

type Envelope struct {
	SchemaVersion               string          `json:"schema_version"`
	TaskID                      string          `json:"task_id"`
	WorkerID                    string          `json:"worker_id"`
	Status                      Status          `json:"status"`
	Summary                     string          `json:"summary"`
	WorkCompleted               []string        `json:"work_completed"`
	FilesChanged                []string        `json:"files_changed"`
	NoFilesChangedReason        string          `json:"no_files_changed_reason,omitempty"`
	Validation                  []Validation    `json:"validation"`
	ValidationNotRunReason      string          `json:"validation_not_run_reason,omitempty"`
	RemainingWork               []string        `json:"remaining_work"`
	BlockingIssues              []string        `json:"blocking_issues"`
	StopReason                  string          `json:"stop_reason,omitempty"`
	FailureStage                string          `json:"failure_stage,omitempty"`
	ErrorSummary                string          `json:"error_summary,omitempty"`
	WorkspaceState              string          `json:"workspace_state,omitempty"`
	ScopeExpansion              *ScopeExpansion `json:"scope_expansion,omitempty"`
	ScopeViolationsSelfReported []string        `json:"scope_violations_self_reported"`
	Risks                       []string        `json:"risks"`
	HandoffNotes                []string        `json:"handoff_notes"`
}

func ValidateEnvelope(e Envelope) error {
	var problems []string
	if e.SchemaVersion != SchemaVersion {
		problems = append(problems, "unsupported or missing schema_version")
	}
	if strings.TrimSpace(e.TaskID) == "" || strings.TrimSpace(e.WorkerID) == "" {
		problems = append(problems, "task_id and worker_id are required")
	}
	if strings.TrimSpace(e.Summary) == "" {
		problems = append(problems, "summary is required")
	}
	if len(e.FilesChanged) == 0 && strings.TrimSpace(e.NoFilesChangedReason) == "" {
		problems = append(problems, "state changed files or explain why no files changed")
	}
	if len(e.Validation) == 0 && strings.TrimSpace(e.ValidationNotRunReason) == "" {
		problems = append(problems, "state validation results or explain why validation was not run")
	}
	switch e.Status {
	case StatusSucceeded:
		if len(e.WorkCompleted) == 0 {
			problems = append(problems, "succeeded requires work_completed")
		}
		if len(e.RemainingWork) > 0 || len(e.BlockingIssues) > 0 {
			problems = append(problems, "succeeded cannot contain remaining work or blocking issues")
		}
	case StatusPartial:
		if len(e.WorkCompleted) == 0 || len(e.RemainingWork) == 0 || strings.TrimSpace(e.StopReason) == "" || strings.TrimSpace(e.WorkspaceState) == "" {
			problems = append(problems, "partial requires completed work, remaining work, stop reason, and workspace state")
		}
	case StatusBlocked:
		if len(e.BlockingIssues) == 0 || strings.TrimSpace(e.StopReason) == "" || strings.TrimSpace(e.WorkspaceState) == "" {
			problems = append(problems, "blocked requires blocking issues, stop reason, and workspace state")
		}
	case StatusFailed:
		if strings.TrimSpace(e.FailureStage) == "" || strings.TrimSpace(e.ErrorSummary) == "" || strings.TrimSpace(e.WorkspaceState) == "" || len(e.HandoffNotes) == 0 {
			problems = append(problems, "failed requires failure stage, error summary, workspace state, and handoff notes")
		}
	case StatusCancelled:
		if strings.TrimSpace(e.StopReason) == "" || strings.TrimSpace(e.WorkspaceState) == "" {
			problems = append(problems, "cancelled requires stop reason and workspace state")
		}
	default:
		problems = append(problems, fmt.Sprintf("invalid status %q", e.Status))
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("invalid result envelope: %s", strings.Join(problems, "; "))
	}
	return nil
}

// Meta is durable report identity + content integrity binding.
type Meta struct {
	SchemaVersion  string    `json:"schema_version"`
	TaskID         string    `json:"task_id"`
	WorkerID       string    `json:"worker_id"`
	AttemptNumber  int       `json:"attempt_number,omitempty"`
	Status         Status    `json:"status"`
	EnvelopeHash   string    `json:"envelope_hash,omitempty"`
	MarkdownHash   string    `json:"markdown_hash,omitempty"`
	PublishedAt    time.Time `json:"published_at"`
	// Unverified is set for legacy reports that lack attempt/hash binding.
	Unverified bool `json:"unverified,omitempty"`
}

// HashBytes returns a stable hex sha256 of content.
func HashBytes(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// CanonicalEnvelopeJSON returns deterministic JSON for envelope hashing.
func CanonicalEnvelopeJSON(e Envelope) ([]byte, error) {
	return json.Marshal(e)
}

// Publish atomically publishes report.meta.json then report.md (formal marker).
// attemptNumber binds the report to the Worker Attempt that produced it.
// report.envelope.json is written for Barrier re-hash verification.
func Publish(taskDir string, e Envelope, attemptNumber int, now time.Time) error {
	if err := ValidateEnvelope(e); err != nil {
		return err
	}
	if attemptNumber <= 0 {
		return fmt.Errorf("attempt_number is required for report publication")
	}
	envelopeJSON, err := CanonicalEnvelopeJSON(e)
	if err != nil {
		return err
	}
	markdown := RenderMarkdown(e)
	meta := Meta{
		SchemaVersion: SchemaVersion,
		TaskID:        e.TaskID,
		WorkerID:      e.WorkerID,
		AttemptNumber: attemptNumber,
		Status:        e.Status,
		EnvelopeHash:  HashBytes(envelopeJSON),
		MarkdownHash:  HashBytes([]byte(markdown)),
		PublishedAt:   now.UTC(),
	}
	metaJSON, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	// Envelope + meta first; report.md is the formal publication marker.
	if err := storage.AtomicWriteFile(filepath.Join(taskDir, "report.envelope.json"), append(envelopeJSON, '\n'), 0o600); err != nil {
		return err
	}
	if err := storage.AtomicWriteFile(filepath.Join(taskDir, "report.meta.json"), append(metaJSON, '\n'), 0o600); err != nil {
		return err
	}
	return storage.AtomicWriteFile(filepath.Join(taskDir, "report.md"), []byte(markdown), 0o600)
}

// ErrReportIdentityMismatch is returned when meta fields disagree with the
// canonical envelope or expected frozen report identity.
var ErrReportIdentityMismatch = fmt.Errorf("report identity mismatch")

// VerifyDiskArtifacts re-reads on-disk report artifacts and validates hashes
// plus meta↔envelope field binding. Envelope is the canonical status source.
func VerifyDiskArtifacts(taskDir string) (Meta, Envelope, error) {
	metaPath := filepath.Join(taskDir, "report.meta.json")
	mdPath := filepath.Join(taskDir, "report.md")
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return Meta{}, Envelope{}, err
	}
	mdData, err := os.ReadFile(mdPath)
	if err != nil {
		return Meta{}, Envelope{}, err
	}
	var meta Meta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return Meta{}, Envelope{}, fmt.Errorf("decode report.meta.json: %w", err)
	}
	var envelope Envelope
	envPath := filepath.Join(taskDir, "report.envelope.json")
	envData, envErr := os.ReadFile(envPath)
	if envErr == nil {
		if err := json.Unmarshal(envData, &envelope); err != nil {
			return meta, Envelope{}, fmt.Errorf("decode report.envelope.json: %w", err)
		}
	}

	// Legacy reports without hash/attempt are unverified.
	if meta.AttemptNumber <= 0 || meta.EnvelopeHash == "" || meta.MarkdownHash == "" {
		meta.Unverified = true
		return meta, envelope, nil
	}
	if envErr != nil {
		return meta, envelope, fmt.Errorf("report.envelope.json missing for hashed report: %w", envErr)
	}

	// Meta must bind to the canonical envelope (status/task/worker).
	if meta.TaskID != envelope.TaskID || meta.WorkerID != envelope.WorkerID || meta.Status != envelope.Status {
		return meta, envelope, fmt.Errorf("%w: meta(task=%q worker=%q status=%q) != envelope(task=%q worker=%q status=%q)",
			ErrReportIdentityMismatch, meta.TaskID, meta.WorkerID, meta.Status, envelope.TaskID, envelope.WorkerID, envelope.Status)
	}

	if got := HashBytes(mdData); got != meta.MarkdownHash {
		return meta, envelope, fmt.Errorf("report.md hash mismatch: meta=%s disk=%s", meta.MarkdownHash, got)
	}
	// Prefer hash of canonical envelope JSON; accept on-disk bytes with optional trailing newline.
	if raw, cErr := CanonicalEnvelopeJSON(envelope); cErr == nil {
		if HashBytes(raw) != meta.EnvelopeHash &&
			HashBytes(bytesTrimTrailingNewline(envData)) != meta.EnvelopeHash &&
			HashBytes(envData) != meta.EnvelopeHash {
			return meta, envelope, fmt.Errorf("report.envelope.json hash mismatch: meta=%s", meta.EnvelopeHash)
		}
	} else {
		return meta, envelope, cErr
	}
	return meta, envelope, nil
}

func bytesTrimTrailingNewline(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\n' {
		return data[:len(data)-1]
	}
	return data
}

func RenderMarkdown(e Envelope) string {
	var b strings.Builder
	b.WriteString("# Task Report\n\n## Status\n\n" + string(e.Status) + "\n\n")
	b.WriteString("## Completed Work\n\n")
	writeItems(&b, e.WorkCompleted, e.Summary)
	b.WriteString("## Changed Files\n\n")
	writeItems(&b, e.FilesChanged, e.NoFilesChangedReason)
	b.WriteString("## Verification\n\n")
	if len(e.Validation) == 0 {
		b.WriteString(e.ValidationNotRunReason + "\n\n")
	} else {
		for _, v := range e.Validation {
			result := "failed"
			if v.Passed {
				result = "passed"
			}
			fmt.Fprintf(&b, "- `%s`: %s", v.Command, result)
			if v.Details != "" {
				fmt.Fprintf(&b, " — %s", v.Details)
			}
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	b.WriteString("## Remaining Work\n\n")
	writeItems(&b, e.RemainingWork, "None.")
	b.WriteString("## Risks and Notes\n\n")
	writeItems(&b, e.Risks, "No known risks.")
	b.WriteString("## Handoff Notes\n\n")
	writeItems(&b, e.HandoffNotes, "No additional handoff notes.")
	return b.String()
}

func writeItems(b *strings.Builder, items []string, fallback string) {
	if len(items) == 0 {
		b.WriteString(fallback + "\n\n")
		return
	}
	for _, item := range items {
		fmt.Fprintf(b, "- %s\n", item)
	}
	b.WriteString("\n")
}
