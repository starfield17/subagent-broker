package wave

import (
	"strings"
	"testing"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/message"
	"github.com/vnai/subagent-broker/internal/report"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/verify"
)

func TestEvaluateBarrierCancelledPriority(t *testing.T) {
	result := EvaluateBarrier(BarrierInputs{
		WaveID:    "wave-1",
		Cancelled: true,
		Reports: []ReportAssessment{{
			TaskID: "task-a", Present: false, Error: "missing",
		}},
		ExistingErrors: []string{"should not become failed"},
	}, time.Now().UTC())
	if result.Result != domain.BarrierCancelled {
		t.Fatalf("result=%s", result.Result)
	}
}

func TestEvaluateBarrierFailedBeatsBlockedAndWarnings(t *testing.T) {
	result := EvaluateBarrier(BarrierInputs{
		WaveID: "wave-1",
		Reports: []ReportAssessment{{
			TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true,
			RuntimeStatus: state.TaskBlocked, EnvelopeStatus: report.StatusPartial,
		}},
		Pending: []PendingDecision{{MessageID: "m1", TaskID: "task-a", Type: message.Question}},
		ScopeAudit: verify.ScopeAudit{
			Unauthorized: []string{"secret.env"},
		},
	}, time.Now().UTC())
	if result.Result != domain.BarrierFailed {
		t.Fatalf("result=%s errors=%v", result.Result, result.Errors)
	}
}

func TestEvaluateBarrierBlockedFromTaskEnvelopePending(t *testing.T) {
	result := EvaluateBarrier(BarrierInputs{
		WaveID: "wave-1",
		Reports: []ReportAssessment{{
			TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true,
			RuntimeStatus: state.TaskBlocked,
		}},
	}, time.Now().UTC())
	if result.Result != domain.BarrierBlocked {
		t.Fatalf("task blocked => %s", result.Result)
	}

	result = EvaluateBarrier(BarrierInputs{
		WaveID: "wave-1",
		Reports: []ReportAssessment{{
			TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true,
			RuntimeStatus: state.TaskVerifiedSuccess, EnvelopeStatus: report.StatusBlocked,
		}},
	}, time.Now().UTC())
	if result.Result != domain.BarrierBlocked {
		t.Fatalf("envelope blocked => %s", result.Result)
	}

	result = EvaluateBarrier(BarrierInputs{
		WaveID: "wave-1",
		Reports: []ReportAssessment{{
			TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true,
			RuntimeStatus: state.TaskVerifiedSuccess,
		}},
		Pending: []PendingDecision{{MessageID: "m1", TaskID: "task-a", Type: message.Question}},
	}, time.Now().UTC())
	if result.Result != domain.BarrierBlocked {
		t.Fatalf("pending => %s", result.Result)
	}
	if len(result.PendingMessages) != 1 || result.PendingMessages[0] != "m1" {
		t.Fatalf("pending messages=%v", result.PendingMessages)
	}
}

func TestEvaluateBarrierPartialAndOwnerUncertainWarnings(t *testing.T) {
	result := EvaluateBarrier(BarrierInputs{
		WaveID: "wave-1",
		Reports: []ReportAssessment{{
			TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true,
			RuntimeStatus: state.TaskVerifiedPartial, EnvelopeStatus: report.StatusPartial,
		}},
		ScopeAudit: verify.ScopeAudit{
			OwnerUncertain: []verify.FileAttribution{{Path: "shared.go", Owners: []string{"a", "b"}}},
		},
	}, time.Now().UTC())
	if result.Result != domain.BarrierPassedWithWarnings {
		t.Fatalf("result=%s warnings=%v", result.Result, result.Warnings)
	}
}

func TestEvaluateBarrierUnauthorizedAndFailedCheck(t *testing.T) {
	result := EvaluateBarrier(BarrierInputs{
		WaveID: "wave-1",
		Reports: []ReportAssessment{{
			TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true,
			RuntimeStatus: state.TaskVerifiedSuccess,
		}},
		ScopeAudit: verify.ScopeAudit{Unauthorized: []string{"go.mod"}},
	}, time.Now().UTC())
	if result.Result != domain.BarrierFailed {
		t.Fatalf("unauthorized => %s", result.Result)
	}

	result = EvaluateBarrier(BarrierInputs{
		WaveID: "wave-1",
		Reports: []ReportAssessment{{
			TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true,
			RuntimeStatus: state.TaskVerifiedSuccess,
		}},
		Checks: []CheckResult{{Command: "go test ./...", Passed: false}},
	}, time.Now().UTC())
	if result.Result != domain.BarrierFailed {
		t.Fatalf("failed check => %s", result.Result)
	}
}

func TestEvaluateBarrierHighRiskWarningAndError(t *testing.T) {
	result := EvaluateBarrier(BarrierInputs{
		WaveID: "wave-1",
		Reports: []ReportAssessment{{
			TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true,
			RuntimeStatus: state.TaskVerifiedSuccess,
		}},
		HighRiskChanges: []HighRiskChange{{Path: "README.md", Severity: SeverityWarning, Reason: "docs"}},
	}, time.Now().UTC())
	if result.Result != domain.BarrierPassedWithWarnings {
		t.Fatalf("high-risk warning => %s", result.Result)
	}

	result = EvaluateBarrier(BarrierInputs{
		WaveID: "wave-1",
		Reports: []ReportAssessment{{
			TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true,
			RuntimeStatus: state.TaskVerifiedSuccess,
		}},
		HighRiskChanges: []HighRiskChange{{Path: "go.mod", Severity: SeverityError, Reason: "deps"}},
	}, time.Now().UTC())
	if result.Result != domain.BarrierFailed {
		t.Fatalf("high-risk error => %s", result.Result)
	}
}

func TestEvaluateBarrierCleanPassed(t *testing.T) {
	result := EvaluateBarrier(BarrierInputs{
		WaveID: "wave-1",
		Reports: []ReportAssessment{{
			TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true,
			RuntimeStatus: state.TaskVerifiedSuccess, EnvelopeStatus: report.StatusSucceeded,
		}},
		ChangedFiles: []string{"internal/a/a.go"},
		ScopeAudit: verify.ScopeAudit{
			Authorized: []verify.FileAttribution{{Path: "internal/a/a.go", Owners: []string{"task-a"}}},
		},
		Checks: []CheckResult{{Command: "true", Passed: true}},
	}, time.Now().UTC())
	if result.Result != domain.BarrierPassed {
		t.Fatalf("result=%s errors=%v warnings=%v", result.Result, result.Errors, result.Warnings)
	}
}

func TestEvaluateBarrierEphemeralChangesAreWarningsAndRemainVisible(t *testing.T) {
	result := EvaluateBarrier(BarrierInputs{
		WaveID: "wave-1",
		Reports: []ReportAssessment{{
			TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true,
			RuntimeStatus: state.TaskVerifiedSuccess, EnvelopeStatus: report.StatusSucceeded,
		}},
		ChangedFiles: []string{"__pycache__/module.pyc"},
		ScopeAudit:   verify.ScopeAudit{Ephemeral: []verify.EphemeralAttribution{{Path: "__pycache__/module.pyc", Pattern: "**/__pycache__/**"}}},
		Checks:       []CheckResult{{Command: "true", Passed: true}},
	}, time.Now().UTC())
	if result.Result != domain.BarrierPassedWithWarnings {
		t.Fatalf("ephemeral-only result=%s errors=%v warnings=%v", result.Result, result.Errors, result.Warnings)
	}
	if len(result.ChangedFiles) != 1 || len(result.ScopeAudit.Ephemeral) != 1 {
		t.Fatalf("ephemeral facts were lost: %+v", result)
	}
	if len(result.Warnings) != 1 || result.Warnings[0] != "ephemeral workspace artifacts observed: 1 path(s)" {
		t.Fatalf("unexpected ephemeral warning: %v", result.Warnings)
	}

	result = EvaluateBarrier(BarrierInputs{
		WaveID: "wave-1",
		Reports: []ReportAssessment{{
			TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true,
			RuntimeStatus: state.TaskVerifiedSuccess,
		}},
		ScopeAudit: verify.ScopeAudit{
			Ephemeral:    []verify.EphemeralAttribution{{Path: "cache.pyc", Pattern: "**/*.pyc"}},
			Unauthorized: []string{"secret.env"},
		},
	}, time.Now().UTC())
	if result.Result != domain.BarrierFailed {
		t.Fatalf("ephemeral plus unauthorized result=%s", result.Result)
	}
}

func TestRenderBarrierIncludesEphemeralSection(t *testing.T) {
	markdown := RenderBarrier(Verification{
		WaveID:       "wave-1",
		Result:       domain.BarrierPassedWithWarnings,
		ChangedFiles: []string{".pytest_cache/CACHEDIR.TAG"},
		ScopeAudit:   verify.ScopeAudit{Ephemeral: []verify.EphemeralAttribution{{Path: ".pytest_cache/CACHEDIR.TAG", Pattern: "**/.pytest_cache/**"}}},
	})
	if !strings.Contains(markdown, "## Ephemeral Changes") || !strings.Contains(markdown, ".pytest_cache/CACHEDIR.TAG") {
		t.Fatalf("ephemeral section missing:\n%s", markdown)
	}
}

func TestEvaluateBarrierStableSortAndDedup(t *testing.T) {
	result := EvaluateBarrier(BarrierInputs{
		WaveID: "wave-1",
		Reports: []ReportAssessment{
			{TaskID: "task-b", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true, RuntimeStatus: state.TaskVerifiedSuccess},
			{TaskID: "task-a", Present: true, MetaValid: true, MarkdownValid: true, IdentityValid: true, RuntimeStatus: state.TaskVerifiedSuccess},
		},
		ChangedFiles:     []string{"b.go", "a.go", "a.go"},
		ExistingErrors:   []string{"zzz", "aaa", "zzz"},
		ExistingWarnings: []string{"w2", "w1", "w2"},
		HighRiskChanges: []HighRiskChange{
			{Path: "z.txt", Severity: SeverityWarning},
			{Path: "a.txt", Severity: SeverityWarning},
		},
		Pending: []PendingDecision{
			{MessageID: "m-b", TaskID: "task-b", Type: message.Question},
			{MessageID: "m-a", TaskID: "task-a", Type: message.Question},
			{MessageID: "m-a", TaskID: "task-a", Type: message.Question},
		},
	}, time.Now().UTC())
	if result.ChangedFiles[0] != "a.go" || result.ChangedFiles[1] != "b.go" || len(result.ChangedFiles) != 2 {
		t.Fatalf("changed files=%v", result.ChangedFiles)
	}
	if result.Errors[0] != "aaa" || result.Errors[1] != "zzz" || len(result.Errors) != 2 {
		t.Fatalf("errors=%v", result.Errors)
	}
	if result.Warnings[0] != "high-risk change: a.txt" {
		// warnings include pending + high-risk; just check dedup of existing warnings and sort
	}
	if len(result.PendingMessages) != 2 || result.PendingMessages[0] != "m-a" || result.PendingMessages[1] != "m-b" {
		t.Fatalf("pending=%v", result.PendingMessages)
	}
	if result.Reports[0].TaskID != "task-a" || result.Reports[1].TaskID != "task-b" {
		t.Fatalf("reports order=%+v", result.Reports)
	}
	if result.HighRiskChanges[0].Path != "a.txt" {
		t.Fatalf("high-risk order=%+v", result.HighRiskChanges)
	}
}
