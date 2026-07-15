package doctor

import (
	"fmt"
	"strings"

	"github.com/vnai/subagent-broker/internal/adapter"
)

func RenderSummary(result RunResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Doctor %s\n\n- Schema: `%s`\n- Doctor run: `%s`\n- Mode: `%s`\n- Evidence directory: `%s`\n\n", result.Mode, result.SchemaVersion, result.DoctorRunID, result.Mode, result.EvidenceDir)
	for _, item := range result.Harnesses {
		fmt.Fprintf(&b, "## %s\n\n- Probe: `%s`\n- Protocol smoke: `%s`\n- Identity: `%s`\n- Workspace: `%s`\n- Cleanup: `%s`\n- Overall: `%s`\n", item.Harness, item.ProbeStatus, item.ProtocolSmokeStatus, item.IdentityStatus, item.WorkspaceStatus, item.CleanupStatus, item.OverallStatus)
		if item.HarnessVersion != "" {
			fmt.Fprintf(&b, "- Harness version: `%s`\n", item.HarnessVersion)
		}
		if item.Result.SHA256 != "" {
			fmt.Fprintf(&b, "- Result status: `%s`\n- Result SHA-256: `%s`\n", item.Result.Status, item.Result.SHA256)
		}
		fmt.Fprintf(&b, "- Result observed: `%t`\n- Task ID match: `%t`\n- Worker ID match: `%t`\n", item.Result.Observed, item.TaskWorker.TaskIDMatch, item.TaskWorker.WorkerIDMatch)
		fmt.Fprintf(&b, "- Requested model: `%s`\n- Observed provider: `%s` (status `%s`, source `%s`)\n- Observed model: `%s` (status `%s`, source `%s`)\n", display(item.RuntimeIdentity.RequestedModel), display(item.RuntimeIdentity.ObservedProvider), item.RuntimeIdentity.ProviderStatus, display(string(item.RuntimeIdentity.ProviderSource)), display(item.RuntimeIdentity.ObservedModel), item.RuntimeIdentity.ModelStatus, display(string(item.RuntimeIdentity.ModelSource)))
		fmt.Fprintf(&b, "- Cleanup: tree exit confirmed=`%t`, PID reused=`%t`, orphan risk=`%t`, remaining PIDs=`%v`, Adapter termination attempted=`%t`, termination requested=`%t`, phase=`%s`\n", item.Cleanup.TreeExitConfirmed, item.Cleanup.PIDReused, item.Cleanup.OrphanRisk, item.Cleanup.RemainingPIDs, item.Cleanup.AdapterTerminateAttempted, item.Cleanup.TerminationRequested, display(item.Cleanup.TerminationPhase))
		if verified := capabilityNames(item.CapabilityEvidence.RuntimeVerified); len(verified) > 0 {
			fmt.Fprintf(&b, "- Runtime-verified capabilities: `%s`\n", strings.Join(verified, "`, `"))
		}
		if len(item.Events) > 0 {
			fmt.Fprintf(&b, "- Normalized event kinds: `%s`\n", strings.Join(item.Events, "`, `"))
		}
		if len(item.CapabilityEvidence.NotExercised) > 0 {
			fmt.Fprintf(&b, "- Capabilities not exercised: `%s`\n", strings.Join(item.CapabilityEvidence.NotExercised, ", "))
		}
		if len(item.CapabilityEvidence.Contradicted) > 0 {
			fmt.Fprintf(&b, "- Contradicted capabilities: `%s`\n", strings.Join(item.CapabilityEvidence.Contradicted, ", "))
		}
		if len(item.Workspace.ChangedPaths) > 0 {
			fmt.Fprintf(&b, "- Workspace changes: `%s`\n", strings.Join(item.Workspace.ChangedPaths, ", "))
		}
		if len(item.Warnings) > 0 {
			fmt.Fprintf(&b, "- Warnings: %s\n", strings.Join(item.Warnings, "; "))
		}
		if len(item.Errors) > 0 {
			fmt.Fprintf(&b, "- Errors: %s\n", strings.Join(item.Errors, "; "))
		}
		fmt.Fprintf(&b, "- Evidence JSON: `%s`\n- Evidence Markdown: `%s`\n\n", item.Artifacts.EvidenceJSON, item.Artifacts.EvidenceMarkdown)
	}
	return b.String()
}

func RenderHarnessMarkdown(item HarnessResult) string {
	return RenderSummary(RunResult{SchemaVersion: item.SchemaVersion, DoctorRunID: item.DoctorRunID, Mode: ModeSmoke, Harnesses: []HarnessResult{item}})
}

func display(value string) string {
	if strings.TrimSpace(value) == "" {
		return "unavailable"
	}
	return value
}

func capabilityNames(capabilities adapter.Capabilities) []string {
	values := make([]string, 0)
	for _, name := range adapter.CapabilityNames() {
		if capabilities.Has(name) {
			values = append(values, name)
		}
	}
	return values
}
