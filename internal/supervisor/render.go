package supervisor

import (
	"fmt"
	"strings"
	"time"
)

func renderStatus(snapshot Snapshot) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Run: %s\nStatus: %s\nWave: %s / %s\nUpdated: %s\n", snapshot.Run.RunID, snapshot.Run.Status, snapshot.Wave.WaveID, snapshot.Wave.Status, snapshot.UpdatedAt.UTC().Format(time.RFC3339))
	if snapshot.LastError != "" {
		fmt.Fprintf(&b, "Last error: %s\n", snapshot.LastError)
	}
	b.WriteString("\n")
	for _, runtime := range snapshot.Tasks {
		fmt.Fprintf(&b, "%s\n", runtime.Task.TaskID)
		fmt.Fprintf(&b, "  task: %s\n", runtime.Task.Status)
		fmt.Fprintf(&b, "  process: %s\n", runtime.Dimensions.Process)
		fmt.Fprintf(&b, "  protocol: %s\n", runtime.Dimensions.Protocol)
		fmt.Fprintf(&b, "  progress: %s\n", runtime.Dimensions.Progress)
		if runtime.Worker != nil {
			fmt.Fprintf(&b, "  harness: %s\n", runtime.Worker.Harness)
			if !runtime.LastProgress.IsZero() {
				fmt.Fprintf(&b, "  last progress: %s\n", runtime.LastProgress.UTC().Format(time.RFC3339))
			}
		}
		if runtime.ReportPath != "" {
			fmt.Fprintf(&b, "  report: %s\n", runtime.ReportPath)
		}
		if runtime.LastError != "" {
			fmt.Fprintf(&b, "  error: %s\n", runtime.LastError)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func renderRunSummary(snapshot Snapshot) string {
	var b strings.Builder
	b.WriteString("# Run Summary\n\n")
	fmt.Fprintf(&b, "## Overall Status\n\n%s\n\n", snapshot.Run.Status)
	fmt.Fprintf(&b, "## Wave Status\n\n- `%s`: %s\n\n", snapshot.Wave.WaveID, snapshot.Wave.Status)
	b.WriteString("## Task Summary\n\n")
	for _, runtime := range snapshot.Tasks {
		fmt.Fprintf(&b, "### %s\n\n- Status: `%s`\n", runtime.Task.TaskID, runtime.Task.Status)
		if runtime.ReportPath != "" {
			fmt.Fprintf(&b, "- Report: `%s`\n", runtime.ReportPath)
		}
		if runtime.LastError != "" {
			fmt.Fprintf(&b, "- Error: %s\n", runtime.LastError)
		}
		b.WriteString("\n")
	}
	b.WriteString("## Pending Decisions\n\n- None.\n\n## Scope Audit\n\n- Phase 1 runs one Task; complete Wave scope auditing is scheduled for Phase 2.\n\n## Integration Verification\n\n")
	for _, runtime := range snapshot.Tasks {
		for _, result := range runtime.Validation {
			status := "failed"
			if result.Passed {
				status = "passed"
			}
			fmt.Fprintf(&b, "- `%s`: %s\n", result.Command, status)
		}
	}
	b.WriteString("\n## Recommended Next Step\n\n- Main Agent should review the report and final diff.\n")
	return b.String()
}
