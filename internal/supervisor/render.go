package supervisor

import (
	"fmt"
	"strings"
	"time"
)

func renderStatus(snapshot Snapshot) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Run: %s\nStatus: %s\nWave: %d/%d %s / %s\nPending decisions: %d\nUpdated: %s\n", snapshot.Run.RunID, snapshot.Run.Status, snapshot.Wave.Ordinal, len(snapshot.Waves), snapshot.Wave.WaveID, snapshot.Wave.Status, len(snapshot.Messages), snapshot.UpdatedAt.UTC().Format(time.RFC3339))
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
			if runtime.Stall != nil {
				fmt.Fprintf(&b, "  progress assessment: %s\n  stall confidence: %s\n  stall reason: %s\n", runtime.Stall.State, runtime.Stall.Confidence, runtime.Stall.Reason)
				for _, evidence := range runtime.Stall.Evidence {
					fmt.Fprintf(&b, "  stall evidence: %s\n", evidence)
				}
			}
		}
		if runtime.ReportPath != "" {
			fmt.Fprintf(&b, "  report: %s\n", runtime.ReportPath)
		}
		if runtime.LastError != "" {
			fmt.Fprintf(&b, "  error: %s\n", runtime.LastError)
		}
		fmt.Fprintf(&b, "  scope: %s\n", strings.Join(runtime.Task.WriteScope, ", "))
		b.WriteString("\n")
	}
	return b.String()
}

// RenderStatus exposes the same projection used for status.md so IPC-first CLI
// reads can render the live Snapshot without trusting a stale markdown file.
func RenderStatus(snapshot Snapshot) string { return renderStatus(snapshot) }

func renderRunSummary(snapshot Snapshot) string {
	var b strings.Builder
	b.WriteString("# Run Summary\n\n")
	fmt.Fprintf(&b, "## Overall Status\n\n%s\n\n", snapshot.Run.Status)
	b.WriteString("## Wave Status\n\n")
	for _, item := range snapshot.Waves {
		fmt.Fprintf(&b, "- `%s`: %s", item.WaveID, item.Status)
		if item.BarrierResult != "" {
			fmt.Fprintf(&b, " (%s)", item.BarrierResult)
		}
		b.WriteString("\n")
	}
	b.WriteString("\n")
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
	b.WriteString("## Pending Decisions\n\n")
	if len(snapshot.Messages) == 0 {
		b.WriteString("- None.\n")
	} else {
		for _, item := range snapshot.Messages {
			fmt.Fprintf(&b, "- `%s` / task `%s` / %s\n", item.MessageID, item.TaskID, item.Type)
		}
	}
	b.WriteString("\n## Scope Audit\n\n- See each Wave `barrier.md` and `verification.json`.\n\n## Integration Verification\n\n")
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
