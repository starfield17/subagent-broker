package claude

import "github.com/vnai/subagent-broker/internal/adapter"

func Descriptor() adapter.Descriptor {
	return adapter.Descriptor{
		Name: adapter.HarnessClaudeCode, AdapterVersion: "phase0", Compatibility: "compatibility_unverified",
		RuntimeImplemented: false,
		Capabilities:       adapter.Capabilities{StructuredStream: true, BidirectionalStream: true, ResumeSession: true, InterruptTurn: true, StructuredFinalOutput: true, PermissionEvents: true, DiffEvents: true, UsageEvents: true, NativeSubagents: true, Hooks: true, SessionHistory: true},
	}
}
