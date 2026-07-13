package codex

import "github.com/vnai/subagent-broker/internal/adapter"

func Descriptor() adapter.Descriptor {
	return adapter.Descriptor{
		Name:               adapter.HarnessCodex,
		AdapterVersion:     "phase4-codex-app-server",
		TestedMinVersion:   "0.144.1",
		TestedMaxVersion:   "0.144.1",
		Compatibility:      "verified",
		RuntimeImplemented: true,
		Capabilities: adapter.Capabilities{
			StructuredStream: true, BidirectionalStream: true, ResumeSession: true,
			SteerActiveTurn: true, InterruptTurn: true, StructuredFinalOutput: true,
			PermissionEvents: true, DiffEvents: true, UsageEvents: true,
			NativeSubagents: true, NativeServerMode: true, SessionHistory: true,
		},
	}
}
