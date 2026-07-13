package opencode

import "github.com/vnai/subagent-broker/internal/adapter"

func Descriptor() adapter.Descriptor {
	return adapter.Descriptor{
		Name:               adapter.HarnessOpenCode,
		AdapterVersion:     "phase4-opencode-server",
		TestedMinVersion:   "1.17.15",
		TestedMaxVersion:   "1.17.15",
		Compatibility:      "verified",
		RuntimeImplemented: true,
		Capabilities: adapter.Capabilities{
			StructuredStream: true, BidirectionalStream: true, ResumeSession: true,
			InterruptTurn: true, StructuredFinalOutput: true, PermissionEvents: true,
			DiffEvents: true, UsageEvents: true, NativeSubagents: true,
			NativeServerMode: true, SessionHistory: true,
		},
	}
}
