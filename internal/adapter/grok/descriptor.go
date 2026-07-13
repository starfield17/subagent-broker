package grok

import "github.com/vnai/subagent-broker/internal/adapter"

func Descriptor() adapter.Descriptor {
	return adapter.Descriptor{
		Name:               adapter.HarnessGrokBuild,
		AdapterVersion:     "phase4-grok-acp",
		TestedMinVersion:   "0.2.99",
		TestedMaxVersion:   "0.2.99",
		Compatibility:      "verified",
		RuntimeImplemented: true,
		Capabilities: adapter.Capabilities{
			StructuredStream: true, BidirectionalStream: true, ResumeSession: true,
			InterruptTurn: true, StructuredFinalOutput: true, PermissionEvents: true,
			UsageEvents: true, NativeSubagents: true, ACP: true, SessionHistory: true,
		},
	}
}
