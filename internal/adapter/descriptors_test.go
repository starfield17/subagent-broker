package adapter_test

import (
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/adapter/claude"
	"github.com/vnai/subagent-broker/internal/adapter/codex"
	"github.com/vnai/subagent-broker/internal/adapter/grok"
	"github.com/vnai/subagent-broker/internal/adapter/opencode"
)

func TestFourHarnessesDeclareCapabilitiesIndependently(t *testing.T) {
	descriptors := []adapter.Descriptor{claude.Descriptor(), codex.Descriptor(), grok.Descriptor(), opencode.Descriptor()}
	seen := map[adapter.HarnessName]bool{}
	for _, d := range descriptors {
		if seen[d.Name] {
			t.Fatalf("duplicate descriptor %s", d.Name)
		}
		seen[d.Name] = true
		if d.RuntimeImplemented {
			t.Fatalf("real adapter %s must remain unimplemented in Phase 0", d.Name)
		}
		if d.Compatibility != "compatibility_unverified" {
			t.Fatalf("descriptor %s must be explicit about unverified compatibility", d.Name)
		}
	}
	if !codex.Descriptor().Capabilities.SteerActiveTurn || claude.Descriptor().Capabilities.SteerActiveTurn {
		t.Fatal("capability declarations should be per-Harness, not copied wholesale")
	}
}
