package adapter

import "testing"

func TestDeriveEffectiveDisablesPermissionWithoutHooks(t *testing.T) {
	declared := Capabilities{PermissionEvents: true, Hooks: true, SteerActiveTurn: true, BidirectionalStream: true, ResumeSession: true}
	probe := declared
	set := DeriveEffective(declared, probe, SessionConfigFact{
		PermissionMode: "acceptEdits",
		HooksInstalled: false,
		SteerVerified:  false,
	})
	if set.Effective.PermissionEvents || set.Effective.Hooks {
		t.Fatalf("expected permission/hooks false: %+v", set.Effective)
	}
	if set.Effective.SteerActiveTurn {
		t.Fatal("steer must be false without contract verification")
	}
	if len(set.Downgrades) == 0 {
		t.Fatal("expected downgrade reasons")
	}
}

func TestDeriveEffectiveNativePermissionWithoutHooks(t *testing.T) {
	// Codex/Grok/OpenCode: protocol-native permission events, no Claude hooks.
	declared := Capabilities{PermissionEvents: true, BidirectionalStream: true, ResumeSession: true}
	set := DeriveEffective(declared, declared, SessionConfigFact{
		PermissionMode:         "default",
		HooksInstalled:         false,
		NativePermissionEvents: true,
		SteerVerified:          true,
	})
	if !set.Effective.PermissionEvents {
		t.Fatalf("native permission events must remain effective without hooks: %+v downs=%v", set.Effective, set.Downgrades)
	}
	if set.Effective.Hooks {
		t.Fatal("hooks must stay false when not installed")
	}
}

func TestDeriveEffectiveClaudeHooksStillRequired(t *testing.T) {
	// Claude-style: PermissionEvents without NativePermissionEvents still needs hooks.
	declared := Capabilities{PermissionEvents: true, Hooks: true}
	set := DeriveEffective(declared, declared, SessionConfigFact{
		HooksInstalled:         false,
		NativePermissionEvents: false,
	})
	if set.Effective.PermissionEvents {
		t.Fatal("Claude hook-backed permission_events must be false without hooks")
	}
}

func TestDeriveEffectiveAllTrueWhenConfiguredAndVerified(t *testing.T) {
	declared := Capabilities{PermissionEvents: true, Hooks: true, SteerActiveTurn: true, BidirectionalStream: true}
	set := DeriveEffective(declared, declared, SessionConfigFact{
		HooksInstalled: true,
		SteerVerified:  true,
		MCPEnabled:     true,
	})
	if !set.Effective.PermissionEvents || !set.Effective.Hooks || !set.Effective.SteerActiveTurn {
		t.Fatalf("%+v", set.Effective)
	}
}

func TestCapabilityMapRoundTrip(t *testing.T) {
	in := Capabilities{ResumeSession: true, PermissionEvents: true, Hooks: true}
	out := CapabilitiesFromMap(CapabilityMap(in))
	if !out.ResumeSession || !out.PermissionEvents || !out.Hooks {
		t.Fatalf("%+v", out)
	}
}
