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
