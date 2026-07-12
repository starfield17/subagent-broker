package adapter

import "strings"

// CapabilitySet is a layered view of what a harness can do in theory vs this session.
type CapabilitySet struct {
	Declared   Capabilities `json:"declared"`
	Probe      Capabilities `json:"probe,omitempty"`
	Configured Capabilities `json:"configured"`
	Effective  Capabilities `json:"effective"`
	// Downgrades explains why Effective differs from Declared.
	Downgrades []string `json:"downgrades,omitempty"`
}

// SessionConfigFact records what was actually installed for a session.
type SessionConfigFact struct {
	PermissionMode string `json:"permission_mode,omitempty"`
	HooksInstalled bool   `json:"hooks_installed"`
	MCPEnabled     bool   `json:"mcp_enabled"`
	SafeMode       bool   `json:"safe_mode"`
	// SteerVerified is true only after a real contract test (or fake that asserts it).
	// Descriptor claims alone never set this true.
	SteerVerified bool `json:"steer_verified"`
	// NextTurnDelivery is true when the adapter only supports next-turn inject.
	NextTurnDelivery bool `json:"next_turn_delivery"`
}

// DeriveEffective intersects declared ∩ probe ∩ configured session facts.
// probe may be zero-value (all false) meaning "no probe data" — in that case
// probe does not further restrict declared (caller should pass declared as probe
// when probe is unavailable, or pass probe results explicitly).
func DeriveEffective(declared, probe Capabilities, fact SessionConfigFact) CapabilitySet {
	set := CapabilitySet{
		Declared:   declared,
		Probe:      probe,
		Configured: Capabilities{},
	}

	// Configured starts from probe∩declared (what hardware/software allows).
	base := intersect(declared, probe)
	set.Configured = base

	// Session configuration further restricts.
	effective := base
	var downs []string

	if fact.SafeMode {
		if effective.PermissionEvents {
			effective.PermissionEvents = false
			downs = append(downs, "permission_events disabled by safe_mode")
		}
		if effective.Hooks {
			effective.Hooks = false
			downs = append(downs, "hooks disabled by safe_mode")
		}
	}

	// Permission events require hooks actually installed this session.
	if effective.PermissionEvents && !fact.HooksInstalled {
		effective.PermissionEvents = false
		downs = append(downs, "permission_events require installed hooks; hooks not installed for this session")
	}
	if effective.Hooks && !fact.HooksInstalled {
		effective.Hooks = false
		downs = append(downs, "hooks declared but not installed for this session")
	}

	// SteerActiveTurn requires explicit verification; otherwise downgrade.
	if effective.SteerActiveTurn && !fact.SteerVerified {
		effective.SteerActiveTurn = false
		downs = append(downs, "steer_active_turn not contract-verified; treating as unavailable")
		if fact.NextTurnDelivery || base.BidirectionalStream {
			// Bidirectional stream may still allow next-turn delivery.
		}
	}

	// ResumeSession is about adapter ability; native session presence is checked at route time.
	set.Configured = base
	// Reflect hooks install in configured view.
	set.Configured.Hooks = base.Hooks && fact.HooksInstalled
	set.Configured.PermissionEvents = base.PermissionEvents && fact.HooksInstalled
	set.Configured.SteerActiveTurn = base.SteerActiveTurn && fact.SteerVerified

	set.Effective = effective
	// Keep effective steer aligned with configured after verification gate.
	set.Effective.SteerActiveTurn = set.Configured.SteerActiveTurn
	set.Effective.Hooks = set.Configured.Hooks
	set.Effective.PermissionEvents = set.Configured.PermissionEvents
	set.Downgrades = downs
	return set
}

func intersect(a, b Capabilities) Capabilities {
	// If probe is zero (all false), treat as "no probe restriction" only when
	// b equals zeroCapabilities. Callers should pass declared as probe when
	// probe was not run.
	return Capabilities{
		StructuredStream:      a.StructuredStream && b.StructuredStream,
		BidirectionalStream:   a.BidirectionalStream && b.BidirectionalStream,
		ResumeSession:         a.ResumeSession && b.ResumeSession,
		SteerActiveTurn:       a.SteerActiveTurn && b.SteerActiveTurn,
		InterruptTurn:         a.InterruptTurn && b.InterruptTurn,
		StructuredFinalOutput: a.StructuredFinalOutput && b.StructuredFinalOutput,
		PermissionEvents:      a.PermissionEvents && b.PermissionEvents,
		DiffEvents:            a.DiffEvents && b.DiffEvents,
		UsageEvents:           a.UsageEvents && b.UsageEvents,
		NativeSubagents:       a.NativeSubagents && b.NativeSubagents,
		NativeServerMode:      a.NativeServerMode && b.NativeServerMode,
		ACP:                   a.ACP && b.ACP,
		Hooks:                 a.Hooks && b.Hooks,
		SessionHistory:        a.SessionHistory && b.SessionHistory,
	}
}

// CapabilityMap converts Capabilities to the map form stored on WorkerSession.
func CapabilityMap(value Capabilities) map[string]bool {
	return map[string]bool{
		"structured_stream":       value.StructuredStream,
		"bidirectional_stream":    value.BidirectionalStream,
		"resume_session":          value.ResumeSession,
		"steer_active_turn":       value.SteerActiveTurn,
		"interrupt_turn":          value.InterruptTurn,
		"structured_final_output": value.StructuredFinalOutput,
		"permission_events":       value.PermissionEvents,
		"diff_events":             value.DiffEvents,
		"usage_events":            value.UsageEvents,
		"hooks":                   value.Hooks,
		"session_history":         value.SessionHistory,
		"native_subagents":        value.NativeSubagents,
		"native_server_mode":      value.NativeServerMode,
		"acp":                     value.ACP,
	}
}

// CapabilitiesFromMap rebuilds Capabilities from a map (e.g. WorkerSession).
func CapabilitiesFromMap(m map[string]bool) Capabilities {
	if m == nil {
		return Capabilities{}
	}
	return Capabilities{
		StructuredStream:      m["structured_stream"],
		BidirectionalStream:   m["bidirectional_stream"],
		ResumeSession:         m["resume_session"],
		SteerActiveTurn:       m["steer_active_turn"],
		InterruptTurn:         m["interrupt_turn"],
		StructuredFinalOutput: m["structured_final_output"],
		PermissionEvents:      m["permission_events"],
		DiffEvents:            m["diff_events"],
		UsageEvents:           m["usage_events"],
		Hooks:                 m["hooks"],
		SessionHistory:        m["session_history"],
		NativeSubagents:       m["native_subagents"],
		NativeServerMode:      m["native_server_mode"],
		ACP:                   m["acp"],
	}
}

// RequiresPermissionRouting reports whether a task/config needs permission events.
func RequiresPermissionRouting(permissionMode string, requirePermission bool) bool {
	if requirePermission {
		return true
	}
	// Non-empty permission modes that expect broker-mediated allow/deny.
	mode := strings.TrimSpace(strings.ToLower(permissionMode))
	return mode == "default" || mode == "acceptedits" || mode == "plan" || mode == "bypasspermissions"
}
