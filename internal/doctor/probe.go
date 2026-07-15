package doctor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/project"
	"github.com/vnai/subagent-broker/internal/storage"
)

// Run performs static probing by default and the explicit live smoke when
// cfg.Mode is ModeSmoke. Harnesses are always processed serially.
func Run(ctx context.Context, registry *adapter.Registry, cfg Config) (RunResult, error) {
	if registry == nil {
		return RunResult{}, fmt.Errorf("doctor adapter registry is required")
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeProbe
	}
	if cfg.Mode != ModeProbe && cfg.Mode != ModeSmoke {
		return RunResult{}, fmt.Errorf("unsupported doctor mode %q", cfg.Mode)
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 2 * time.Minute
	}
	if cfg.Timeout < 0 {
		return RunResult{}, fmt.Errorf("doctor timeout must be positive")
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if strings.TrimSpace(cfg.BrokerHome) == "" {
		return RunResult{}, fmt.Errorf("doctor broker home is required")
	}
	layout, err := storage.NewLayout(cfg.BrokerHome)
	if err != nil {
		return RunResult{}, err
	}
	now := cfg.Now().UTC()
	runIDValue, err := project.NewRunID(now)
	if err != nil {
		return RunResult{}, err
	}
	doctorRunID := "doctor-" + string(runIDValue)
	evidenceDir := filepath.Join(layout.Home, "doctor", "runs", doctorRunID)
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		return RunResult{}, fmt.Errorf("create Doctor evidence directory: %w", err)
	}

	descriptors := registry.Descriptors()
	selected := selectDescriptors(descriptors, cfg.Harnesses)
	result := RunResult{SchemaVersion: SchemaVersion, DoctorRunID: doctorRunID, Mode: cfg.Mode, EvidenceDir: evidenceDir, Harnesses: make([]HarnessResult, 0, len(selected))}
	for _, descriptor := range selected {
		value, ok := registry.Get(descriptor.Name)
		if !ok {
			result.Harnesses = append(result.Harnesses, missingHarnessResult(result.DoctorRunID, descriptor))
			continue
		}
		probeCtx, probeCancel := context.WithTimeout(ctx, cfg.Timeout)
		item := runProbe(probeCtx, result.DoctorRunID, value, descriptor, cfg)
		probeCancel()
		if cfg.Mode == ModeSmoke && item.ProbeStatus == "passed" {
			item = runSmoke(ctx, result.DoctorRunID, value, descriptor, item, cfg, evidenceDir)
		} else if cfg.Mode == ModeSmoke {
			item.ProtocolSmokeStatus = "not_run"
			item.IdentityStatus = IdentityUnavailable
			item.WorkspaceStatus = "not_run"
			item.CleanupStatus = "not_run"
			item.OverallStatus = "failed"
		}
		if item.Artifacts.HarnessDir == "" {
			item.Artifacts.HarnessDir = filepath.Join(evidenceDir, safeSegment(string(descriptor.Name)))
			item.Artifacts.EvidenceJSON = filepath.Join(item.Artifacts.HarnessDir, "evidence.json")
			item.Artifacts.EvidenceMarkdown = filepath.Join(item.Artifacts.HarnessDir, "evidence.md")
		}
		result.Harnesses = append(result.Harnesses, item)
	}
	sort.Slice(result.Harnesses, func(i, j int) bool { return result.Harnesses[i].Harness < result.Harnesses[j].Harness })
	result = sanitizeRunResult(result)
	if err := PersistRun(evidenceDir, result); err != nil {
		return result, err
	}
	if anyFailed(result) {
		return result, fmt.Errorf("one or more selected Harness checks failed")
	}
	return result, nil
}

func selectDescriptors(descriptors []adapter.Descriptor, requested []adapter.HarnessName) []adapter.Descriptor {
	byName := make(map[adapter.HarnessName]adapter.Descriptor, len(descriptors))
	for _, descriptor := range descriptors {
		byName[descriptor.Name] = descriptor
	}
	if len(requested) == 0 {
		selected := append([]adapter.Descriptor(nil), descriptors...)
		sort.Slice(selected, func(i, j int) bool { return selected[i].Name < selected[j].Name })
		return selected
	}
	selected := make([]adapter.Descriptor, 0, len(requested))
	seen := map[adapter.HarnessName]bool{}
	for _, name := range requested {
		if seen[name] {
			continue
		}
		seen[name] = true
		if descriptor, ok := byName[name]; ok {
			selected = append(selected, descriptor)
		} else {
			selected = append(selected, adapter.Descriptor{Name: name})
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Name < selected[j].Name })
	return selected
}

func runProbe(ctx context.Context, runID string, harness adapter.Adapter, descriptor adapter.Descriptor, cfg Config) HarnessResult {
	started := cfg.Now().UTC()
	probe, probeErr := harness.Probe(ctx, adapter.ProbeRequest{Executable: cfg.Executable})
	if probeErr != nil {
		probe.Compatibility = "probe_failed"
		probe.Warnings = append(probe.Warnings, probeErr.Error())
	}
	probe.Warnings = sanitizeStrings(probe.Warnings)
	status := probeStatusForDescriptor(descriptor, probe)
	item := HarnessResult{
		SchemaVersion: SchemaVersion, DoctorRunID: runID, Harness: descriptor.Name,
		AdapterVersion: descriptor.AdapterVersion, HarnessVersion: probe.Version,
		StartedAt: started, EndedAt: cfg.Now().UTC(), Implemented: descriptor.RuntimeImplemented,
		Compatibility: probe.Compatibility, Installed: probe.Installed, Authenticated: probe.Authenticated,
		Capabilities: probe.Capabilities, Warnings: append([]string(nil), probe.Warnings...), Probe: probe,
		Descriptor: descriptor, ProbeStatus: status, ProtocolSmokeStatus: "not_run",
		IdentityStatus: "not_run", WorkspaceStatus: "not_run", CleanupStatus: "not_run",
		Stages: map[string]StageResult{"probe": {Status: status}},
	}
	item.CapabilityEvidence = CapabilityEvidenceForProbe(descriptor.Capabilities, probe.Capabilities)
	item.EndedAt = cfg.Now().UTC()
	item.Duration = item.EndedAt.Sub(item.StartedAt)
	item.OverallStatus = item.ProbeStatus
	return item
}

func missingHarnessResult(runID string, descriptor adapter.Descriptor) HarnessResult {
	return HarnessResult{
		SchemaVersion: SchemaVersion, DoctorRunID: runID, Harness: descriptor.Name,
		AdapterVersion: descriptor.AdapterVersion, Implemented: descriptor.RuntimeImplemented,
		Compatibility: "unavailable", ProbeStatus: "failed", ProtocolSmokeStatus: "not_run",
		IdentityStatus: IdentityUnavailable, WorkspaceStatus: "not_run", CleanupStatus: "not_run",
		OverallStatus: "failed", Errors: []string{"registered adapter is unavailable"}, Descriptor: descriptor,
		Stages:             map[string]StageResult{"probe": {Status: "failed", Error: "registered adapter is unavailable"}},
		CapabilityEvidence: CapabilityEvidenceForProbe(descriptor.Capabilities, adapter.Capabilities{}),
	}
}

func probeStatusForDescriptor(descriptor adapter.Descriptor, probe adapter.ProbeResult) string {
	if !descriptor.RuntimeImplemented || descriptor.Compatibility == "incompatible" {
		return "failed"
	}
	for _, version := range descriptor.KnownIncompatible {
		if strings.TrimSpace(version) != "" && strings.TrimSpace(version) == strings.TrimSpace(probe.Version) {
			return "failed"
		}
	}
	return probeStatus(probe)
}

func probeStatus(probe adapter.ProbeResult) string {
	if !probe.Installed || probe.Compatibility == "incompatible" || probe.Compatibility == "probe_failed" || probe.Compatibility == "unavailable" {
		return "failed"
	}
	if probe.Authenticated != nil && !*probe.Authenticated {
		return "failed"
	}
	return "passed"
}

func smokeProbeUsable(probe adapter.ProbeResult) bool {
	if probeStatus(probe) != "passed" {
		return false
	}
	// A live smoke must not turn an unknown authentication state into a live
	// request. Probe adapters must report usable authentication explicitly.
	return probe.Authenticated != nil && *probe.Authenticated
}

func anyFailed(result RunResult) bool {
	for _, item := range result.Harnesses {
		if item.OverallStatus != "passed" {
			return true
		}
	}
	return false
}

func sanitizeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, sanitizeText(value))
	}
	return out
}
