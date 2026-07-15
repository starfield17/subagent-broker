package doctor

import "github.com/vnai/subagent-broker/internal/adapter"

// CapabilityEvidenceForProbe deliberately leaves runtime verification empty.
func CapabilityEvidenceForProbe(declared, probe adapter.Capabilities) adapter.CapabilityEvidence {
	evidence := adapter.CapabilityEvidence{Declared: declared, ProbeReported: probe}
	for _, name := range adapter.CapabilityNames() {
		if declared.Has(name) || probe.Has(name) {
			evidence.NotExercised = append(evidence.NotExercised, name)
		}
	}
	return evidence
}

// CapabilityEvidenceForSmoke records only the two structured capabilities
// exercised by the basic smoke. Other descriptor/probe claims stay explicitly
// not_exercised; they are not upgraded by session startup alone.
func CapabilityEvidenceForSmoke(declared, probe adapter.Capabilities, stages map[string]StageResult, events []string) adapter.CapabilityEvidence {
	evidence := adapter.CapabilityEvidence{Declared: declared, ProbeReported: probe}
	if stages["event_stream"].Status == "passed" && len(events) > 0 {
		evidence.RuntimeVerified.Set("structured_stream", true)
	}
	if stages["result_validation"].Status == "passed" {
		evidence.RuntimeVerified.Set("structured_final_output", true)
	}
	for _, name := range adapter.CapabilityNames() {
		if !declared.Has(name) && !probe.Has(name) {
			continue
		}
		if evidence.RuntimeVerified.Has(name) {
			continue
		}
		if (name == "structured_stream" || name == "structured_final_output") && declared.Has(name) {
			evidence.Contradicted = append(evidence.Contradicted, name)
		} else {
			evidence.NotExercised = append(evidence.NotExercised, name)
		}
	}
	return evidence
}
