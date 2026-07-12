package adapter

import "testing"

func TestCapabilityModelHasIndependentControlSignals(t *testing.T) {
	caps := Capabilities{BidirectionalStream: true, ResumeSession: false, SteerActiveTurn: true}
	if !caps.BidirectionalStream || caps.ResumeSession || !caps.SteerActiveTurn {
		t.Fatal("capabilities must remain independent, not inferred from one another")
	}
}
