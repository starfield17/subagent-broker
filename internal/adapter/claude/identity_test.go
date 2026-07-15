package claude

import (
	"context"
	"sync"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/event"
)

func TestRuntimeIdentityUsesNativeMetadataAndKeepsRequestedModelSeparate(t *testing.T) {
	a := New("claude")
	state := &sessionState{
		id: "claude-native",
		runtimeIdentity: adapter.RuntimeIdentity{
			RequestedModel: "requested-model",
			ProviderSource: adapter.EvidenceUnavailable,
			ModelSource:    adapter.EvidenceUnavailable,
		},
	}
	a.mu.Lock()
	a.sessions[state.id] = state
	a.mu.Unlock()

	captureRuntimeIdentity(state, []byte(`{"type":"system","model":"claude-native-model","provider":"anthropic"}`))
	identity, err := a.RuntimeIdentity(context.Background(), state.id)
	if err != nil {
		t.Fatal(err)
	}
	if identity.RequestedModel != "requested-model" || identity.ObservedModel != "claude-native-model" || identity.ObservedProvider != "anthropic" {
		t.Fatalf("unexpected identity: %+v", identity)
	}
	if identity.ModelSource != adapter.EvidenceNativeProtocol || identity.ProviderSource != adapter.EvidenceNativeProtocol {
		t.Fatalf("native evidence sources missing: %+v", identity)
	}
}

func TestRuntimeIdentityMalformedMetadataDoesNotInventValues(t *testing.T) {
	state := &sessionState{
		runtimeIdentity: adapter.RuntimeIdentity{
			RequestedModel: "requested-model",
			ProviderSource: adapter.EvidenceUnavailable,
			ModelSource:    adapter.EvidenceUnavailable,
		},
	}
	captureRuntimeIdentity(state, []byte(`{"model":`))
	if state.runtimeIdentity.ObservedModel != "" || state.runtimeIdentity.ObservedProvider != "" {
		t.Fatalf("malformed metadata invented identity: %+v", state.runtimeIdentity)
	}
}

func TestRuntimeIdentitySurvivesTerminalResultUntilQueried(t *testing.T) {
	a := New("claude")
	state := &sessionState{
		id:          "claude-terminal",
		resultReady: make(chan struct{}),
		runtimeIdentity: adapter.RuntimeIdentity{
			RequestedModel: "requested-model",
			ProviderSource: adapter.EvidenceUnavailable,
			ModelSource:    adapter.EvidenceUnavailable,
		},
	}
	a.mu.Lock()
	a.sessions[state.id] = state
	a.mu.Unlock()
	captureRuntimeIdentity(state, []byte(`{"type":"system","model":"claude-terminal-model"}`))
	captureResult(state, adapter.NativeEvent{Kind: event.ResultSubmitted, Payload: []byte(`{"subtype":"success","result":"{}"}`)})
	identity, err := a.RuntimeIdentity(context.Background(), state.id)
	if err != nil || identity.ObservedModel != "claude-terminal-model" {
		t.Fatalf("terminal result lost native identity: %+v, %v", identity, err)
	}
}

func TestRuntimeIdentityConcurrentCaptureAndQueryIsRaceSafe(t *testing.T) {
	a := New("claude")
	state := &sessionState{id: "claude-race", runtimeIdentity: adapter.RuntimeIdentity{RequestedModel: "requested-model"}}
	a.mu.Lock()
	a.sessions[state.id] = state
	a.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			captureRuntimeIdentity(state, []byte(`{"model":"claude-native-model"}`))
		}()
		go func() {
			defer wg.Done()
			_, _ = a.RuntimeIdentity(context.Background(), state.id)
		}()
	}
	wg.Wait()
	identity, err := a.RuntimeIdentity(context.Background(), state.id)
	if err != nil || identity.ObservedModel != "claude-native-model" {
		t.Fatalf("identity did not survive concurrent access: %+v, %v", identity, err)
	}
}
