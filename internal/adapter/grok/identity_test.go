package grok

import (
	"context"
	"sync"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
)

func TestRuntimeIdentityParsesNativeACPMetadata(t *testing.T) {
	a := New("grok")
	state := &sessionState{
		acpSessionID: "grok-native",
		runtimeIdentity: adapter.RuntimeIdentity{
			RequestedModel: "requested-model",
			ProviderSource: adapter.EvidenceUnavailable,
			ModelSource:    adapter.EvidenceUnavailable,
		},
	}
	a.mu.Lock()
	a.sessions[state.acpSessionID] = state
	a.mu.Unlock()

	a.captureRuntimeIdentity(state, []byte(`{"session":{"providerId":"xai","modelId":"grok-native-model"}}`))
	identity, err := a.RuntimeIdentity(context.Background(), state.acpSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if identity.RequestedModel != "requested-model" || identity.ObservedProvider != "xai" || identity.ObservedModel != "grok-native-model" {
		t.Fatalf("unexpected identity: %+v", identity)
	}
	if identity.ProviderSource != adapter.EvidenceNativeProtocol || identity.ModelSource != adapter.EvidenceNativeProtocol {
		t.Fatalf("native evidence sources missing: %+v", identity)
	}
}

func TestRuntimeIdentityMalformedACPMetadataDoesNotInventValues(t *testing.T) {
	state := &sessionState{runtimeIdentity: adapter.RuntimeIdentity{RequestedModel: "requested-model", ProviderSource: adapter.EvidenceUnavailable, ModelSource: adapter.EvidenceUnavailable}}
	a := New("grok")
	a.captureRuntimeIdentity(state, []byte(`{"result":{"providerId":`))
	if state.runtimeIdentity.ObservedModel != "" || state.runtimeIdentity.ObservedProvider != "" {
		t.Fatalf("malformed metadata invented identity: %+v", state.runtimeIdentity)
	}
}

func TestRuntimeIdentitySurvivesTerminalResultUntilQueried(t *testing.T) {
	a := New("grok")
	state := &sessionState{
		acpSessionID: "grok-terminal",
		currentTurn:  &turnResult{generation: 1, ready: make(chan struct{})},
		runtimeIdentity: adapter.RuntimeIdentity{
			RequestedModel: "requested-model",
			ProviderSource: adapter.EvidenceUnavailable,
			ModelSource:    adapter.EvidenceUnavailable,
		},
	}
	a.mu.Lock()
	a.sessions[state.acpSessionID] = state
	a.mu.Unlock()
	a.captureRuntimeIdentity(state, []byte(`{"prompt":{"providerId":"xai","modelId":"grok-terminal-model"}}`))
	a.finalizeGeneration(state, 1, true)
	identity, err := a.RuntimeIdentity(context.Background(), state.acpSessionID)
	if err != nil || identity.ObservedModel != "grok-terminal-model" {
		t.Fatalf("terminal result lost native identity: %+v, %v", identity, err)
	}
}

func TestRuntimeIdentityConcurrentACPEventAndQueryIsRaceSafe(t *testing.T) {
	a := New("grok")
	state := &sessionState{acpSessionID: "grok-race", runtimeIdentity: adapter.RuntimeIdentity{RequestedModel: "requested-model"}}
	a.mu.Lock()
	a.sessions[state.acpSessionID] = state
	a.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			a.captureRuntimeIdentity(state, []byte(`{"providerId":"xai","modelId":"grok-native-model"}`))
		}()
		go func() {
			defer wg.Done()
			_, _ = a.RuntimeIdentity(context.Background(), state.acpSessionID)
		}()
	}
	wg.Wait()
	identity, err := a.RuntimeIdentity(context.Background(), state.acpSessionID)
	if err != nil || identity.ObservedProvider != "xai" || identity.ObservedModel != "grok-native-model" {
		t.Fatalf("identity did not survive concurrent access: %+v, %v", identity, err)
	}
}
