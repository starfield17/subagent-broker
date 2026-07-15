package codex

import (
	"context"
	"sync"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/event"
)

func TestRuntimeIdentityParsesNativeThreadMetadata(t *testing.T) {
	a := New("codex")
	state := &sessionState{
		threadID: "thread-native",
		runtimeIdentity: adapter.RuntimeIdentity{
			RequestedModel: "requested-model",
			ProviderSource: adapter.EvidenceUnavailable,
			ModelSource:    adapter.EvidenceUnavailable,
		},
	}
	a.mu.Lock()
	a.sessions[state.threadID] = state
	a.mu.Unlock()

	a.captureRuntimeIdentity(state, []byte(`{"thread":{"modelProvider":"openai","model":"codex-native-model"}}`))
	identity, err := a.RuntimeIdentity(context.Background(), state.threadID)
	if err != nil {
		t.Fatal(err)
	}
	if identity.RequestedModel != "requested-model" || identity.ObservedProvider != "openai" || identity.ObservedModel != "codex-native-model" {
		t.Fatalf("unexpected identity: %+v", identity)
	}
	if identity.ProviderSource != adapter.EvidenceNativeProtocol || identity.ModelSource != adapter.EvidenceNativeProtocol {
		t.Fatalf("native evidence sources missing: %+v", identity)
	}
}

func TestRuntimeIdentityMalformedMetadataRemainsUnavailable(t *testing.T) {
	state := &sessionState{runtimeIdentity: adapter.RuntimeIdentity{RequestedModel: "requested-model", ProviderSource: adapter.EvidenceUnavailable, ModelSource: adapter.EvidenceUnavailable}}
	a := New("codex")
	a.captureRuntimeIdentity(state, []byte(`{"result":{"model":`))
	if state.runtimeIdentity.ObservedModel != "" || state.runtimeIdentity.ObservedProvider != "" {
		t.Fatalf("malformed metadata invented identity: %+v", state.runtimeIdentity)
	}
}

func TestRuntimeIdentitySurvivesTerminalResultUntilQueried(t *testing.T) {
	a := New("codex")
	state := &sessionState{
		threadID:        "thread-terminal",
		resultReady:     make(chan struct{}),
		runtimeIdentity: adapter.RuntimeIdentity{RequestedModel: "requested-model", ProviderSource: adapter.EvidenceUnavailable, ModelSource: adapter.EvidenceUnavailable},
	}
	a.mu.Lock()
	a.sessions[state.threadID] = state
	a.mu.Unlock()
	a.captureRuntimeIdentity(state, []byte(`{"turn":{"provider":"openai","model":"codex-terminal-model"}}`))
	a.recordTerminalEvent(state, adapter.NativeEvent{Kind: event.ResultSubmitted})
	identity, err := a.RuntimeIdentity(context.Background(), state.threadID)
	if err != nil || identity.ObservedModel != "codex-terminal-model" {
		t.Fatalf("terminal result lost native identity: %+v, %v", identity, err)
	}
}

func TestRuntimeIdentityConcurrentEventAndQueryIsRaceSafe(t *testing.T) {
	a := New("codex")
	state := &sessionState{threadID: "thread-race", runtimeIdentity: adapter.RuntimeIdentity{RequestedModel: "requested-model"}}
	a.mu.Lock()
	a.sessions[state.threadID] = state
	a.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			a.captureRuntimeIdentity(state, []byte(`{"provider":"openai","model":"codex-native-model"}`))
		}()
		go func() {
			defer wg.Done()
			_, _ = a.RuntimeIdentity(context.Background(), state.threadID)
		}()
	}
	wg.Wait()
	identity, err := a.RuntimeIdentity(context.Background(), state.threadID)
	if err != nil || identity.ObservedProvider != "openai" || identity.ObservedModel != "codex-native-model" {
		t.Fatalf("identity did not survive concurrent access: %+v, %v", identity, err)
	}
}
