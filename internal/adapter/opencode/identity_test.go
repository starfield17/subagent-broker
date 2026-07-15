package opencode

import (
	"context"
	"sync"
	"testing"

	"github.com/vnai/subagent-broker/internal/adapter"
)

func TestRuntimeIdentityPreservesOpenCodeProviderIDs(t *testing.T) {
	a := New("opencode")
	state := &sessionState{
		sessionID: "opencode-native",
		runtimeIdentity: adapter.RuntimeIdentity{
			RequestedModel: "openai/gpt-requested",
			ProviderSource: adapter.EvidenceUnavailable,
			ModelSource:    adapter.EvidenceUnavailable,
		},
	}
	a.mu.Lock()
	a.sessions[state.sessionID] = state
	a.mu.Unlock()

	a.captureRuntimeIdentity(state, "openai", "gpt-native")
	identity, err := a.RuntimeIdentity(context.Background(), state.sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if identity.RequestedModel != "openai/gpt-requested" || identity.ObservedProvider != "openai" || identity.ObservedModel != "gpt-native" {
		t.Fatalf("unexpected identity: %+v", identity)
	}
	a.captureRuntimeIdentity(state, "anthropic", "claude-native")
	identity, err = a.RuntimeIdentity(context.Background(), state.sessionID)
	if err != nil || identity.ObservedProvider != "anthropic" || identity.ObservedModel != "claude-native" {
		t.Fatalf("OpenCode identity was hard-coded or not refreshed: %+v, %v", identity, err)
	}
}

func TestRuntimeIdentityMissingMetadataRemainsUnavailable(t *testing.T) {
	a := New("opencode")
	state := &sessionState{
		sessionID: "opencode-missing",
		runtimeIdentity: adapter.RuntimeIdentity{
			RequestedModel: "provider/requested",
			ProviderSource: adapter.EvidenceUnavailable,
			ModelSource:    adapter.EvidenceUnavailable,
		},
	}
	a.mu.Lock()
	a.sessions[state.sessionID] = state
	a.mu.Unlock()
	a.captureRuntimeIdentity(state, "", "")
	identity, err := a.RuntimeIdentity(context.Background(), state.sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if identity.ObservedProvider != "" || identity.ObservedModel != "" || identity.ProviderSource != adapter.EvidenceUnavailable || identity.ModelSource != adapter.EvidenceUnavailable {
		t.Fatalf("missing metadata invented identity: %+v", identity)
	}
}

func TestRuntimeIdentitySurvivesTerminalCollectionUntilQueried(t *testing.T) {
	a := New("opencode")
	state := &sessionState{
		sessionID: "opencode-terminal",
		runtimeIdentity: adapter.RuntimeIdentity{
			RequestedModel: "openai/requested",
			ProviderSource: adapter.EvidenceUnavailable,
			ModelSource:    adapter.EvidenceUnavailable,
		},
	}
	a.mu.Lock()
	a.sessions[state.sessionID] = state
	a.mu.Unlock()
	a.captureRuntimeIdentity(state, "openai", "gpt-terminal")
	state.mu.Lock()
	state.resultSignaled = true
	state.mu.Unlock()
	identity, err := a.RuntimeIdentity(context.Background(), state.sessionID)
	if err != nil || identity.ObservedModel != "gpt-terminal" {
		t.Fatalf("terminal collection lost native identity: %+v, %v", identity, err)
	}
}

func TestRuntimeIdentityConcurrentMessageProcessingAndQueryIsRaceSafe(t *testing.T) {
	a := New("opencode")
	state := &sessionState{sessionID: "opencode-race", runtimeIdentity: adapter.RuntimeIdentity{RequestedModel: "provider/requested"}}
	a.mu.Lock()
	a.sessions[state.sessionID] = state
	a.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			a.captureRuntimeIdentity(state, "openai", "gpt-native")
		}()
		go func() {
			defer wg.Done()
			_, _ = a.RuntimeIdentity(context.Background(), state.sessionID)
		}()
	}
	wg.Wait()
	identity, err := a.RuntimeIdentity(context.Background(), state.sessionID)
	if err != nil || identity.ObservedProvider != "openai" || identity.ObservedModel != "gpt-native" {
		t.Fatalf("identity did not survive concurrent access: %+v, %v", identity, err)
	}
}
