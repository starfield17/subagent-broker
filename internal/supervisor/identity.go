package supervisor

import (
	"context"
	"fmt"
	"strings"

	"github.com/vnai/subagent-broker/internal/adapter"
)

// refreshWorkerRuntimeIdentity records optional adapter evidence at lifecycle
// boundaries. Identity is intentionally best-effort: an unavailable native
// fact is a warning/evidence gap, never a reason to rewrite the requested
// model or to claim verification.
func (s *Service) refreshWorkerRuntimeIdentity(runtime *TaskState, harness adapter.Adapter, nativeSessionID, phase string) {
	if runtime == nil || runtime.Worker == nil {
		return
	}
	identity := runtime.Worker.RuntimeIdentity
	if identity.ProviderSource == "" {
		identity.ProviderSource = adapter.EvidenceUnavailable
	}
	if identity.ModelSource == "" {
		identity.ModelSource = adapter.EvidenceUnavailable
	}
	var warning string
	provider, supported := harness.(adapter.RuntimeIdentityProvider)
	if !supported {
		warning = fmt.Sprintf("runtime identity unavailable at %s: adapter does not expose native identity", phase)
	} else {
		observed, err := provider.RuntimeIdentity(context.Background(), nativeSessionID)
		if err != nil {
			warning = fmt.Sprintf("runtime identity unavailable at %s: %s", phase, sanitizeIdentityError(err))
		} else {
			// RequestedModel is configuration evidence and remains separate from
			// observations returned by the adapter.
			observed.RequestedModel = identity.RequestedModel
			if observed.ProviderSource == "" {
				observed.ProviderSource = adapter.EvidenceUnavailable
			}
			if observed.ModelSource == "" {
				observed.ModelSource = adapter.EvidenceUnavailable
			}
			if observed.ObservedProvider == "" {
				observed.ProviderSource = adapter.EvidenceUnavailable
			}
			if observed.ObservedModel == "" {
				observed.ModelSource = adapter.EvidenceUnavailable
			}
			identity = observed
		}
	}
	if warning != "" {
		identity.ProviderSource = sourceOrUnavailable(identity.ProviderSource, identity.ObservedProvider)
		identity.ModelSource = sourceOrUnavailable(identity.ModelSource, identity.ObservedModel)
		if !containsString(runtime.Worker.IdentityWarnings, warning) {
			runtime.Worker.IdentityWarnings = append(runtime.Worker.IdentityWarnings, warning)
		}
	}
	runtime.Worker.RuntimeIdentity = identity
	updateAttemptWorker(runtime, *runtime.Worker)
	// A persistence error is handled by the normal Supervisor commit path. It
	// must not turn an identity observation gap into a fabricated verified fact.
	_ = s.saveRuntime(*runtime)
}

func sourceOrUnavailable(source adapter.EvidenceSource, observed string) adapter.EvidenceSource {
	if strings.TrimSpace(observed) == "" {
		return adapter.EvidenceUnavailable
	}
	if source == "" || source == adapter.EvidenceRequestedConfig {
		return adapter.EvidenceNativeProtocol
	}
	return source
}

func sanitizeIdentityError(err error) string {
	if err == nil {
		return "unknown error"
	}
	message := strings.TrimSpace(err.Error())
	// Keep warnings useful without allowing common credential-shaped values to
	// become durable identity evidence.
	for _, marker := range []string{"Bearer ", "token=", "api_key=", "apikey="} {
		if index := strings.Index(strings.ToLower(message), strings.ToLower(marker)); index >= 0 {
			return strings.TrimSpace(message[:index]) + "[redacted]"
		}
	}
	return message
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
