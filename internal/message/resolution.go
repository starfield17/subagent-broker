package message

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// NewAnswerResolution constructs the only valid resolution shape for a Question.
func NewAnswerResolution(text string) Resolution {
	return Resolution{
		Kind:   ResolutionKindAnswer,
		Answer: &AnswerPayload{Text: text},
	}
}

// NewDecisionResolution constructs the only valid resolution shape for a
// PermissionRequest or ScopeExpansionRequest.
func NewDecisionResolution(allowed bool, reason string, allowPublicInterfaceChange bool) Resolution {
	return Resolution{
		Kind: ResolutionKindDecision,
		Decision: &DecisionPayload{
			Allowed:                    allowed,
			Reason:                     reason,
			AllowPublicInterfaceChange: allowPublicInterfaceChange,
		},
	}
}

// ValidateResolutionForType validates the tagged resolution union against the
// message type that owns it. A nil Decision is distinct from an explicit denial.
func ValidateResolutionForType(messageType Type, resolution Resolution) error {
	if resolution.Answer != nil && resolution.Decision != nil {
		return fmt.Errorf("resolution cannot contain both answer and decision")
	}

	switch messageType {
	case Question:
		if resolution.Kind != ResolutionKindAnswer {
			return fmt.Errorf("question requires an answer resolution")
		}
		if resolution.Answer == nil {
			return fmt.Errorf("question requires answer")
		}
		if strings.TrimSpace(resolution.Answer.Text) == "" {
			return fmt.Errorf("question answer is required")
		}
		if resolution.Decision != nil {
			return fmt.Errorf("question cannot contain a decision")
		}
		return nil

	case PermissionRequest:
		if err := validateDecisionResolution(messageType, resolution); err != nil {
			return err
		}
		if resolution.Decision.AllowPublicInterfaceChange {
			return fmt.Errorf("permission decision cannot allow public interface changes")
		}
		return nil

	case ScopeExpansionRequest:
		if err := validateDecisionResolution(messageType, resolution); err != nil {
			return err
		}
		if !resolution.Decision.Allowed && resolution.Decision.AllowPublicInterfaceChange {
			return fmt.Errorf("scope denial cannot allow public interface changes")
		}
		return nil

	default:
		return fmt.Errorf("message type %s is not resolvable", messageType)
	}
}

func validateDecisionResolution(messageType Type, resolution Resolution) error {
	if resolution.Kind != ResolutionKindDecision {
		return fmt.Errorf("%s requires a decision resolution", messageType)
	}
	if resolution.Decision == nil {
		return fmt.Errorf("%s requires an explicit decision", messageType)
	}
	if resolution.Answer != nil {
		return fmt.Errorf("%s cannot contain an answer", messageType)
	}
	if !resolution.Decision.Allowed && strings.TrimSpace(resolution.Decision.Reason) == "" {
		return fmt.Errorf("denial reason is required")
	}
	return nil
}

// DecodeResolutionForType decodes both the current tagged union and the
// historical untagged journal representation. It never mutates the journal.
func DecodeResolutionForType(messageType Type, raw json.RawMessage) (Resolution, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return Resolution{}, fmt.Errorf("resolution is required")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Resolution{}, fmt.Errorf("decode resolution: %w", err)
	}
	if fields == nil {
		return Resolution{}, fmt.Errorf("resolution must be an object")
	}

	if _, hasKind := fields["kind"]; hasKind {
		var resolution Resolution
		if err := json.Unmarshal(raw, &resolution); err != nil {
			return Resolution{}, fmt.Errorf("decode typed resolution: %w", err)
		}
		if err := ValidateResolutionForType(messageType, resolution); err != nil {
			return Resolution{}, err
		}
		return resolution, nil
	}

	return decodeLegacyResolutionForType(messageType, fields)
}

func decodeLegacyResolutionForType(messageType Type, fields map[string]json.RawMessage) (Resolution, error) {
	answer, err := legacyAnswer(fields)
	if err != nil {
		return Resolution{}, err
	}
	decision, decisionPresent, decisionMeaningful, err := legacyDecision(fields)
	if err != nil {
		return Resolution{}, err
	}

	switch messageType {
	case Question:
		if strings.TrimSpace(answer) != "" {
			if decisionMeaningful {
				return Resolution{}, fmt.Errorf("legacy question resolution is ambiguous: answer and decision are both populated")
			}
			resolution := NewAnswerResolution(answer)
			return resolution, ValidateResolutionForType(messageType, resolution)
		}
		return Resolution{}, fmt.Errorf("question requires a non-empty answer")

	case PermissionRequest, ScopeExpansionRequest:
		if strings.TrimSpace(answer) != "" {
			if !decisionMeaningful {
				return Resolution{}, fmt.Errorf("legacy %s answer with zero-value decision requires reconciliation", messageType)
			}
			return Resolution{}, fmt.Errorf("legacy %s resolution is ambiguous: answer and decision are both populated", messageType)
		}
		if !decisionPresent {
			return Resolution{}, fmt.Errorf("%s requires an explicit decision", messageType)
		}
		resolution := NewDecisionResolution(decision.Allowed, decision.Reason, decision.AllowPublicInterfaceChange)
		return resolution, ValidateResolutionForType(messageType, resolution)

	default:
		return Resolution{}, fmt.Errorf("message type %s is not resolvable", messageType)
	}
}

func legacyAnswer(fields map[string]json.RawMessage) (string, error) {
	raw, ok := fields["answer"]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", nil
	}
	var answer string
	if err := json.Unmarshal(raw, &answer); err != nil {
		return "", fmt.Errorf("legacy answer must be a string: %w", err)
	}
	return answer, nil
}

func legacyDecision(fields map[string]json.RawMessage) (DecisionPayload, bool, bool, error) {
	raw, ok := fields["decision"]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return DecisionPayload{}, false, false, nil
	}
	var decision DecisionPayload
	if err := json.Unmarshal(raw, &decision); err != nil {
		return DecisionPayload{}, true, false, fmt.Errorf("legacy decision must be an object: %w", err)
	}
	meaningful := decision.Allowed || strings.TrimSpace(decision.Reason) != "" || decision.AllowPublicInterfaceChange
	return decision, true, meaningful, nil
}

// CanonicalResolutionJSON validates and marshals a resolution using the new
// tagged encoding. Newly persisted journal records must use this output.
func CanonicalResolutionJSON(messageType Type, resolution Resolution) (json.RawMessage, error) {
	if err := ValidateResolutionForType(messageType, resolution); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(resolution)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func normalizeResolutionJSON(messageType Type, raw json.RawMessage) (json.RawMessage, error) {
	resolution, err := DecodeResolutionForType(messageType, raw)
	if err != nil {
		return nil, err
	}
	return CanonicalResolutionJSON(messageType, resolution)
}

// ResolutionsEqualForType compares typed and legacy encodings by semantic
// meaning, so a legacy journal answer and a new typed answer are idempotent.
func ResolutionsEqualForType(messageType Type, left, right json.RawMessage) bool {
	leftResolution, err := DecodeResolutionForType(messageType, left)
	if err != nil {
		return false
	}
	rightResolution, err := DecodeResolutionForType(messageType, right)
	if err != nil {
		return false
	}
	leftCanonical, err := CanonicalResolutionJSON(messageType, leftResolution)
	if err != nil {
		return false
	}
	rightCanonical, err := CanonicalResolutionJSON(messageType, rightResolution)
	if err != nil {
		return false
	}
	return bytes.Equal(leftCanonical, rightCanonical)
}
