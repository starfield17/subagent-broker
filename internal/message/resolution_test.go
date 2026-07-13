package message

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateResolutionForType(t *testing.T) {
	answer := NewAnswerResolution("yes")
	decision := NewDecisionResolution(true, "", false)
	denial := NewDecisionResolution(false, "not safe", false)
	both := Resolution{
		Kind:     ResolutionKindAnswer,
		Answer:   &AnswerPayload{Text: "yes"},
		Decision: &DecisionPayload{Allowed: true},
	}
	cases := []struct {
		name     string
		message  Type
		value    Resolution
		wantErr  bool
		contains string
	}{
		{name: "question answer", message: Question, value: answer},
		{name: "question decision", message: Question, value: decision, wantErr: true},
		{name: "permission approval", message: PermissionRequest, value: decision},
		{name: "permission denial", message: PermissionRequest, value: denial},
		{name: "permission answer", message: PermissionRequest, value: answer, wantErr: true},
		{name: "scope approval", message: ScopeExpansionRequest, value: decision},
		{name: "scope denial", message: ScopeExpansionRequest, value: denial},
		{name: "scope answer", message: ScopeExpansionRequest, value: answer, wantErr: true},
		{name: "missing decision", message: PermissionRequest, value: Resolution{Kind: ResolutionKindDecision}, wantErr: true},
		{name: "both present", message: PermissionRequest, value: both, wantErr: true},
		{name: "unsupported type", message: Instruction, value: answer, wantErr: true},
		{name: "missing denial reason", message: PermissionRequest, value: NewDecisionResolution(false, "", false), wantErr: true},
		{name: "permission public change", message: PermissionRequest, value: NewDecisionResolution(true, "ok", true), wantErr: true},
		{name: "scope denial public change", message: ScopeExpansionRequest, value: NewDecisionResolution(false, "no", true), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateResolutionForType(tc.message, tc.value)
			if tc.wantErr && err == nil {
				t.Fatal("expected validation error")
			}
			if !tc.wantErr && err != nil {
				t.Fatal(err)
			}
			if tc.contains != "" && (err == nil || !strings.Contains(err.Error(), tc.contains)) {
				t.Fatalf("error=%v, want substring %q", err, tc.contains)
			}
		})
	}
}

func TestDecodeResolutionForTypeLegacyAndTyped(t *testing.T) {
	tests := []struct {
		name       string
		message    Type
		raw        string
		wantKind   ResolutionKind
		wantAnswer string
		wantAllow  *bool
		wantErr    bool
		wantText   string
	}{
		{
			name:       "legacy question zero decision",
			message:    Question,
			raw:        `{"answer":"text","decision":{"allowed":false}}`,
			wantKind:   ResolutionKindAnswer,
			wantAnswer: "text",
		},
		{
			name:       "typed question",
			message:    Question,
			raw:        `{"kind":"answer","answer":{"text":"text"}}`,
			wantKind:   ResolutionKindAnswer,
			wantAnswer: "text",
		},
		{
			name:      "legacy permission approval",
			message:   PermissionRequest,
			raw:       `{"answer":"","decision":{"allowed":true}}`,
			wantKind:  ResolutionKindDecision,
			wantAllow: boolPtr(true),
		},
		{
			name:      "legacy permission denial",
			message:   PermissionRequest,
			raw:       `{"decision":{"allowed":false,"reason":"unsafe"}}`,
			wantKind:  ResolutionKindDecision,
			wantAllow: boolPtr(false),
		},
		{
			name:     "malformed permission answer",
			message:  PermissionRequest,
			raw:      `{"answer":"deny","decision":{"allowed":false}}`,
			wantErr:  true,
			wantText: "reconciliation",
		},
		{
			name:    "ambiguous legacy decision",
			message: PermissionRequest,
			raw:     `{"answer":"x","decision":{"allowed":true}}`,
			wantErr: true,
		},
		{
			name:    "missing decision",
			message: PermissionRequest,
			raw:     `{}`,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeResolutionForType(tc.message, json.RawMessage(tc.raw))
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected decode error")
				}
				if tc.wantText != "" && !strings.Contains(err.Error(), tc.wantText) {
					t.Fatalf("error=%v, want %q", err, tc.wantText)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Kind != tc.wantKind || got.Answer == nil && tc.wantAnswer != "" {
				t.Fatalf("resolution=%+v", got)
			}
			if tc.wantAnswer != "" && got.Answer.Text != tc.wantAnswer {
				t.Fatalf("answer=%+v", got.Answer)
			}
			if tc.wantAllow != nil {
				if got.Decision == nil || got.Decision.Allowed != *tc.wantAllow {
					t.Fatalf("decision=%+v", got.Decision)
				}
			}
		})
	}
}

func TestResolutionCanonicalJSONAndSemanticEquality(t *testing.T) {
	answer := NewAnswerResolution("text")
	canonical, err := CanonicalResolutionJSON(Question, answer)
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != `{"kind":"answer","answer":{"text":"text"}}` {
		t.Fatalf("canonical answer=%s", canonical)
	}
	if !ResolutionsEqualForType(Question, json.RawMessage(`{"answer":"text","decision":{"allowed":false}}`), canonical) {
		t.Fatal("legacy and typed question answers must compare equal")
	}
	if ResolutionsEqualForType(Question, canonical, json.RawMessage(`{"kind":"answer","answer":{"text":"different"}}`)) {
		t.Fatal("different answers must conflict")
	}

	decision := NewDecisionResolution(true, "ok", false)
	decisionJSON, err := CanonicalResolutionJSON(PermissionRequest, decision)
	if err != nil {
		t.Fatal(err)
	}
	if string(decisionJSON) != `{"kind":"decision","decision":{"allowed":true,"reason":"ok"}}` {
		t.Fatalf("canonical decision=%s", decisionJSON)
	}
}

func TestRouterCanonicalizesLegacyAndKeepsLegacyJournalReadable(t *testing.T) {
	router, err := NewRouter(NewRouterOptions{RunID: "run-1", Store: NewStore(filepath.Join(t.TempDir(), "messages.jsonl"))})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := router.EnqueueDecision("task-a", "worker-a", Question, Decision, json.RawMessage(`{"schema_version":"v1alpha1","question":"Q","reason":"R","current_scope":["x"],"workspace_state":"w"}`))
	if err != nil {
		t.Fatal(err)
	}
	legacy := json.RawMessage(`{"answer":"legacy text","decision":{"allowed":false}}`)
	frozen, result, err := router.FreezeResolution(queued.MessageID, legacy)
	if err != nil || result != ResolutionFrozen {
		t.Fatalf("freeze legacy resolution: result=%d err=%v", result, err)
	}
	if !strings.Contains(string(frozen.Resolution), `"kind":"answer"`) || strings.Contains(string(frozen.Resolution), `"decision"`) {
		t.Fatalf("frozen resolution is not canonical typed answer: %s", frozen.Resolution)
	}

	answered, err := router.Transition(queued.MessageID, Answered, "", frozen.Resolution, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := router.GetAnsweredResolution(answered.MessageID)
	if err != nil || !ok || got.Answer == nil || got.Answer.Text != "legacy text" {
		t.Fatalf("answered resolution=%+v ok=%v err=%v", got, ok, err)
	}

	legacyAnswered := answered
	legacyAnswered.Resolution = legacy
	legacyRouter, err := NewRouter(NewRouterOptions{
		RunID: "run-1", Store: NewStore(filepath.Join(t.TempDir(), "messages.jsonl")),
		Initial: map[string]Message{legacyAnswered.MessageID: legacyAnswered},
	})
	if err != nil {
		t.Fatal(err)
	}
	typed := NewAnswerResolution("legacy text")
	typedJSON, _ := json.Marshal(typed)
	_, result, err = legacyRouter.FreezeResolution(legacyAnswered.MessageID, typedJSON)
	if err != nil || result != ResolutionAlreadyIdentical {
		t.Fatalf("legacy/typed retry should be idempotent: result=%d err=%v", result, err)
	}
}

func boolPtr(value bool) *bool { return &value }
