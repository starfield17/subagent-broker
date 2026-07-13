package message

import "testing"

func TestSelectPermissionOptionIDAllowOncePreferred(t *testing.T) {
	options := []PermissionOption{
		{OptionID: "always-a", Kind: "allow_always", Name: "Always"},
		{OptionID: "once-a", Kind: "allow_once", Name: "Once"},
		{OptionID: "rej", Kind: "reject_once"},
	}
	id, err := SelectPermissionOptionID(options, true)
	if err != nil || id != "once-a" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}

func TestSelectPermissionOptionIDAllowAlwaysFallback(t *testing.T) {
	options := []PermissionOption{
		{OptionID: "always-a", Kind: "allow_always"},
		{OptionID: "rej", Kind: "reject_once"},
	}
	id, err := SelectPermissionOptionID(options, true)
	if err != nil || id != "always-a" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}

func TestSelectPermissionOptionIDDenyOncePreferred(t *testing.T) {
	options := []PermissionOption{
		{OptionID: "rej-always", Kind: "reject_always"},
		{OptionID: "rej-once", Kind: "reject_once"},
		{OptionID: "allow", Kind: "allow_once"},
	}
	id, err := SelectPermissionOptionID(options, false)
	if err != nil || id != "rej-once" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}

func TestSelectPermissionOptionIDDenyAlwaysFallback(t *testing.T) {
	options := []PermissionOption{
		{OptionID: "rej-always", Kind: "reject_always"},
		{OptionID: "allow", Kind: "allow_once"},
	}
	id, err := SelectPermissionOptionID(options, false)
	if err != nil || id != "rej-always" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}

func TestSelectPermissionOptionIDMissingCompatible(t *testing.T) {
	options := []PermissionOption{
		{OptionID: "only-reject", Kind: "reject_once"},
	}
	if _, err := SelectPermissionOptionID(options, true); err == nil {
		t.Fatal("expected error when no allow option")
	}
	options = []PermissionOption{
		{OptionID: "only-allow", Kind: "allow_once"},
	}
	if _, err := SelectPermissionOptionID(options, false); err == nil {
		t.Fatal("expected error when no reject option")
	}
}

func TestSelectPermissionOptionIDIgnoresNameWhenKindPresent(t *testing.T) {
	// Kind must win; display name "Allow once" on a reject option must not be selected.
	options := []PermissionOption{
		{OptionID: "wrong", Kind: "reject_once", Name: "Allow once"},
		{OptionID: "right", Kind: "allow_once", Name: "Something else"},
	}
	id, err := SelectPermissionOptionID(options, true)
	if err != nil || id != "right" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}
