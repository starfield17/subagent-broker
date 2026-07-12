package scope

import "testing"

func TestDoubleStarMatch(t *testing.T) {
	for _, candidate := range []string{"internal/auth/token.go", "internal/auth/nested/key.go"} {
		ok, err := Match("internal/auth/**", candidate)
		if err != nil || !ok {
			t.Fatalf("expected %s to match: ok=%v err=%v", candidate, ok, err)
		}
	}
	ok, _ := Match("internal/auth/**", "internal/api/handler.go")
	if ok {
		t.Fatal("unrelated path matched")
	}
}

func TestConservativeOverlap(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"internal/auth/**", "internal/cache/**", false},
		{"internal/auth/**", "internal/auth/token.go", true},
		{"internal/*/config.go", "internal/auth/**", true},
		{"go.mod", "go.sum", false},
	}
	for _, tc := range cases {
		got, err := MayOverlap(tc.a, tc.b)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Fatalf("MayOverlap(%q,%q)=%v want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestRejectEscapingScope(t *testing.T) {
	if _, err := Compile("../secrets/**"); err == nil {
		t.Fatal("scope traversal must be rejected")
	}
}
