package backend

import "testing"

// TestAvailableFollowsPrecedence asserts Available() returns the installed backends
// as an in-order subsequence of Precedence — never reordered, never including an
// uninstalled one. The default backend pick depends on this ordering invariant, and
// it holds regardless of which runtimes happen to be installed on the test host.
func TestAvailableFollowsPrecedence(t *testing.T) {
	avail := Available()
	pi := 0
	for _, b := range avail {
		for pi < len(Precedence) && Precedence[pi] != b.Name() {
			pi++
		}
		if pi == len(Precedence) {
			t.Fatalf("Available() = %v is not an in-order subsequence of Precedence %v", names(avail), Precedence)
		}
		if !b.Available() {
			t.Errorf("Available() returned %q which reports Available() = false", b.Name())
		}
		pi++
	}
}

// TestSelectReturnsFirstAvailable asserts Select() is exactly the first entry of
// Available(), and (nil, false) when no runtime is installed.
func TestSelectReturnsFirstAvailable(t *testing.T) {
	b, ok := Select()
	avail := Available()
	switch {
	case len(avail) == 0:
		if ok {
			t.Fatal("Select() ok = true with no available backends")
		}
	case !ok:
		t.Fatalf("Select() ok = false but %d backends are available", len(avail))
	case b.Name() != avail[0].Name():
		t.Fatalf("Select() = %q, want the first available %q", b.Name(), avail[0].Name())
	}
}

// TestValidateBackend asserts the shared validation predicate: a name passes only
// when it is in Precedence, registered, and its runtime is installed. It mirrors
// Available() so the set of names that validate is exactly the set Available()
// returns, regardless of which runtimes are installed on the test host.
func TestValidateBackend(t *testing.T) {
	available := map[string]bool{}
	for _, b := range Available() {
		available[b.Name()] = true
	}
	for _, name := range Precedence {
		t.Run(name, func(t *testing.T) {
			err := ValidateBackend(name)
			if available[name] && err != nil {
				t.Fatalf("ValidateBackend(%q) = %v, want nil for an available backend", name, err)
			}
			if !available[name] && err == nil {
				t.Fatalf("ValidateBackend(%q) = nil, want error for an unavailable backend", name)
			}
		})
	}
	if err := ValidateBackend("not-a-backend"); err == nil {
		t.Fatal("ValidateBackend(\"not-a-backend\") = nil, want error")
	}
}

func names(bs []Backend) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = b.Name()
	}
	return out
}
