package process

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
)

func TestIsProcessNotFoundTypedOnly(t *testing.T) {
	if !IsProcessNotFound(ErrProcessNotFound) {
		t.Fatal("sentinel")
	}
	if !IsProcessNotFound(fmt.Errorf("%w: pid 9", ErrProcessNotFound)) {
		t.Fatal("wrapped sentinel")
	}
	if !IsProcessNotFound(os.ErrNotExist) {
		t.Fatal("os.ErrNotExist")
	}
	if !IsProcessNotFound(syscall.ESRCH) {
		t.Fatal("ESRCH")
	}
	if IsProcessNotFound(nil) {
		t.Fatal("nil")
	}
	if IsProcessNotFound(errors.New("permission denied")) {
		t.Fatal("permission must be unknown")
	}
	if IsProcessNotFound(errors.New("no such process")) {
		t.Fatal("free-form string must not prove not-found")
	}
	if IsProcessNotFound(errors.New("open /etc/foo: no such file or directory")) {
		t.Fatal("unrelated no-such-file must not prove not-found")
	}
}
