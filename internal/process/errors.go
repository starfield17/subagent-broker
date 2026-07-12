package process

import (
	"errors"
	"os"
	"syscall"
)

// ErrProcessNotFound is the typed sentinel for "this PID no longer refers to a process".
// Platform Inspect/Kill paths should wrap platform-specific missing-process errors with it.
// Callers (Recovery, Controller) must use IsProcessNotFound — never parse error strings.
var ErrProcessNotFound = errors.New("process not found")

// IsProcessNotFound reports whether err proves the OS process does not exist.
// Only typed sentinels count: ErrProcessNotFound, os.ErrNotExist, and syscall.ESRCH.
// Free-form strings such as "no such file" are intentionally NOT treated as proof.
// Permission, IO, and other failures return false (inspect_unknown territory).
func IsProcessNotFound(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrProcessNotFound) ||
		errors.Is(err, os.ErrNotExist) ||
		errors.Is(err, syscall.ESRCH)
}
