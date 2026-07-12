//go:build !linux

package process

import (
	"context"
	"fmt"
	"os/exec"
)

func ConfigureCommand(_ *exec.Cmd) {}

func ConfigureDetachedCommand(_ *exec.Cmd) {}

func Inspect(_ context.Context, _ int) (Identity, error) {
	return Identity{}, fmt.Errorf("process identity inspection is not implemented on this platform")
}

func Interrupt(_ context.Context, _ Identity) error {
	return fmt.Errorf("process interruption is not implemented on this platform")
}

func TerminateGracefully(_ context.Context, _ Identity) error {
	return fmt.Errorf("process termination is not implemented on this platform")
}

func KillTree(_ context.Context, _ Identity) error {
	return fmt.Errorf("process-tree termination is not implemented on this platform")
}
