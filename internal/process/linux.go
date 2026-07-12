//go:build linux

package process

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// ConfigureCommand puts a worker in its own process group so cancellation can
// reach descendants instead of only the top-level CLI process.
func ConfigureCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func ConfigureDetachedCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}

func Inspect(_ context.Context, pid int) (Identity, error) {
	if pid <= 0 {
		return Identity{}, fmt.Errorf("invalid pid %d", pid)
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return Identity{}, err
	}
	closeParen := strings.LastIndex(string(data), ")")
	if closeParen < 0 || closeParen+2 > len(data) {
		return Identity{}, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	fields := strings.Fields(string(data[closeParen+2:]))
	// The suffix starts at field 3. pgrp is field 5 and starttime is field 22.
	if len(fields) <= 19 {
		return Identity{}, fmt.Errorf("incomplete /proc/%d/stat", pid)
	}
	if _, err := strconv.Atoi(fields[2]); err != nil {
		return Identity{}, fmt.Errorf("invalid process group for pid %d: %w", pid, err)
	}
	return Identity{PID: pid, StartToken: fields[19], ProcessGroupToken: fields[2]}, nil
}

func Signal(ctx context.Context, identity Identity, signal syscall.Signal) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if identity.PID <= 0 || identity.StartToken == "" {
		return fmt.Errorf("incomplete process identity")
	}
	current, err := Inspect(ctx, identity.PID)
	if err != nil {
		return err
	}
	if !identity.SameProcess(current) {
		return fmt.Errorf("process identity mismatch for pid %d", identity.PID)
	}
	pgid, err := strconv.Atoi(current.ProcessGroupToken)
	if err != nil || pgid <= 0 {
		return fmt.Errorf("invalid process group identity for pid %d", identity.PID)
	}
	if err := syscall.Kill(-pgid, signal); err != nil {
		return err
	}
	return nil
}

func Interrupt(ctx context.Context, identity Identity) error {
	return Signal(ctx, identity, syscall.SIGINT)
}

func TerminateGracefully(ctx context.Context, identity Identity) error {
	return Signal(ctx, identity, syscall.SIGTERM)
}

func KillTree(ctx context.Context, identity Identity) error {
	return Signal(ctx, identity, syscall.SIGKILL)
}
