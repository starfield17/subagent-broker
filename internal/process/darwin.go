//go:build darwin

package process

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// Darwin exposes the process start time, PID, and process group through ps.
// The start time is an identity token; the PGID is used only after the start
// token has been re-checked for the root process.
func ConfigureCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func ConfigureDetachedCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}

func Inspect(ctx context.Context, pid int) (Identity, error) {
	if pid <= 0 {
		return Identity{}, fmt.Errorf("invalid pid %d", pid)
	}
	output, err := exec.CommandContext(ctx, "ps", "-p", strconv.Itoa(pid), "-o", "pid=", "-o", "pgid=", "-o", "lstart=").Output()
	if err != nil {
		return Identity{}, fmt.Errorf("%w: pid %d: %v", ErrProcessNotFound, pid, err)
	}
	identity, err := parsePSIdentity(string(output), pid)
	if err != nil {
		return Identity{}, err
	}
	return identity, nil
}

func parsePSIdentity(line string, expectedPID int) (Identity, error) {
	fields := strings.Fields(line)
	if len(fields) < 7 {
		return Identity{}, fmt.Errorf("malformed ps identity for pid %d", expectedPID)
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil || pid != expectedPID {
		return Identity{}, fmt.Errorf("invalid ps pid for %d", expectedPID)
	}
	pgid, err := strconv.Atoi(fields[1])
	if err != nil || pgid <= 0 {
		return Identity{}, fmt.Errorf("invalid process group for pid %d", expectedPID)
	}
	return Identity{PID: pid, StartToken: strings.Join(fields[2:], " "), ProcessGroupToken: strconv.Itoa(pgid)}, nil
}

func signal(ctx context.Context, identity Identity, signal syscall.Signal) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !identity.Complete() {
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
	return syscall.Kill(-pgid, signal)
}

func Interrupt(ctx context.Context, identity Identity) error {
	return signal(ctx, identity, syscall.SIGINT)
}

func TerminateGracefully(ctx context.Context, identity Identity) error {
	return signal(ctx, identity, syscall.SIGTERM)
}

func KillTree(ctx context.Context, identity Identity) error {
	return signal(ctx, identity, syscall.SIGKILL)
}

func GroupMembers(ctx context.Context, identity Identity) ([]Identity, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if identity.ProcessGroupToken == "" {
		return nil, fmt.Errorf("incomplete process group identity")
	}
	output, err := exec.CommandContext(ctx, "ps", "-axo", "pid=", "-o", "pgid=", "-o", "lstart=").Output()
	if err != nil {
		return nil, err
	}
	var members []Identity
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 7 || strings.TrimSpace(fields[1]) != identity.ProcessGroupToken {
			continue
		}
		pid, pidErr := strconv.Atoi(fields[0])
		if pidErr != nil || pid <= 0 {
			continue
		}
		members = append(members, Identity{PID: pid, StartToken: strings.Join(fields[2:], " "), ProcessGroupToken: fields[1]})
	}
	return members, nil
}
