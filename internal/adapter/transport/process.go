// Package transport contains the small amount of process plumbing shared by
// native harness adapters. Protocol details stay in the adapter packages.
package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/vnai/subagent-broker/internal/adapter"
	"github.com/vnai/subagent-broker/internal/process"
)

// Process owns one harness process and exposes lossless line/stderr/exit
// streams. The adapter is responsible for interpreting stdout lines.
type Process struct {
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	identity  process.Identity
	lines     chan []byte
	stderr    chan adapter.OutputChunk
	exited    chan adapter.ExitStatus
	exitReady chan struct{}
	exitMu    sync.RWMutex
	exitValue adapter.ExitStatus
	writeMu   sync.Mutex
	closed    bool
}

// Start starts executable with an isolated process group and begins draining
// all three process streams before returning.
func Start(ctx context.Context, executable string, args []string, dir string) (*Process, error) {
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Dir = dir
	process.ConfigureCommand(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, err
	}
	identity, err := process.Inspect(context.Background(), cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, err
	}

	p := &Process{
		cmd: cmd, stdin: stdin, identity: identity,
		lines: make(chan []byte, 256), stderr: make(chan adapter.OutputChunk, 128),
		exited:    make(chan adapter.ExitStatus, 1),
		exitReady: make(chan struct{}),
	}
	go p.readLines(stdout)
	go p.readStderr(stderr)
	go p.wait()
	return p, nil
}

func (p *Process) Lines() <-chan []byte               { return p.lines }
func (p *Process) Stderr() <-chan adapter.OutputChunk { return p.stderr }
func (p *Process) Exited() <-chan adapter.ExitStatus  { return p.exited }
func (p *Process) Identity() process.Identity         { return p.identity }

// Wait returns the exit status without consuming the public Exited stream.
// Adapters use it for internal cleanup while Supervisor independently observes
// the same lifecycle through Session.Exited.
func (p *Process) Wait(ctx context.Context) (adapter.ExitStatus, error) {
	select {
	case <-p.exitReady:
		p.exitMu.RLock()
		defer p.exitMu.RUnlock()
		return p.exitValue, nil
	case <-ctx.Done():
		return adapter.ExitStatus{}, ctx.Err()
	}
}

// WriteJSON writes one JSONL protocol message atomically with respect to
// other adapter requests.
func (p *Process) WriteJSON(value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if p.closed {
		return os.ErrProcessDone
	}
	_, err = p.stdin.Write(data)
	return err
}

func (p *Process) CloseInput() error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.closed = true
	return p.stdin.Close()
}

func (p *Process) Terminate(ctx context.Context) error {
	return process.TerminateGracefully(ctx, p.identity)
}

func (p *Process) Interrupt(ctx context.Context) error {
	return process.Interrupt(ctx, p.identity)
}

func (p *Process) readLines(reader io.Reader) {
	defer close(p.lines)
	br := bufio.NewReader(reader)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			p.lines <- append([]byte(nil), line...)
		}
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			return
		}
	}
}

func (p *Process) readStderr(reader io.Reader) {
	defer close(p.stderr)
	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			p.stderr <- adapter.OutputChunk{Timestamp: time.Now().UTC(), Data: append([]byte(nil), buf[:n]...)}
		}
		if err != nil {
			return
		}
	}
}

func (p *Process) wait() {
	err := p.cmd.Wait()
	status := adapter.ExitStatus{Code: 0}
	if err != nil {
		status.Code = -1
		status.Error = err.Error()
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			status.Code = exitErr.ExitCode()
		}
	}
	p.writeMu.Lock()
	p.closed = true
	_ = p.stdin.Close()
	p.writeMu.Unlock()
	p.exitMu.Lock()
	p.exitValue = status
	p.exitMu.Unlock()
	close(p.exitReady)
	p.exited <- status
	close(p.exited)
}
