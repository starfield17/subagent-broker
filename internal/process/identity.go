package process

import "context"

type Identity struct {
	PID               int    `json:"pid"`
	StartToken        string `json:"start_token"`
	ProcessGroupToken string `json:"process_group_token,omitempty"`
}

// Complete reports whether identity carries enough information to target a
// process tree without relying on PID alone.
func (i Identity) Complete() bool {
	return i.PID > 0 && i.StartToken != "" && i.ProcessGroupToken != ""
}

// SameProcess reports whether two identities refer to the same OS process.
// PID equality alone is never sufficient.
func (i Identity) SameProcess(other Identity) bool {
	return i.PID > 0 && i.PID == other.PID && i.StartToken != "" && i.StartToken == other.StartToken
}

// SameGroup reports whether two identities share a process-group token.
// PID equality alone is never sufficient.
func (i Identity) SameGroup(other Identity) bool {
	return i.ProcessGroupToken != "" && i.ProcessGroupToken == other.ProcessGroupToken
}

// TreeManager inspects and signals process trees. Production code uses the
// platform package functions; tests inject fakes.
type TreeManager interface {
	Inspect(ctx context.Context, pid int) (Identity, error)
	GroupMembers(ctx context.Context, identity Identity) ([]Identity, error)
	Interrupt(ctx context.Context, identity Identity) error
	TerminateGracefully(ctx context.Context, identity Identity) error
	KillTree(ctx context.Context, identity Identity) error
}

// PlatformManager delegates to the platform-specific package functions.
type PlatformManager struct{}

func (PlatformManager) Inspect(ctx context.Context, pid int) (Identity, error) {
	return Inspect(ctx, pid)
}

func (PlatformManager) GroupMembers(ctx context.Context, identity Identity) ([]Identity, error) {
	return GroupMembers(ctx, identity)
}

func (PlatformManager) Interrupt(ctx context.Context, identity Identity) error {
	return Interrupt(ctx, identity)
}

func (PlatformManager) TerminateGracefully(ctx context.Context, identity Identity) error {
	return TerminateGracefully(ctx, identity)
}

func (PlatformManager) KillTree(ctx context.Context, identity Identity) error {
	return KillTree(ctx, identity)
}
