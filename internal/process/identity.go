package process

import "context"

type Identity struct {
	PID               int    `json:"pid"`
	StartToken        string `json:"start_token"`
	ProcessGroupToken string `json:"process_group_token,omitempty"`
}

func (i Identity) SameProcess(other Identity) bool {
	return i.PID > 0 && i.PID == other.PID && i.StartToken != "" && i.StartToken == other.StartToken
}

type TreeManager interface {
	Inspect(ctx context.Context, pid int) (Identity, error)
	Interrupt(ctx context.Context, identity Identity) error
	TerminateGracefully(ctx context.Context, identity Identity) error
	KillTree(ctx context.Context, identity Identity) error
}
