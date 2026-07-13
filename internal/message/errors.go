package message

import "fmt"

// Typed resolution errors for Service-level decision operations.
// Prefer errors.Is / type switches over matching free-form strings.

// ErrResolutionConflict is returned when a submitted resolution conflicts with
// a frozen or terminal resolution.
type ErrResolutionConflict struct {
	MessageID string
	Status    Status
}

func (e *ErrResolutionConflict) Error() string {
	return fmt.Sprintf("resolution_conflict: message %q status=%s", e.MessageID, e.Status)
}

// ErrMessageTerminalNotAnswered is returned when resolution is submitted against
// a terminal message that is not Answered (Expired, Failed, etc.).
// Even an identical historical resolution is not success.
type ErrMessageTerminalNotAnswered struct {
	MessageID string
	Status    Status
}

func (e *ErrMessageTerminalNotAnswered) Error() string {
	return fmt.Sprintf("message_terminal_not_answered: message %q status=%s", e.MessageID, e.Status)
}

// ErrResolutionReconciliationRequired is returned when expiration cannot safely
// proceed because a frozen resolution exists on a still-queued message.
type ErrResolutionReconciliationRequired struct {
	MessageID string
	Reason    string
}

func (e *ErrResolutionReconciliationRequired) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("resolution_reconciliation_required: message %q: %s", e.MessageID, e.Reason)
	}
	return fmt.Sprintf("resolution_reconciliation_required: message %q", e.MessageID)
}
