package message

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/vnai/subagent-broker/internal/storage"
)

type Store struct {
	mu             sync.Mutex
	path           string
	appendDisabled bool
	disabledReason string
}

func NewStore(path string) *Store { return &Store{path: path} }

// Path returns the journal file path.
func (s *Store) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// DisableAppend permanently refuses further journal writes (e.g. after lifecycle corruption).
func (s *Store) DisableAppend(reason string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendDisabled = true
	if reason == "" {
		reason = "message journal append disabled"
	}
	s.disabledReason = reason
}

// AppendDisabled reports whether Append will refuse writes.
func (s *Store) AppendDisabled() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendDisabled
}

func (s *Store) Append(value Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.appendDisabled {
		return fmt.Errorf("message journal append disabled: %s", s.disabledReason)
	}
	// Durable marker written by Supervisor Load on journal corruption.
	if s.path != "" {
		if _, err := os.Stat(s.path + ".append-disabled"); err == nil {
			s.appendDisabled = true
			s.disabledReason = "message journal append disabled by corruption marker"
			return fmt.Errorf("message journal append disabled: %s", s.disabledReason)
		}
	}
	if value.MessageID == "" || value.RunID == "" || value.Type == "" || value.Status == "" {
		return fmt.Errorf("message identity, type, and status are required")
	}
	line, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return storage.AppendJSONL(s.path, line, 0o600)
}

// ReplayResult captures message journal replay metadata.
type ReplayResult struct {
	Messages       map[string]Message
	TailRepaired   bool
	QuarantinePath string
}

// ErrJournalCorrupt marks a complete journal record that violates lifecycle rules.
// Unlike incomplete-tail repair, this is not auto-truncated.
type ErrJournalCorrupt struct {
	MessageID string
	Reason    string
}

func (e *ErrJournalCorrupt) Error() string {
	if e == nil {
		return "message journal corrupt"
	}
	if e.MessageID == "" {
		return "message journal corrupt: " + e.Reason
	}
	return fmt.Sprintf("message journal corrupt for %s: %s", e.MessageID, e.Reason)
}

// ReplayDetailed replays the message journal with incomplete-tail repair enabled
// and full lifecycle validation for complete records.
func ReplayDetailed(path string) (ReplayResult, error) {
	messages := map[string]Message{}
	previousByID := map[string]Message{}
	validateLine := func(line []byte, _ int) error {
		var value Message
		if err := json.Unmarshal(line, &value); err != nil {
			return fmt.Errorf("decode message: %w", err)
		}
		if value.MessageID == "" || value.RunID == "" || value.Type == "" || value.Status == "" {
			return fmt.Errorf("message_id, run_id, type, and status are required")
		}
		if previous, ok := previousByID[value.MessageID]; ok {
			if err := validateMessageLifecycle(previous, value); err != nil {
				return &ErrJournalCorrupt{MessageID: value.MessageID, Reason: err.Error()}
			}
		} else {
			// First complete record for this id must obey status/resolution rules and
			// any DeliveryMode must be a legal first routing assignment.
			if err := validateMessageStatusFields(value); err != nil {
				return &ErrJournalCorrupt{MessageID: value.MessageID, Reason: err.Error()}
			}
			if value.DeliveryMode != "" {
				if err := validateDeliveryModeFirstAssignment(value); err != nil {
					return &ErrJournalCorrupt{MessageID: value.MessageID, Reason: err.Error()}
				}
			}
		}
		previousByID[value.MessageID] = value
		return nil
	}
	repair, err := storage.ReplayJSONL(path, storage.JSONLReplayOptions{RepairIncompleteTail: true}, validateLine, func(line []byte, _ int) error {
		var value Message
		if err := json.Unmarshal(line, &value); err != nil {
			return fmt.Errorf("decode message: %w", err)
		}
		messages[value.MessageID] = value
		return nil
	})
	result := ReplayResult{
		Messages:       messages,
		TailRepaired:   repair.Repaired,
		QuarantinePath: repair.QuarantinePath,
	}
	if err != nil {
		if result.Messages == nil {
			result.Messages = map[string]Message{}
		}
		return result, err
	}
	return result, nil
}

// validateMessageLifecycle checks status machine and immutable/monotonic fields.
// Same-status records are only allowed when immutable fields are identical and
// monotonic fields do not regress — payload/DeliveryMode replacement is corruption.
func validateMessageLifecycle(previous, next Message) error {
	// Absolute status/resolution rules for the new record.
	if err := validateMessageStatusFields(next); err != nil {
		return err
	}

	// Immutable identity / content fields (must never change once recorded).
	if previous.MessageID != next.MessageID {
		return fmt.Errorf("message_id drift")
	}
	if previous.RunID != next.RunID || previous.TaskID != next.TaskID || previous.Type != next.Type {
		return fmt.Errorf("identity drift: run_id/task_id/type must remain stable")
	}
	if previous.SchemaVersion != next.SchemaVersion {
		return fmt.Errorf("schema_version changed from %q to %q", previous.SchemaVersion, next.SchemaVersion)
	}
	if previous.Category != next.Category {
		return fmt.Errorf("category changed from %q to %q", previous.Category, next.Category)
	}
	if previous.InReplyTo != next.InReplyTo {
		return fmt.Errorf("in_reply_to changed from %q to %q", previous.InReplyTo, next.InReplyTo)
	}
	if !previous.CreatedAt.Equal(next.CreatedAt) {
		return fmt.Errorf("created_at must remain stable")
	}
	if !bytes.Equal(previous.Payload, next.Payload) {
		return fmt.Errorf("payload is immutable and must not change")
	}

	// DeliveryMode: unset → set once only for Instruction in Validated/Queued;
	// once set must not change or clear, except the explicit next_turn → resume
	// reclassification when the active session is gone but resume is possible.
	if previous.DeliveryMode != "" {
		if next.DeliveryMode == "" {
			return fmt.Errorf("delivery_mode cannot be cleared once set")
		}
		if next.DeliveryMode != previous.DeliveryMode {
			if !(previous.Type == Instruction &&
				previous.Status == Queued && next.Status == Queued &&
				previous.DeliveryMode == DeliveryNextTurn && next.DeliveryMode == DeliveryResume) {
				return fmt.Errorf("delivery_mode changed from %q to %q", previous.DeliveryMode, next.DeliveryMode)
			}
		}
	} else if next.DeliveryMode != "" {
		if err := validateDeliveryModeFirstAssignment(next); err != nil {
			return err
		}
	}

	// Monotonic fields.
	if next.UpdatedAt.Before(previous.UpdatedAt) {
		return fmt.Errorf("updated_at went backwards")
	}
	if next.DeliveryAttempts < previous.DeliveryAttempts {
		return fmt.Errorf("delivery_attempts went backwards (%d -> %d)", previous.DeliveryAttempts, next.DeliveryAttempts)
	}

	// Worker/Attempt: unset → set is allowed; once set must not clear or replace.
	if previous.WorkerID != "" {
		if next.WorkerID == "" {
			return fmt.Errorf("worker_id cannot be cleared once set")
		}
		if previous.WorkerID != next.WorkerID {
			return fmt.Errorf("worker_id drift %q -> %q", previous.WorkerID, next.WorkerID)
		}
	}
	if previous.AttemptNumber > 0 {
		if next.AttemptNumber == 0 {
			return fmt.Errorf("attempt_number cannot be cleared once set")
		}
		if previous.AttemptNumber != next.AttemptNumber {
			return fmt.Errorf("attempt_number drift %d -> %d", previous.AttemptNumber, next.AttemptNumber)
		}
	}

	// Resolution once present cannot be cleared or rewritten.
	if len(previous.Resolution) > 0 {
		if len(next.Resolution) == 0 {
			return fmt.Errorf("resolution cannot be cleared once set")
		}
		if !bytes.Equal(previous.Resolution, next.Resolution) {
			return fmt.Errorf("resolution cannot be rewritten")
		}
	}

	if previous.Status == next.Status {
		// Same-status: immutables (including DeliveryMode) already checked.
		return nil
	}
	if err := ValidateTransition(previous.Status, next.Status); err != nil {
		return err
	}
	return nil
}

// validateMessageStatusFields enforces Resolution and DeliveryMode type/enum rules.
func validateMessageStatusFields(value Message) error {
	if err := validateDeliveryModePresence(value); err != nil {
		return err
	}
	switch value.Status {
	case Created, Validated, Delivered:
		// Final resolution must not appear before Answered, except native
		// permission frozen intent which is stored on Queued pending delivery.
		if len(value.Resolution) > 0 {
			return fmt.Errorf("status %s must not carry resolution", value.Status)
		}
	case Queued:
		// Native permission decisions may freeze Resolution while remaining
		// non-terminal Queued until Adapter.RespondPermission succeeds.
		// Other types must not carry resolution before Answered.
		if len(value.Resolution) > 0 && value.Type != PermissionRequest {
			return fmt.Errorf("status queued must not carry resolution for type %s", value.Type)
		}
	case Answered:
		if len(value.Resolution) == 0 {
			return fmt.Errorf("status answered requires resolution")
		}
	case Acknowledged, Expired, Failed:
		// Acknowledgement/terminal without forged answer is allowed; if resolution
		// is present it must remain stable once set (checked against previous).
	}
	return nil
}

// validateDeliveryModePresence: any non-empty mode must be on Instruction with a valid enum.
func validateDeliveryModePresence(value Message) error {
	if value.DeliveryMode == "" {
		return nil
	}
	if value.Type != Instruction {
		return fmt.Errorf("delivery_mode is only valid on instruction messages (type=%s)", value.Type)
	}
	if !IsValidDeliveryMode(value.DeliveryMode) {
		return fmt.Errorf("invalid delivery_mode %q", value.DeliveryMode)
	}
	return nil
}

// validateDeliveryModeFirstAssignment restricts first-time assignment of DeliveryMode
// to Instruction routing (Validated/Queued) with a legal enum value.
func validateDeliveryModeFirstAssignment(value Message) error {
	if value.DeliveryMode == "" {
		return nil
	}
	if err := validateDeliveryModePresence(value); err != nil {
		return err
	}
	switch value.Status {
	case Validated, Queued:
	default:
		return fmt.Errorf("delivery_mode may only be first set in validated/queued status (status=%s)", value.Status)
	}
	return nil
}

// Replay is a compatibility wrapper around ReplayDetailed.
func Replay(path string) (map[string]Message, error) {
	result, err := ReplayDetailed(path)
	if result.Messages == nil {
		result.Messages = map[string]Message{}
	}
	return result.Messages, err
}

func Sorted(values map[string]Message, includeResolved bool) []Message {
	result := make([]Message, 0, len(values))
	for _, value := range values {
		if !includeResolved && (value.Status == Answered || value.Status == Expired || value.Status == Failed) {
			continue
		}
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].MessageID < result[j].MessageID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}

func NewID(now time.Time) (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return "msg-" + now.UTC().Format("20060102T150405.000Z") + "-" + hex.EncodeToString(raw[:]), nil
}
