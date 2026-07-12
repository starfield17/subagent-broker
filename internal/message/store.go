package message

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/vnai/subagent-broker/internal/storage"
)

type Store struct {
	mu   sync.Mutex
	path string
}

func NewStore(path string) *Store { return &Store{path: path} }

// Path returns the journal file path.
func (s *Store) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *Store) Append(value Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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

// ReplayDetailed replays the message journal with incomplete-tail repair enabled.
func ReplayDetailed(path string) (ReplayResult, error) {
	messages := map[string]Message{}
	type identity struct {
		RunID  string
		TaskID string
		Type   Type
	}
	seen := map[string]identity{}
	repair, err := storage.ReplayJSONL(path, storage.JSONLReplayOptions{RepairIncompleteTail: true}, func(line []byte, _ int) error {
		var value Message
		if err := json.Unmarshal(line, &value); err != nil {
			return fmt.Errorf("decode message: %w", err)
		}
		if value.MessageID == "" || value.RunID == "" || value.Type == "" || value.Status == "" {
			return fmt.Errorf("message_id, run_id, type, and status are required")
		}
		if previous, ok := seen[value.MessageID]; ok {
			if previous.RunID != value.RunID || previous.TaskID != value.TaskID || previous.Type != value.Type {
				return fmt.Errorf("message %q identity drift: run_id/task_id/type must remain stable", value.MessageID)
			}
		} else {
			seen[value.MessageID] = identity{RunID: value.RunID, TaskID: value.TaskID, Type: value.Type}
		}
		return nil
	}, func(line []byte, _ int) error {
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
