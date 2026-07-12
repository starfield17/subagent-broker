package event

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/vnai/subagent-broker/internal/storage"
)

const SchemaVersion = "v1alpha1"

type Event struct {
	SchemaVersion string          `json:"schema_version"`
	Seq           uint64          `json:"seq"`
	EventID       string          `json:"event_id"`
	RunID         string          `json:"run_id"`
	TaskID        string          `json:"task_id,omitempty"`
	WorkerID      string          `json:"worker_id,omitempty"`
	Timestamp     time.Time       `json:"timestamp"`
	Source        string          `json:"source"`
	Type          string          `json:"type"`
	Severity      string          `json:"severity"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

type Input struct {
	TaskID    string
	WorkerID  string
	Timestamp time.Time
	Source    string
	Type      string
	Severity  string
	Payload   any
}

type Store struct {
	mu      sync.Mutex
	path    string
	runID   string
	nextSeq uint64
	now     func() time.Time
	newID   func() (string, error)
}

func NewStore(path, runID string, startSeq uint64) *Store {
	return &Store{path: path, runID: runID, nextSeq: startSeq + 1, now: time.Now, newID: randomID}
}

func (s *Store) Append(input Input) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if input.Type == "" || input.Source == "" {
		return Event{}, fmt.Errorf("event source and type are required")
	}
	payload, err := json.Marshal(input.Payload)
	if err != nil {
		return Event{}, fmt.Errorf("marshal event payload: %w", err)
	}
	id, err := s.newID()
	if err != nil {
		return Event{}, err
	}
	timestamp := input.Timestamp
	if timestamp.IsZero() {
		timestamp = s.now().UTC()
	}
	e := Event{SchemaVersion: SchemaVersion, Seq: s.nextSeq, EventID: id, RunID: s.runID, TaskID: input.TaskID, WorkerID: input.WorkerID, Timestamp: timestamp.UTC(), Source: input.Source, Type: input.Type, Severity: input.Severity, Payload: payload}
	line, err := json.Marshal(e)
	if err != nil {
		return Event{}, err
	}
	if err := storage.AppendJSONL(s.path, line, 0o600); err != nil {
		return Event{}, err
	}
	s.nextSeq++
	return e, nil
}

type ReplayResult struct {
	Events         []Event
	IncompleteTail bool
	TailRepaired   bool
	QuarantinePath string
}

func Replay(path string) (ReplayResult, error) {
	var events []Event
	var lastSeq uint64
	var runID string
	repair, err := storage.ReplayJSONL(path, storage.JSONLReplayOptions{RepairIncompleteTail: true}, func(line []byte, _ int) error {
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			return fmt.Errorf("decode event: %w", err)
		}
		if e.SchemaVersion == "" || e.EventID == "" || e.RunID == "" {
			return fmt.Errorf("schema_version, event_id, and run_id are required")
		}
		if e.Seq == 0 {
			return fmt.Errorf("event seq must be greater than 0")
		}
		if e.Seq <= lastSeq {
			return fmt.Errorf("non-monotonic event sequence: %d after %d", e.Seq, lastSeq)
		}
		if runID == "" {
			runID = e.RunID
		} else if e.RunID != runID {
			return fmt.Errorf("event run_id changed from %q to %q", runID, e.RunID)
		}
		lastSeq = e.Seq
		return nil
	}, func(line []byte, _ int) error {
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			return fmt.Errorf("decode event: %w", err)
		}
		events = append(events, e)
		return nil
	})
	result := ReplayResult{
		Events:         events,
		IncompleteTail: repair.IncompleteTail,
		TailRepaired:   repair.Repaired,
		QuarantinePath: repair.QuarantinePath,
	}
	if err != nil {
		return result, err
	}
	return result, nil
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
