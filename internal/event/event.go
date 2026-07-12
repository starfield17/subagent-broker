package event

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
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
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return Event{}, err
	}
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return Event{}, err
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		_ = file.Close()
		return Event{}, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return Event{}, err
	}
	if err := file.Close(); err != nil {
		return Event{}, err
	}
	s.nextSeq++
	return e, nil
}

type ReplayResult struct {
	Events         []Event
	IncompleteTail bool
}

func Replay(path string) (ReplayResult, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return ReplayResult{}, nil
	}
	if err != nil {
		return ReplayResult{}, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	var result ReplayResult
	var lastSeq uint64
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			if line[len(line)-1] != '\n' {
				result.IncompleteTail = true
				break
			}
			var e Event
			if err := json.Unmarshal(line, &e); err != nil {
				return result, fmt.Errorf("decode complete event line: %w", err)
			}
			if e.Seq <= lastSeq {
				return result, fmt.Errorf("non-monotonic event sequence: %d after %d", e.Seq, lastSeq)
			}
			lastSeq = e.Seq
			result.Events = append(result.Events, e)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return result, readErr
		}
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
