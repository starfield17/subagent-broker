package message

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
	"sort"
	"sync"
	"time"
)

type Store struct {
	mu   sync.Mutex
	path string
}

func NewStore(path string) *Store { return &Store{path: path} }

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
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func Replay(path string) (map[string]Message, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]Message{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	result := map[string]Message{}
	reader := bufio.NewReader(file)
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] == '\n' {
			var value Message
			if err := json.Unmarshal(line, &value); err != nil {
				return nil, err
			}
			result[value.MessageID] = value
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return result, nil
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
