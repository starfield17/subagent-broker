package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// JSONLValidator validates one complete JSONL record (without trailing newline).
type JSONLValidator func(line []byte, lineNumber int) error

// JSONLRepairResult summarizes a ReplayJSONL scan and optional incomplete-tail repair.
type JSONLRepairResult struct {
	RecordCount     int
	LastValidOffset int64
	IncompleteTail  bool
	Repaired        bool
	QuarantinePath  string
}

// ErrIncompleteJSONLTail is returned when a journal ends with a non-empty partial line
// that has not been repaired, or when AppendJSONL refuses to write after such a tail.
var ErrIncompleteJSONLTail = errors.New("incomplete JSONL tail")

// JSONLReplayOptions controls incomplete-tail handling during ReplayJSONL.
type JSONLReplayOptions struct {
	RepairIncompleteTail bool
	Now                  func() time.Time // nil uses time.Now
}

// ReplayJSONL scans a JSONL file by byte offset.
//
// Complete records must end with '\n'. Incomplete trailing bytes (no final newline)
// are never silently ignored: either they are repaired into a quarantine file, or
// ErrIncompleteJSONLTail is returned. Corrupt complete lines always fail without
// modifying the original file.
func ReplayJSONL(
	path string,
	options JSONLReplayOptions,
	validate JSONLValidator,
	consume func(line []byte, lineNumber int) error,
) (JSONLRepairResult, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return JSONLRepairResult{}, nil
	}
	if err != nil {
		return JSONLRepairResult{}, err
	}

	var result JSONLRepairResult
	offset := 0
	lineNumber := 0
	for offset < len(data) {
		newline := -1
		for i := offset; i < len(data); i++ {
			if data[i] == '\n' {
				newline = i
				break
			}
		}
		if newline < 0 {
			if offset < len(data) {
				result.IncompleteTail = true
			}
			break
		}

		line := data[offset:newline]
		lineNumber++
		if validate != nil {
			if err := validate(line, lineNumber); err != nil {
				return result, fmt.Errorf("jsonl line %d at byte offset %d: %w", lineNumber, offset, err)
			}
		}
		if consume != nil {
			if err := consume(line, lineNumber); err != nil {
				return result, fmt.Errorf("jsonl line %d at byte offset %d: %w", lineNumber, offset, err)
			}
		}
		result.RecordCount++
		result.LastValidOffset = int64(newline + 1)
		offset = newline + 1
	}

	if !result.IncompleteTail {
		return result, nil
	}

	if !options.RepairIncompleteTail {
		return result, fmt.Errorf("%w: %s", ErrIncompleteJSONLTail, path)
	}

	tail := data[result.LastValidOffset:]
	now := time.Now
	if options.Now != nil {
		now = options.Now
	}
	quarantinePath := path + ".corrupt-tail." + now().UTC().Format("20060102T150405.000Z")
	if err := AtomicWriteFile(quarantinePath, tail, 0o600); err != nil {
		return result, fmt.Errorf("quarantine incomplete JSONL tail: %w", err)
	}
	if err := truncateAndSync(path, result.LastValidOffset); err != nil {
		return result, fmt.Errorf("truncate incomplete JSONL tail: %w", err)
	}
	syncParentDir(path)

	result.Repaired = true
	result.QuarantinePath = quarantinePath
	return result, nil
}

// AppendJSONL appends one JSON-encoded record followed by a newline.
// encodedRecord must be non-empty and must not already end with '\n'.
// Append refuses to write when the existing file has an unrepaired incomplete tail.
func AppendJSONL(path string, encodedRecord []byte, mode os.FileMode) error {
	if len(encodedRecord) == 0 {
		return fmt.Errorf("encoded JSONL record must not be empty")
	}
	if encodedRecord[len(encodedRecord)-1] == '\n' {
		return fmt.Errorf("encoded JSONL record must not include a trailing newline")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if err := ensureCompleteJSONLTail(path); err != nil {
		return err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := file.Write(encodedRecord); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write([]byte{'\n'}); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func ensureCompleteJSONLTail(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	buf := make([]byte, 1)
	if _, err := file.ReadAt(buf, info.Size()-1); err != nil {
		return err
	}
	if buf[0] != '\n' {
		return fmt.Errorf("%w: refusing to append to %s", ErrIncompleteJSONLTail, path)
	}
	return nil
}

func truncateAndSync(path string, offset int64) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := file.Truncate(offset); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func syncParentDir(path string) {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return
	}
	_ = dir.Sync()
	_ = dir.Close()
}
