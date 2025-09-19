// Package storage handles data persistence and recovery
// wal.go: Handles Write-Ahead Log operations for data persistence

package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/adevsh/kvdb/pkg/types"
)

// WALEntry represents an entry in the Write-Ahead Log
type WALEntry struct {
	Index   uint64        `json:"index"`
	Term    uint64        `json:"term"`
	Command types.Command `json:"command"`
}

// WAL manages the Write-Ahead Log for crash recovery
type WAL struct {
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
	path   string
}

// NewWAL creates or opens a Write-Ahead Log file
func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		file:   file,
		writer: bufio.NewWriter(file),
		path:   path,
	}, nil
}

// Write appends and entry to the Write-Ahead Log
func (w *WAL) Write(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal WAL entry: %w", err)
	}

	if _, err := w.writer.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Flush to ensure data is written to disk
	return w.writer.Flush()
}

// Reply reads the WAL and applies all operations to storage
func (w *WAL) Replay(storage *Storage, startIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close the current file to allow reading
	w.writer.Flush()
	w.file.Close()

	// Reopen for reading
	file, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			// No WAL file exists yet, which is fine
			file, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("failed to create WAL file: %w", err)
			}

			w.file = file
			w.writer = bufio.NewWriter(file)
			return nil
		}
		return fmt.Errorf("failed to open WAL for replay: %w", err)

	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry WALEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return fmt.Errorf("failed to unmarshal WAL entry: %w", err)
		}

		// Only apply entries after the snapshot index
		if entry.Index > startIndex {
			logEntry := types.LogEntry{
				Index:   entry.Index,
				Term:    entry.Term,
				Command: entry.Command,
			}

			if err := storage.ApplyCommand(logEntry); err != nil {
				return fmt.Errorf("failed to apply WAL entry: %w", err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading WAL: %w", err)
	}

	// Reopen the file for writing
	file, err = os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL file: %w", err)
	}

	w.file = file
	w.writer = bufio.NewWriter(file)

	return nil
}

// Close closes the Write-Ahead Log file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Truncate removes all entries from the WAL
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close current file
	w.writer.Flush()
	w.file.Close()

	// Truncate the file
	file, err := os.OpenFile(w.path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	w.file = file
	w.writer = bufio.NewWriter(file)

	return nil
}
