// Package storage handles data persistence and recovery
// wal_raft.go: WAL implementation spesifically for Raft log entries
package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"kvdb/pkg/types"
	"os"
	"sync"
)

// RaftWAL implements WAL interface for Raft log entries
type RaftWAL struct {
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
	path   string
}

// NewRaftWAL creates a new WAL for Raft entries
func NewRaftWAL(path string) (*RaftWAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open Raft WAL file: %w", err)
	}

	return &RaftWAL{
		file:   file,
		writer: bufio.NewWriter(file),
		path:   path,
	}, nil
}

// Write appends a Raft log entry to the WAL
func (rw *RaftWAL) Write(entry types.LogEntry) error {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal Raft log entry: %w", err)
	}

	if _, err := rw.writer.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write to Raft WAL: %w", err)
	}

	return rw.writer.Flush()
}

// Replay reads the WAL and processes entries with a handler
func (rw *RaftWAL) Replay(handler func(entry types.LogEntry) error, startIndex uint64) error {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	// Close current writer and reopen for reading
	rw.writer.Flush()
	rw.file.Close()

	file, err := os.Open(rw.path)
	if err != nil {
		if os.IsNotExist(err) {
			// No WAL exists yet, recreate it
			file, err := os.OpenFile(rw.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("failed to create Raft WAL file: %w", err)
			}

			rw.file = file
			rw.writer = bufio.NewWriter(file)
			return nil
		}

		return fmt.Errorf("failed to open Raft WAL for replay: %w", err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	currentIndex := uint64(0)

	for scanner.Scan() {
		currentIndex++
		if currentIndex < startIndex {
			continue // Skip entries before startIndex
		}

		var entry types.LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return fmt.Errorf("failed to unmarshal Raft log entry: %w", err)
		}

		if err := handler(entry); err != nil {
			return fmt.Errorf("handler failed for entry %d: %w", currentIndex, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading Raft WAL: %w", err)
	}

	// Reopen the file for writing
	file, err = os.OpenFile(rw.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen Raft WAL file: %w", err)
	}

	rw.file = file
	rw.writer = bufio.NewWriter(file)

	return nil
}

// Close closes the WAL file
func (rw *RaftWAL) Close() error {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if err := rw.writer.Flush(); err != nil {
		return err
	}

	return rw.file.Close()
}

// Truncate clears the WAL file
func (rw *RaftWAL) Truncate() error {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	rw.writer.Flush()
	rw.file.Close()

	file, err := os.OpenFile(rw.path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to truncate Raft WAL: %w", err)
	}

	rw.file = file
	rw.writer = bufio.NewWriter(file)

	return nil
}
