// Package storage handles data persistence and recovery
// Purpose: Manage in-memory data with WAL and snapshot persistance

package storage

import (
	"encoding/json"
	"fmt"
	"kvdb/pkg/types"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Storage represents the persistance key-value store
type Storage struct {
	mu        sync.RWMutex
	data      map[string]string // In-memory data store
	wal       *WAL              // Write-Ahead Log
	snapshot  *SnapshotManager  // Snapshot manager
	dataDir   string            // Data directory path
	lastIndex uint64            // Last applied log index
}

// NewStorage creates a new storage instance with recovery
func NewStorage(dataDir string) (*Storage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	wal, err := NewWAL(filepath.Join(dataDir, "wal.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	snapshot, err := NewSnapshotManager(filepath.Join(dataDir, "snapshots"))
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot manager: %w", err)
	}

	storage := &Storage{
		data:     make(map[string]string),
		wal:      wal,
		snapshot: snapshot,
		dataDir:  dataDir,
	}

	// Recover from latest snaphost and reply WAL
	if err := storage.Recover(); err != nil {
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	return storage, nil
}

// Recover restores state from snapshot and WAL
func (s *Storage) Recover() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Try to load from latest snapshot
	snapshotIndex, err := s.snapshot.LoadLatest(s)
	if err != nil {
		return fmt.Errorf("snapshot load failed: %w", err)
	}

	// Replay WAL from after snapshot index
	if err := s.wal.Replay(s, snapshotIndex); err != nil {
		return fmt.Errorf("WAL replay failed: %w", err)
	}

	return nil
}

// ApplyCommand applies a Raft log entry to the storage
func (s *Storage) ApplyCommand(entry types.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write to WAL first for crash safety
	walEntry := WALEntry{
		Index:   entry.Index,
		Term:    entry.Term,
		Command: entry.Command,
	}

	if err := s.wal.Write(walEntry); err != nil {
		return fmt.Errorf("WAL write failed: %w", err)
	}

	// Apply command to in-memory store
	switch entry.Command.Op {
	case "PUT":
		s.data[entry.Command.Key] = entry.Command.Value
	case "DEL":
		delete(s.data, entry.Command.Key)
	default:
		return fmt.Errorf("unknown operation: %s", entry.Command.Op)
	}

	s.lastIndex = entry.Index

	// Check if we should create a snapshot
	if s.lastIndex%1000 == 0 { // Snapshot every 1000 commands
		if err := s.CreateSnapshot(); err != nil {
			return fmt.Errorf("snapshot creation failed: %w", err)
		}
	}

	return nil
}

// CreateSnapshot creates a persistent snapshot of current state
func (s *Storage) CreateSnapshot() error {
	snapshotData, err := json.Marshal(s.data)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	metadata := types.SnapshotMetadata{
		LastIndex: s.lastIndex,
		LastTerm:  0, // Will be set by Raft
		Timestamp: time.Now(),
		Size:      int64(len(snapshotData)),
	}

	return s.snapshot.Create(snapshotData, metadata)
}

// Get retrieves a value by key
func (s *Storage) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, exists := s.data[key]
	return value, exists
}

// Count returns the number of key-value pairs
func (s *Storage) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.data)
}

// Close cleanly shuts down the storage
func (s *Storage) Close() error {
	if err := s.wal.Close(); err != nil {
		return err
	}

	return s.snapshot.Close()
}

// GetData returns a copy of the current data  (for snapshots)
func (s *Storage) GetData() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data := make(map[string]string)
	for k, v := range s.data {
		data[k] = v
	}

	return data
}

// SetData replaces the current data (for snapshot loading)
func (s *Storage) SetData(data map[string]string, lastIndex uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = data
	s.lastIndex = lastIndex
}

// LastIndex returns the last applied log index
func (s *Storage) LastIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lastIndex
}
