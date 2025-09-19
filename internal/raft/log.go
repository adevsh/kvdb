// Package raft implements the Raft consensus algorithm
// log.go: Persistent log storage for Raft entries
package raft

import (
	"fmt"
	"kvdb/pkg/types"
	"os"
	"sync"
)

// LogStore manages persitent storage of Raft log entries
type LogStore struct {
	mu         sync.RWMutex
	wal        WAL
	entries    []types.LogEntry // Memory cache
	startIndex uint64           // Index of first entry (for snapshots)
	lastIndex  uint64           // Index of last entry
	lastTerm   uint64           // Term of last entry
	logDir     string
}

// WAL represents the write-ahead log interface
type WAL interface {
	Write(entry types.LogEntry) error
	Reply(handler func(entry types.LogEntry) error, startIndex uint64) error
	Close() error
	Truncate() error
}

// NewLogStore creates a new persistent log store
func NewLogStore(logDir string, wal WAL) (*LogStore, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	store := &LogStore{
		wal:     wal,
		entries: make([]types.LogEntry, 0),
		logDir:  logDir,
	}

	// Load existing entries from WAL
	if err := store.loadFromWAL(); err != nil {
		return nil, fmt.Errorf("failed to load from WAL: %w", err)
	}

	return store, nil
}

// loadFromWAL replays WAL entries to rebuild in-memory log
func (ls *LogStore) loadFromWAL() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	return ls.wal.Reply(func(entry types.LogEntry) error {
		ls.entries = append(ls.entries, entry)
		ls.lastIndex = entry.Index
		ls.lastTerm = entry.Term
		return nil
	}, 0)
}

// Append adds a new entry to the log
func (ls *LogStore) Append(entry types.LogEntry) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// Validate entry index
	if entry.Index != ls.lastIndex+1 {
		return fmt.Errorf("invalid index: expected %d, got %d", ls.lastIndex+1, entry.Index)
	}

	// Write to WAL first
	if err := ls.wal.Write(entry); err != nil {
		return fmt.Errorf("WAL write failed: %w", err)
	}

	// Update in-memory log
	ls.entries = append(ls.entries, entry)
	ls.lastIndex = entry.Index
	ls.lastTerm = entry.Term

	return nil
}

// Get retrieves an entry by index
func (ls *LogStore) Get(index uint64) (types.LogEntry, bool) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	if index < ls.startIndex || index > ls.lastIndex {
		return types.LogEntry{}, false
	}

	// Calculate position in entries slice
	pos := int(index - ls.startIndex - 1)
	if pos < 0 || pos >= len(ls.entries) {
		return types.LogEntry{}, false
	}

	return ls.entries[pos], true
}

// GetRange returns entries between start and end indices
func (ls *LogStore) GetRange(start, end uint64) ([]types.LogEntry, error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	if start < ls.startIndex || end > ls.lastIndex || start > end {
		return nil, fmt.Errorf("invalid range: %d-%d", start, end)
	}

	var result []types.LogEntry
	for i := start; i <= end; i++ {
		pos := int(i - ls.startIndex - 1)
		if pos >= 0 && pos < len(ls.entries) {
			result = append(result, ls.entries[pos])
		}
	}

	return result, nil
}

// LastIndex returns the last log index
func (ls *LogStore) LastIndex() uint64 {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	return ls.lastIndex
}

// LastTerm returns the last log term
func (ls *LogStore) LastTerm() uint64 {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	return ls.lastTerm
}

// Term returns the term of an entry at given index
func (ls *LogStore) Term(index uint64) (uint64, bool) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	if index < ls.startIndex || index > ls.lastIndex {
		return 0, false
	}

	if index == ls.startIndex {
		return 0, true // Snapshot term would be stored separately
	}

	pos := int(index - ls.startIndex - 1)
	if pos < 0 || pos >= len(ls.entries) {
		return 0, false
	}

	return ls.entries[pos].Term, true
}

// TruncatePrefix removes entries before the given index
func (ls *LogStore) TruncatePrefix(newStartIndex uint64) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if newStartIndex <= ls.startIndex {
		return nil // Nothing to do
	}

	if newStartIndex > ls.lastIndex {
		return fmt.Errorf("new start index %d exceeds last index %d", newStartIndex, ls.lastIndex)
	}

	// Calculate entries to keep
	keepFrom := int(newStartIndex - ls.startIndex - 1)
	if keepFrom < 0 {
		keepFrom = 0
	}

	ls.entries = ls.entries[keepFrom:]
	ls.startIndex = newStartIndex

	// Truncate WAL (in real implementation, this would involve snapshotting)

	return ls.wal.Truncate()
}

// Close cleanly shuts down the log store
func (ls *LogStore) Close() error {
	return ls.wal.Close()
}

// Size returns the number of entries in the log
func (ls *LogStore) Size() int {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	return len(ls.entries)
}
