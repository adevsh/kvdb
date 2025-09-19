// Package storage handles data persistance and recovery
// Purpose: Manages database snapshots for log compaction

package storage

import (
	"encoding/json"
	"fmt"
	"kvdb/pkg/types"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// SnapshotManager handles database snapshot creation and restoration
type SnapshotManager struct {
	mu           sync.Mutex
	snapshotDir  string
	maxSnapshots int
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(snapshotDir string) (*SnapshotManager, error) {
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	return &SnapshotManager{
		snapshotDir:  snapshotDir,
		maxSnapshots: 5, // Keep last 5 snapshots
	}, nil
}

// Create creates a new snapshot with the given data and metada
func (sm *SnapshotManager) Create(data []byte, metadata types.SnapshotMetadata) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Generate snapshot filename
	timestamp := metadata.Timestamp.Format("20060102_150405")
	filename := fmt.Sprintf("snapshot_%s_%d.json", timestamp, metadata.LastIndex)
	filepath := filepath.Join(sm.snapshotDir, filename)

	// Create snapshot file
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()

	// Write snapshot data
	snapshot := struct {
		Metadata types.SnapshotMetadata `json:"metadata"`
		Data     []byte                 `json:"data"`
	}{
		Metadata: metadata,
		Data:     data,
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(snapshot); err != nil {
		return fmt.Errorf("failed to encode snapshot: %w", err)
	}

	// Clean up old snapshots
	if err := sm.cleanupOldSnapshots(); err != nil {
		fmt.Printf("Warning: failed to clean up old snapshots: %v\n", err)
	}

	fmt.Printf("Created snapshot: %s (index: %d)\n", filename, metadata.LastIndex)
	return nil
}

// LoadLatest loads the most recent snapshot into the storage
func (sm *SnapshotManager) LoadLatest(storage *Storage) (uint64, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	snapshots, err := sm.listSnapshots()
	if err != nil {
		return 0, fmt.Errorf("failed to list snapshots: %w", err)
	}

	if len(snapshots) == 0 {
		return 0, nil // No snapshots available
	}

	// Load the latest snapshot
	latest := snapshots[len(snapshots)-1]
	filepath := filepath.Join(sm.snapshotDir, latest)

	file, err := os.Open(filepath)
	if err != nil {
		return 0, fmt.Errorf("failed to open snapshot file: %w", err)
	}

	defer file.Close()

	var snapshot struct {
		Metadata types.SnapshotMetadata `json:"metadata"`
		Data     []byte                 `json:"data"`
	}

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&snapshot); err != nil {
		return 0, fmt.Errorf("failed to decode snapshot: %w", err)
	}

	// Parse snapshot data
	var data map[string]string
	if err := json.Unmarshal(snapshot.Data, &data); err != nil {
		return 0, fmt.Errorf("failed to unmarshal snapshot data: %w", err)
	}

	// Load data into storage
	storage.SetData(data, snapshot.Metadata.LastIndex)

	fmt.Printf("Loaded snapshot: %s (index: %d, entries: %d)\n",
		latest, snapshot.Metadata.LastIndex, len(data))

	return snapshot.Metadata.LastIndex, nil
}

// listSnapshots returns all snapshot files sorted by timestamp
func (sm *SnapshotManager) listSnapshots() ([]string, error) {
	entries, err := os.ReadDir(sm.snapshotDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot directory: %w", err)
	}

	var snapshots []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			snapshots = append(snapshots, entry.Name())
		}
	}

	// Sort by filename (which includes timestamp)
	sort.Strings(snapshots)
	return snapshots, nil
}

// cleanupOldSnapshots removes old snapshots beyond the limit
func (sm *SnapshotManager) cleanupOldSnapshots() error {
	snapshots, err := sm.listSnapshots()
	if err != nil {
		return err
	}

	if len(snapshots) <= sm.maxSnapshots {
		return nil
	}

	// Remove oldest snapshots
	toRemove := len(snapshots) - sm.maxSnapshots
	for i := 0; i < toRemove; i++ {
		filepath := filepath.Join(sm.snapshotDir, snapshots[i])
		if err := os.Remove(filepath); err != nil {
			return fmt.Errorf("failed to remove snapshot %s: %w", snapshots[i], err)
		}
		fmt.Printf("Removed old snapshot: %s\n", snapshots[i])
	}

	return nil
}

// GetLatestSnapshotInfo returns metadata for the latest snapshot
func (sm *SnapshotManager) GetLatestSnapshotInfo() (*types.SnapshotMetadata, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	snapshots, err := sm.listSnapshots()
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		return nil, nil
	}

	latest := snapshots[len(snapshots)-1]
	filepath := filepath.Join(sm.snapshotDir, latest)

	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot file: %w", err)
	}

	defer file.Close()

	var snapshot struct {
		Metadata types.SnapshotMetadata `json:"metadata"`
		Data     []byte                 `json:"data"`
	}

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("failed to decode snapshot: %w", err)
	}

	return &snapshot.Metadata, nil
}

// Close cleans up the snapshot manager
func (sm *SnapshotManager) Close() error {
	return nil // Nothing to close for now
}
