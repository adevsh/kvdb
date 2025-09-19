// Package types defines common data strcutures for the KVDB system
// Purpose: Shared types accross storage, network, and raft components

package types

import "time"

// Command represents a database operation for Raft replication
type Command struct {
	Op    string `json:"op"`    // Operation: PUT, DEL, etc.
	Key   string `json:"key"`   // Key for the operation
	Value string `json:"value"` // Value for PUT operations
}

// LogEntry represents a Raft log entry with database command
type LogEntry struct {
	Index     uint64    `json:"index"`    // Log index
	Term      uint64    `json:"term"`     // Term when entry was received
	Command   Command   `json:"command"`  // Database command
	Timestamp time.Time `json:"timestamp` // Entry creation time
}

// NodeConfig holds configuration for a cluster node
type NodeConfig struct {
	ID        int      `json:"id"`        // Node ID
	HTTPAddr  string   `json:"httpAddr"`  // HTTP API address
	RaftAddr  string   `json:"raftAddr"`  // Raft protocol address
	DataDir   string   `json:"dataDir"`   // Data directory path
	Peers     []string `json:"peers"`     // Cluster peer addresses
	Bootstrap bool     `json:"bootstrap"` // Bootstrap the cluster
}

// Response represents a generic API response
type Response struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// SnapshotMetadata containts metadata about a storage snapshot
type SnapshotMetadata struct {
	LastIndex uint64    `json:"lastIndex"` // Last included log index
	LastTerm  uint64    `json:"lastTerm"`  // Last included log term
	Timestamp time.Time `json:"timestamp"` // Snapshot creation time
	Size      int64     `json:"size"`      // Snapshot size in bytes
}
