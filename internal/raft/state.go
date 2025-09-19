// Package raft implements the Raft consensus algorithm
// state.go: Manages Raft node state and persistence

package raft

import (
	"encoding/json"
	"fmt"
	"kvdb/internal/storage"
	"kvdb/pkg/types"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// NodeState represents the state of a Raft node
type NodeState int

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

// PersistentState represents state that needs to be persisted
type PersistentState struct {
	CurrentTerm uint64 `json:"currentTerm"`
	VotedFor    int    `json:"votedFor"` // Node ID that received vote in current term
}

// VolatileState represents state that doesn't need persistence
type VolatileState struct {
	CommitIndex uint64 // Highest log entry known to be committed
	LastApplied uint64 // Highest log entry applied to state machine
}

// LeaderState represents state spesific to leaders
type LeaderState struct {
	NextIndex  map[int]uint64 // For each node, index of the next log entry to send
	MatchIndex map[int]uint64 // For each node, index of highest log entry known to be replicated
}

// Node represents a Raft consensus node
type Node struct {
	mu sync.RWMutex

	// Configuration
	ID        int
	Peers     []int // Other node IDs in the cluster
	Storage   *storage.Storage
	StateDir  string
	logStore  *LogStore
	transport *Transport

	// Raft state
	State          NodeState
	Persistent     PersistentState
	Volatile       VolatileState
	Leader         LeaderState
	ElectionTimer  *time.Timer
	HeartbeatTimer *time.Timer

	// Communication
	MessageHandler func(msg types.LogEntry) error

	// Timers
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration

	// Log entries (in practice, this would be in storage)
	Log []types.LogEntry
}

// NewNode creates a new Raft node with persistent log
func NewNode(id int, peers []int, storage *storage.Storage, stateDir string) (*Node, error) {
	// Create Raft WAL
	walPath := filepath.Join(stateDir, "raft-wal.log")
	raftWAL, err := storage.NewRaftWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft WAL: %w", err)
	}

	// Create log store
	logStore, err := NewLogStore(stateDir, raftWAL)
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %w", &err)
	}

	node := &Node{
		ID:       id,
		Peers:    peers,
		Storage:  storage,
		StateDir: stateDir,
		logStore: logStore,
		State:    StateFollower,
		Persistent: PersistentState{
			CurrentTerm: 0,
			VotedFor:    -1,
		},
		Volatile: VolatileState{
			CommitIndex: 0,
			LastApplied: 0,
		},
		ElectionTimeout:  time.Duration(150+rand.Intn(150)) * time.Millisecond,
		HeartbeatTimeout: 50 * time.Millisecond,
	}

	// Initialize leader state maps
	node.Leader.NextIndex = make(map[int]uint64)
	node.Leader.MatchIndex = make(map[int]uint64)

	for _, peer := range peers {
		node.Leader.NextIndex[peer] = node.logStore.LastIndex() + 1
		node.Leader.MatchIndex[peer] = 0
	}

	// Load persistent state if it exists
	node.loadState()

	// Start election timer
	node.resetElectionTimer()

	return node, nil
}

// loadState loads persistent state from disk
func (n *Node) loadState() {
	stateFile := filepath.Join(n.StateDir, "raft-state.json")
	data, err := os.ReadFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return // No existing state, use defaults
		}
		fmt.Printf("Error reading state file: %v\n", err)
		return
	}

	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		fmt.Printf("Error unmarshalling state: %v\n", err)
		return
	}

	n.Persistent = state
}

// saveState saves persistent state to disk
func (n *Node) saveState() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	stateFile := filepath.Join(n.StateDir, "raft-state.json")
	data, err := json.Marshal(n.Persistent)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := os.WriteFile(stateFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	return nil
}

// becomeFollower transitions the node to follower state
func (n *Node) becomeFollower(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.State = StateFollower
	n.Persistent.CurrentTerm = term
	n.Persistent.VotedFor = -1

	// Reset election timer
	n.resetElectionTimer()

	// Save state
	go n.saveState()

	fmt.Printf("Node %d became follower in term %d\n", n.ID, term)
}

// becomeCandidate transitions the node to candidate state
func (n *Node) becomeCandidate() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.State = StateCandidate
	n.Persistent.CurrentTerm++
	n.Persistent.VotedFor = n.ID

	// Reset election timer
	n.resetElectionTimer()

	// Save state
	go n.saveState()

	fmt.Printf("Node %d became candidate in term %d\n", n.ID, n.Persistent.CurrentTerm)
}

// becomeLeader transitions the node to leader state
func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.State = StateLeader

	// Initialize leader state
	for _, peer := range n.Peers {
		lastIndex := n.getLastLogIndex()
		n.Leader.NextIndex[peer] = lastIndex + 1
		n.Leader.MatchIndex[peer] = 0
	}

	// Start heartbeat timer
	n.resetHearthbeatTimer()

	fmt.Printf("Node %d became leader in term %d\n", n.ID, n.Persistent.CurrentTerm)
}

// resetElectionTimer resets the election timeout timer
func (n *Node) resetElectionTimer() {
	if n.ElectionTimer != nil {
		n.ElectionTimer.Stop()
	}

	// Add some randomness to timeout to avoid split votes
	timeout := n.ElectionTimeout + time.Duration(rand.Int63n(int64(n.ElectionTimeout)))
	n.ElectionTimer = time.AfterFunc(timeout, n.startElection)
}

// resetHearthbeatTimer resets the heartbeat timer
func (n *Node) resetHearthbeatTimer() {
	if n.HeartbeatTimer != nil {
		n.HeartbeatTimer.Stop()
	}

	n.HeartbeatTimer = time.AfterFunc(n.HeartbeatTimeout, n.sendHeartbeats)
}

// getLastLogIndex returns the index of the last log entry
func (n *Node) getLastLogIndex() uint64 {
	return n.logStore.LastIndex()

}

// getLastLogTerm returns the term of the last log entry
func (n *Node) getLastLogTerm() uint64 {
	return n.logStore.LastTerm()
}

// appendEntry adds a new log entry
func (n *Node) appendEntry(entry types.LogEntry) error {
	return n.logStore.Append(entry)
}

// getLogEntry returns a log entry by index
func (n *Node) getLogEntry(index uint64) (types.LogEntry, bool) {
	return n.logStore.Get(index)
}

// GetState returns the current node state
func (n *Node) GetState() (NodeState, uint64) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.State, n.Persistent.CurrentTerm
}

// GetNodeID returns the node ID
func (n *Node) GetNodeID() int {
	return n.ID
}

// Stop stops all timers and cleans up
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.ElectionTimer != nil {
		n.ElectionTimer.Stop()
	}

	if n.HeartbeatTimer != nil {
		n.HeartbeatTimer.Stop()
	}
}

// Close cleanly shuts down the node
func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.ElectionTimer != nil {
		n.ElectionTimer.Stop()
	}

	if n.HeartbeatTimer != nil {
		n.HeartbeatTimer.Stop()
	}

	return n.logStore.Close()
}

// SubmitCommand submits a new command to the Raft log
func (n *Node) SubmitCommand(command types.Command) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.State != StateLeader {
		return fmt.Errorf("not the leader")
	}

	// Create log entry
	entry := types.LogEntry{
		Index:     n.getLastLogIndex() + 1,
		Term:      n.Persistent.CurrentTerm,
		Command:   command,
		Timestamp: time.Now(),
	}

	// Append to local log
	if err := n.appendEntry(entry); err != nil {
		return fmt.Errorf("failed to append entry: %w", err)
	}

	// Replicate to followers
	for _, peer := range n.Peers {
		go n.sendAppendEntries(peer, false)
	}

	return nil
}
