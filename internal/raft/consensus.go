// Package raft implements the Raft consensus algorithm
// consensus.go: Enhanced with real network communication adn commit

package raft

import (
	"fmt"
	"sort"
	"time"

	"github.com/adevsh/kvdb/pkg/types"
)

// voteResult represents the result of a vote request
type voteResult struct {
	peerID  int
	granted bool
	err     error
}

// startElection initiates a leader election with real network calls
func (n *Node) startElection() {
	n.becomeCandidate()

	// Request votes from all peers
	votes := 1 // Vote for self
	voteChan := make(chan voteResult, len(n.Peers))

	for _, peer := range n.Peers {
		go func(peerID int) {
			granted, err := n.transport.SendRequestVote(
				peerID,
				n.Persistent.CurrentTerm,
				n.logStore.LastIndex(),
				n.logStore.LastTerm(),
			)

			voteChan <- voteResult{peerID: peerID, granted: granted, err: err}
		}(peer)
	}

	// Collect votes with timeout
	timeout := time.After(n.ElectionTimeout)
	votesNeeded := len(n.Peers)/2 + 1

	for i := 0; i < len(n.Peers); i++ {
		select {
		case result := <-voteChan:
			if result.err != nil {
				fmt.Printf("Error getting vote from node %d: %v\n", result.peerID, result.err)
				continue
			}

			if result.granted {
				votes++
				fmt.Printf("Received vote from node %d\n", result.peerID)
			} else {
				fmt.Printf("Node %d declined vote \n", result.peerID)
			}

			// Check if we have enough votes
			if votes >= votesNeeded {
				n.mu.Lock()
				if n.State == StateCandidate {
					n.becomeLeader()
				}
				n.mu.Unlock()
				return
			}
		case <-timeout:
			fmt.Println("Election timeout reached")
			break
		}
	}

	// Election failed, become follower
	n.mu.Lock()
	n.becomeFollower(n.Persistent.CurrentTerm)
	n.mu.Unlock()
}

// // requestVote requests a vote from a peer (simplified)
// func (n *Node) requestVote(peerID int) bool {
// 	// In real implementation, this would send a network message
// 	// For now, simulate with random responses
// 	time.Sleep(10 * time.Millisecond) // Simulate network latency

// 	// 70% chance for granting vote for simulation
// 	return rand.Float32() < 0.7
// }

// sendHeartbeats sends real heartbeat message to all followers
func (n *Node) sendHeartbeats() {
	n.mu.RLock()
	if n.State != StateLeader {
		n.mu.RUnlock()
		return
	}
	currentTerm := n.Persistent.CurrentTerm
	n.mu.RUnlock()

	for _, peer := range n.Peers {
		go func(peerID int) {
			// Send empty AppendEntries as heartbeat
			_, err := n.transport.SendAppendEntries(
				peerID,
				currentTerm,
				n.Leader.NextIndex[peerID]-1,
				0,
				[]interface{}{},
				n.Volatile.CommitIndex)

			if err != nil {
				fmt.Printf("Error sending heartbeat to node %d: %v\n", peerID, err)
				// Handle network issue, maybe mark node as unreachable
			}
		}(peer)
	}

	n.resetHearthbeatTimer()
}

// sendAppendEntries sends log entries to a follower with real network calls
func (n *Node) sendAppendEntries(peerID int, heartbeat bool) {
	n.mu.RLock()
	if n.State != StateLeader {
		n.mu.RUnlock()
		return
	}

	nextIndex := n.Leader.NextIndex[peerID]
	prevLogIndex := nextIndex - 1
	prevLogTerm, _ := n.logStore.Term(prevLogIndex)
	currentTerm := n.Persistent.CurrentTerm
	n.mu.RUnlock()

	var entriesInterface []interface{}

	if !heartbeat {
		// Get entries to send
		entries, _ := n.logStore.GetRange(nextIndex, n.logStore.LastIndex())

		// Convert to []interface{} for JSON marshalling
		entriesInterface = make([]interface{}, len(entries))
		for i, entry := range entries {
			entriesInterface[i] = entry
		}
	}

	// Send AppendEntries RPC
	success, err := n.transport.SendAppendEntries(peerID, currentTerm, prevLogIndex,
		prevLogTerm, entriesInterface, n.Volatile.CommitIndex)

	n.mu.Lock()
	defer n.mu.Unlock()

	if err != nil {
		fmt.Printf("Error sending entries to node %d: %v\n", peerID, err)

		// Decrement nextIndex and retry later
		if n.Leader.NextIndex[peerID] > 1 {
			n.Leader.NextIndex[peerID]--
		}

		return
	}

	if success {
		// Update match index and next index
		n.Leader.MatchIndex[peerID] = n.logStore.LastIndex()
		n.Leader.NextIndex[peerID] = n.logStore.LastIndex() + 1

		// Advance commit index
		n.advanceCommitIndex()
	} else {
		// Follower rejected entries, decrement nextIndex
		if n.Leader.NextIndex[peerID] > 1 {
			n.Leader.NextIndex[peerID]--
		}
	}
}

// advanceCommitIndex advances the commit index based on matchIndex
func (n *Node) advanceCommitIndex() {
	if n.State != StateLeader {
		return
	}

	// Create a sorted list of match indices
	matchIndices := make([]uint64, 0, len(n.Leader.MatchIndex))
	for _, index := range n.Leader.MatchIndex {
		matchIndices = append(matchIndices, index)
	}

	// Add leader's own last index
	matchIndices = append(matchIndices, n.logStore.LastIndex())

	// Sort in descending order
	sort.Slice(matchIndices, func(i, j int) bool {
		return matchIndices[i] > matchIndices[j]
	})

	// Find the median (majority)
	majorityIndex := matchIndices[len(matchIndices)/2]

	// Only advance commit index for current term
	if majorityIndex > n.Volatile.CommitIndex {
		if term, exists := n.logStore.Term(majorityIndex); exists && term == n.Persistent.CurrentTerm {
			n.Volatile.CommitIndex = majorityIndex
			fmt.Printf("Advanced commit index to %d\n", majorityIndex)

			// Apply commited entries
			go n.applyCommittedEntries()
		}
	}
}

// applyCommittedEntries applies committed entries to state machine
func (n *Node) applyCommittedEntries() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.Volatile.LastApplied < n.Volatile.CommitIndex {
		n.Volatile.LastApplied++
		entry, exists := n.logStore.Get(n.Volatile.LastApplied)
		if !exists {
			fmt.Printf("Error: log entry %d not found \n", n.Volatile.LastApplied)
			continue
		}

		if n.MessageHandler != nil {
			if err := n.MessageHandler(entry); err != nil {
				fmt.Printf("Error applying log entry %d: %v\n", n.Volatile.LastApplied, err)
			} else {
				fmt.Printf("Applied entry %d: %s %s\n", n.Volatile.LastApplied, entry.Command.Op, entry.Command.Key)
			}
		}
	}
}

// HandleMessage processes incoming Raft messages
func (n *Node) HandleMessage(msg types.LogEntry) error {
	switch msg.Command.Op {
	case "PUT", "DEL":
		return n.SubmitCommand(msg.Command)
	default:
		return fmt.Errorf("unknown operation: %s", msg.Command.Op)
	}
}

// GetClusterStatus returns the current cluster status
func (n *Node) GetClusterStatus() map[int]string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	status := make(map[int]string)
	for _, peer := range n.Peers {
		status[peer] = "follower" // Simplified
	}
	status[n.ID] = n.State.String()

	return status
}

// String returns representation of NodeState
func (s NodeState) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return "unknown"
	}
}
