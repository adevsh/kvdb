// Package raft implements the Raft consensus algorithm
// transport.go: Handles network communication for Raft nodes

package raft

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/adevsh/kvdb/internal/network"
	"github.com/adevsh/kvdb/pkg/types"
)

// Transport handles network communication for Raft
type Transport struct {
	node   *Node
	server *network.Server
	peers  map[int]string // nodeID -> address
}

// NewTransport creates a new Raft transport layer
func NewTransport(node *Node, raftAddr string, peers map[int]string) *Transport {
	transport := &Transport{
		node:  node,
		peers: peers,
	}

	// Create network server
	transport.server = network.NewServer(raftAddr, transport)
	return transport
}

// Start begins listening for incoming message
func (t *Transport) Start() error {
	return t.server.Start()
}

// HandleMessage processes incoming messages
func (t *Transport) HandleMessage(msg network.Message) ([]byte, error) {
	switch msg.Type {
	case network.MessageTypeRequestVote:
		return t.handleRequestVote(msg)
	case network.MessageTypeRequestVoteResponse:
		return t.handleRequestVoteResponse(msg)
	case network.MessageTypeAppendEntries:
		return t.handleAppendEntries(msg)
	case network.MessageTypeAppendEntriesResponse:
		return t.handleAppendEntriesResponse(msg)
	case network.MessageTypeCommand:
		return t.handleCommand(msg)
	default:
		return nil, fmt.Errorf("unknown message type: %d", msg.Type)
	}
}

// handleRequestVote processes vote request
func (t *Transport) handleRequestVote(msg network.Message) ([]byte, error) {
	var data network.RequestVoteData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal request vote data: %w", err)
	}

	t.node.mu.Lock()
	defer t.node.mu.Unlock()

	// Check if we should grant vote
	grantVote := false
	if msg.Term > t.node.Persistent.CurrentTerm {
		// Update term and become follower
		t.node.becomeFollower(msg.Term)
		grantVote = true
	} else if msg.Term == t.node.Persistent.CurrentTerm && (t.node.Persistent.VotedFor == -1 || t.node.Persistent.VotedFor == msg.From) {
		// Check log completness
		if data.LastLogTerm > t.node.getLastLogTerm() ||
			(data.LastLogTerm == t.node.getLastLogTerm() && data.LastLogIndex >= t.node.getLastLogIndex()) {
			grantVote = true
			t.node.Persistent.VotedFor = msg.From
			t.node.saveState()
		}
	}

	response := network.ResponseData{
		Success: grantVote,
		Message: "vote response",
	}

	responseData, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	return responseData, nil
}

// handleRequestVoteResponse processes vote responses
func (t *Transport) handleRequestVoteResponse(msg network.Message) ([]byte, error) {
	// This would be handled in the election login
	// For now, just acknowledge receipt
	return json.Marshal(network.ResponseData{Success: true})
}

// handleAppendEntries processes log replication message
func (t *Transport) handleAppendEntries(msg network.Message) ([]byte, error) {
	var data network.AppendEntriesData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal append entries data: %w", err)
	}

	t.node.mu.Lock()
	defer t.node.mu.Unlock()

	success := false

	// Check term
	if msg.Term < t.node.Persistent.CurrentTerm {
		// Stale leader, reject
	} else if msg.Term > t.node.Persistent.CurrentTerm {
		t.node.becomeFollower(msg.Term)
		success = true
	} else {
		// Same term, process normally
		if t.node.State != StateFollower {
			t.node.becomeFollower(msg.Term)
		}
		t.node.resetElectionTimer()
		success = true
	}

	// TODO: Implement actual log matching and entry application

	response := network.ResponseData{
		Success: success,
		Message: "append entries response",
	}

	responseData, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	return responseData, nil
}

// handleAppendEntriesResponse processes append entries responses
func (t *Transport) handleAppendEntriesResponse(msg network.Message) ([]byte, error) {
	// This would update leader state based on follower response
	// For now, just acknowledge receipt
	return json.Marshal(network.ResponseData{Success: true})
}

// handleCommand processes database command messages
func (t *Transport) handleCommand(msg network.Message) ([]byte, error) {
	var data network.CommandData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal command data: %w", err)
	}

	// Create log entry and submit to Raft
	command := types.Command{
		Op:    data.Operation,
		Key:   data.Key,
		Value: data.Value,
	}

	entry := types.LogEntry{
		Index:     t.node.getLastLogIndex() + 1,
		Term:      t.node.Persistent.CurrentTerm,
		Command:   command,
		Timestamp: time.Now(),
	}

	if err := t.node.SubmitCommand(command); err != nil {
		response := network.ResponseData{
			Success: false,
			Message: err.Error(),
		}
		return json.Marshal(response)
	}

	response := network.ResponseData{
		Success: true,
		Message: "command submitted",
		Data:    []byte(fmt.Sprintf(`{"index":%d}`, entry.Index)),
	}

	return json.Marshal(response)
}

// SendRequestVote sends a vote request to a peer
func (t *Transport) SendRequestVote(peerID int, term, lastLogIndex, lastLogTerm uint64) (bool, error) {
	msg, err := network.CreateRequestVote(t.node.ID, peerID, term, lastLogIndex, lastLogTerm)
	if err != nil {
		return false, err
	}

	response, err := t.server.SendMessage(peerID, msg)
	if err != nil {
		return false, err
	}

	var responseData network.ResponseData
	if err := json.Unmarshal(response, &responseData); err != nil {
		return false, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return responseData.Success, nil
}

// SendAppendEntries sends log entries to a follower
func (t *Transport) SendAppendEntries(peerID int, term uint64, prevLogIndex, prevLogTerm uint64,
	entries []interface{}, leaderCommit uint64) (bool, error) {
	msg, err := network.CreateAppendEntries(t.node.ID, peerID, term, prevLogIndex, prevLogTerm, entries, uint64(leaderCommit))

	if err != nil {
		return false, err
	}

	response, err := t.server.SendMessage(peerID, msg)
	if err != nil {
		return false, err
	}

	var responseData network.ResponseData
	if err := json.Unmarshal(response, &responseData); err != nil {
		return false, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return responseData.Success, nil
}

// BroadcaseRequestVote sends vote requests to all peers
func (t *Transport) BroadcaseRequestVote(term, lastLogIndex, lastLogTerm uint64) map[int]error {
	errors := make(map[int]error)
	for peerID := range t.peers {
		if peerID == t.node.ID {
			continue
		}

		if _, err := t.SendRequestVote(peerID, term, lastLogIndex, lastLogTerm); err != nil {
			errors[peerID] = err
		}
	}

	return errors
}

// ConnectToPeers establishes connections to all peer nodes
func (t *Transport) ConnectToPeers() error {
	for peerID, addr := range t.peers {
		if peerID == t.node.ID {
			continue
		}

		if err := t.server.ConnectToNode(peerID, addr); err != nil {
			return fmt.Errorf("failed to connect to node %d: %w", peerID, err)
		}
	}

	return nil
}

// Stop shuts down the transport layer
func (t *Transport) Stop() {
	t.server.Stop()
}

// GetNodeID returns the node ID for network interface
func (t *Transport) GetNodeID() int {
	return t.node.ID
}
