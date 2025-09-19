// Package network handles TCP-based communication for the KVDB cluster
// protocol.go: Defines the network protocol and message format

package network

import (
	"encoding/json"
	"fmt"
)

// MessageType represents the type of the network message
type MessageType int

const (
	MessageTypeRequestVote MessageType = iota
	MessageTypeRequestVoteResponse
	MessageTypeAppendEntries
	MessageTypeAppendEntriesResponse
	MessageTypeCommand
	MessageTypeCommandResponse
	MessageTypeSnapshot
	MessageTypeSnapshotResponse
)

// Message represents a network message between nodes
type Message struct {
	Type      MessageType `json:"type"`
	From      int         `json:"from"`      // Sender node ID
	To        int         `json:"to"`        // Receiver node ID
	Term      uint64      `json:"term"`      // Current term
	Data      []byte      `json:"data"`      // Message paylod
	Timestamp int64       `json:"timestamp"` // Message creation time
}

// RequestVoteData contains data for vote requests
type RequestVoteData struct {
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
}

// AppendEntriesData containts data for log replication
type AppendEntriesData struct {
	PrevLogIndex uint64        `json:"prevLogIndex"`
	PrevLogTerm  uint64        `json:"prevLogTerm"`
	Entries      []interface{} `json:"entries"`
	LeaderCommit uint64        `json:"leaderCommit"`
}

// CommandData containts database operation data
type CommandData struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// ResponseData containts response information
type ResponseData struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    []byte `json:"data,omitempty"`
}

// EncodeMessage serializes a message to JSON
func EncodeMessage(msg Message) ([]byte, error) {
	return json.Marshal(msg)
}

// DecodeMessage deserializes a message from JSON
func DecodeMessage(data []byte) (Message, error) {
	var msg Message
	err := json.Unmarshal(data, &msg)
	return msg, err
}

// CreateRequestVote creates a vote request message
func CreateRequestVote(from, to int, term, lastLogIndex, lastLogTerm uint64) (Message, error) {
	data := RequestVoteData{
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return Message{}, fmt.Errorf("failed to marshal request vote data: %w", err)
	}

	return Message{
		Type: MessageTypeRequestVote,
		From: from,
		To:   to,
		Term: term,
		Data: jsonData,
	}, nil

}

// CreateAppendEntries creates a log replication message
func CreateAppendEntries(from, to int, term, lastLogIndex, prevLogTerm uint64,
	entries []interface{}, leaderCommit uint64) (Message, error) {
	data := AppendEntriesData{
		PrevLogIndex: lastLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return Message{}, fmt.Errorf("failed to marshal append entries data: %w", err)
	}

	return Message{
		Type: MessageTypeAppendEntries,
		From: from,
		To:   to,
		Term: term,
		Data: jsonData,
	}, nil

}

// CreateCommand creates a database command message
func CreateCommand(from, to int, operation, key, value string) Message {
	data := CommandData{
		Operation: operation,
		Key:       key,
		Value:     value,
	}

	jsonData, _ := json.Marshal(data) // Simple data, error unlikely

	return Message{
		Type: MessageTypeCommand,
		From: from,
		To:   to,
		Data: jsonData,
	}

}
