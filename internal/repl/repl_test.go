package repl

import (
	"kvdb/pkg/types"
)

// MockRaftNode is a mock implementation of raft.Node for testing
type MockRaftNode struct {
	// state         raft.State
	term          int
	clusterStatus map[uint64]string
	submitError   error
}

// func (m *MockRaftNode) GetState() (raft.State, int) {
// 	return m.state, m.term
// }

func (m *MockRaftNode) GetClusterStatus() map[uint64]string {
	return m.clusterStatus
}

func (m *MockRaftNode) SubmitCommand(command types.Command) error {
	return m.submitError
}

// MockStorage is a mock implementation of storage.Storage for testing
type MockStorage struct {
	data      map[string]string
	count     int
	lastIndex int
}

func (m *MockStorage) Get(key string) (string, bool) {
	value, exists := m.data[key]
	return value, exists
}

func (m *MockStorage) Count() int {
	return m.count
}

func (m *MockStorage) LastIndex() int {
	return m.lastIndex
}

// func TestNewREPL(t *testing.T) {
// 	mockRaft := &MockRaftNode{}
// 	mockStorage := &MockStorage{}

// 	repl := NewREPL(mockRaft, mockStorage)

// 	if repl.raftNode != mockRaft {
// 		t.Errorf("Expected raftNode to be %v, got %v", mockRaft, repl.raftNode)
// 	}

// }
