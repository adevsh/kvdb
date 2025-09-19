// Package network handles TCP-based communication for the KVDB cluster
// server.go: TCP server implementation for node communication

package network

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

// Server handles TCP connections for node communication
type Server struct {
	address     string
	listener    net.Listener
	handlers    MessageHandler
	connections map[int]net.Conn
	mu          sync.RWMutex
	running     bool
}

// MessageHandler defines the interface for processing incoming message
type MessageHandler interface {
	HandleMessage(ms Message) ([]byte, error)
	GetNodeID() int
}

// NewServer creates a new TCP server
func NewServer(address string, handler MessageHandler) *Server {
	return &Server{
		address:     address,
		handlers:    handler,
		connections: make(map[int]net.Conn),
		running:     false,
	}
}

// Start begins listening for incoming connections
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	s.listener = listener
	s.running = true

	fmt.Printf("Server listening on %s\n", s.address)

	go s.acceptConnections()
	return nil
}

// acceptConnections handles incoming connections
func (s *Server) acceptConnections() {
	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.running {
				fmt.Printf("Error accepting connection: %v\n", err)
			}
			continue
		}

		go s.handleConnection(conn)
	}
}

// handleConnection processes a single client connection
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		// Set read timeout
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		// Read message length
		lengthBytes := make([]byte, 4)

		if _, err := reader.Read(lengthBytes); err != nil {
			fmt.Printf("Error reading message length: %v\n", err)

			return
		}

		length := int(uint32(lengthBytes[0])<<24 | uint32(lengthBytes[1])<<16 |
			uint32(lengthBytes[2])<<8 | uint32(lengthBytes[3]))

		// Read message data
		data := make([]byte, length)
		if _, err := reader.Read(data); err != nil {
			fmt.Printf("Error reading message data: %v\n", err)
			return
		}

		// Decode message
		msg, err := DecodeMessage(data)
		if err != nil {
			fmt.Printf("Error decoding message: %v\n", err)
			continue
		}

		// Handle message
		response, err := s.handlers.HandleMessage(msg)
		if err != nil {
			fmt.Printf("Error handling message: %v\n", err)
			continue
		}

		// Send response
		if err := s.sendResponse(writer, response); err != nil {
			fmt.Printf("Error sending response: %v\n", err)
			return
		}
	}
}

// sendResponse sends a response back to the client
func (s *Server) sendResponse(writer *bufio.Writer, data []byte) error {
	// Write response length
	length := len(data)
	lenghtBytes := []byte{
		byte(length >> 24),
		byte(length >> 16),
		byte(length >> 8),
		byte(length),
	}

	if _, err := writer.Write(lenghtBytes); err != nil {
		return err
	}

	// Write response data
	if _, err := writer.Write(data); err != nil {
		return err
	}

	return writer.Flush()
}

// ConnectToNode establishes a connection to another node
func (s *Server) ConnectToNode(nodeID int, address string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.connections[nodeID]; exists {
		return nil // Already connected
	}

	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to node %d: %w", nodeID, err)
	}

	s.connections[nodeID] = conn
	fmt.Printf("Connected to node %d at %s\n", nodeID, address)

	return nil
}

// SendMessage sends a message to a spesific node
func (s *Server) SendMessage(nodeID int, msg Message) ([]byte, error) {
	s.mu.RLock()
	conn, exists := s.connections[nodeID]
	defer s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no connection to node %d", nodeID)
	}

	// Encode message
	data, err := EncodeMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to encode message: %w", err)
	}

	// Seng message length
	length := len(data)
	lengthBytes := []byte{
		byte(length >> 24),
		byte(length >> 16),
		byte(length >> 8),
		byte(length),
	}

	if _, err := conn.Write(lengthBytes); err != nil {
		return nil, fmt.Errorf("failed to send message length: %w", err)
	}

	// Send message data
	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("failed to send message data: %w", err)
	}

	// Read response
	reader := bufio.NewReader(conn)

	// Read response length
	responseLengthBytes := make([]byte, 4)
	if _, err := reader.Read(responseLengthBytes); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}

	responseLength := int(uint32(responseLengthBytes[0])<<24 |
		uint32(responseLengthBytes[1])<<16 |
		uint32(responseLengthBytes[2])<<8 |
		uint32(responseLengthBytes[3]))

	// Read response data
	responseData := make([]byte, responseLength)
	if _, err := reader.Read(responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %w", err)
	}

	return responseData, nil
}

// BroadcastMessage send a message to all connected nodes
func (s *Server) BroadcastMessage(msg Message) map[int]error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	errors := make(map[int]error)
	for nodeID := range s.connections {
		if nodeID == s.handlers.GetNodeID() {
			continue // Don't send to self
		}

		if _, err := s.SendMessage(nodeID, msg); err != nil {
			errors[nodeID] = err
		}
	}

	return errors
}

// Stop shuts down the server
func (s *Server) Stop() {
	s.running = false
	if s.listener != nil {
		s.listener.Close()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, conn := range s.connections {
		conn.Close()
	}

	s.connections = make(map[int]net.Conn)
}

// GetConnectedNodes returns list of connected node IDs
func (s *Server) GetConnectedNodes() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodes := make([]int, 0, len(s.connections))
	for nodeID := range s.connections {
		nodes = append(nodes, nodeID)
	}

	return nodes
}
