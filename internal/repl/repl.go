// Package repl provides the Read-Eval-Print Loop interface
// Purpose: Interactive command-line interface for database operations

package repl

import (
	"bufio"
	"fmt"
	"kvdb/internal/raft"
	"kvdb/internal/storage"
	"kvdb/pkg/types"
	"os"
	"strings"
)

// REPL handles the interactive command interface
type REPL struct {
	raftNode *raft.Node
	storage  *storage.Storage
	reader   *bufio.Reader
}

// NewREPL creates a new REPL instance
func NewREPL(raftNode *raft.Node, storage *storage.Storage) *REPL {
	return &REPL{
		raftNode: raftNode,
		storage:  storage,
		reader:   bufio.NewReader(os.Stdin),
	}
}

// Start begins the REPL interaction
func (r *REPL) Start() {
	fmt.Println("KVDB Distributed Key-Value Store")
	fmt.Println("Type 'help' for available commands")
	fmt.Println("Type 'exit' to quit")

	for {
		r.displayPrompt()
		input, err := r.reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input: %v\n", err)
			continue
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		if input == "exit" || input == "quit" || input == "q" {
			fmt.Println("Goodbye!")
			break
		}

		r.processCommand(input)
	}

}

// displayPrompt shows the appropriate command prompt
func (r *REPL) displayPrompt() {
	state, term := r.raftNode.GetState()
	fmt.Printf("kvdb [%s|term:%d]> ", state.String(), term)
}

// processCommand handles user input commands
func (r *REPL) processCommand(input string) {
	parts := strings.SplitN(input, " ", 3)
	command := strings.ToUpper(parts[0])

	switch command {
	case "PUT":
		r.handlePut(parts)
	case "GET":
		r.handleGet(parts)
	case "DEL":
		r.handleDelete(parts)
	case "COUNT":
		r.handleCount()
	case "CLUSTER":
		r.handleCluster(parts)
	case "INFO":
		r.handleInfo()
	case "SNAPSHOT":
		r.handleSnapshot()
	// case "HELP":
	// 	r.handleHelp()
	case "CLEAR":
		fmt.Print("\033[H\033[2J") // Clear screen
	default:
		fmt.Println("Unknown command: %s\n", command)
		fmt.Println("Type 'help' for available commands")
	}
}

// handlePut processes PUT commands
func (r *REPL) handlePut(parts []string) {
	if len(parts) < 3 {
		fmt.Println("Error: PUT requires key and values")
		fmt.Println("Usage: PUT <key> <value>")
		return
	}

	command := types.Command{
		Op:    "PUT",
		Key:   parts[1],
		Value: parts[2],
	}

	if err := r.raftNode.SubmitCommand(command); err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("OK")
	}
}

// handleGet processes GET commands
func (r *REPL) handleGet(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Error: GET requires a key")
		fmt.Println("Usage: GET <key>")
		return
	}

	value, exists := r.storage.Get(parts[1])
	if !exists {
		fmt.Println("Error: Key not found")
	} else {
		fmt.Printf("\"%s\"\n", value)
	}
}

// handleDelete processes DEL commands
func (r *REPL) handleDelete(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Error: DEL requires a key")
		fmt.Println("Usage: DEL <key>")
		return
	}

	command := types.Command{
		Op:  "DEL",
		Key: parts[1],
	}

	if err := r.raftNode.SubmitCommand(command); err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("OK")
	}
}

// handleCount shows the number of items
func (r *REPL) handleCount() {
	count := r.storage.Count()
	fmt.Printf("%d items\n", count)
}

// handleCluster handles cluster management commands
func (r *REPL) handleCluster(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Error: CLUSTER requires a subcommand")
		fmt.Println("Usage: CLUSTER [status|nodes]")
		return
	}

	subcommand := strings.ToUpper(parts[1])
	switch subcommand {
	case "STATUS":
		status := r.raftNode.GetClusterStatus()
		for nodeID, state := range status {
			fmt.Printf("Node %d: %s\n", nodeID, state)
		}
	case "NODES":
		// Show connected nodes
		fmt.Println("Cluster nodes functionality not implemented yet")
	default:
		fmt.Printf("Error: Unknown subcommand %s\n", subcommand)
		fmt.Println("Usage: CLUSTER [status|nodes]")
	}
}

// handleInfo shows system information
func (r *REPL) handleInfo() {
	state, term := r.raftNode.GetState()
	count := r.storage.Count()
	lastIndex := r.storage.LastIndex()

	fmt.Println("=== System Information ===")
	fmt.Printf("Node State: %s\n", state.String())
	fmt.Printf("Current Term: %d\n", term)
	fmt.Printf("Storage Items: %d\n", count)
	fmt.Printf("Last Applied Index: %d\n", lastIndex)
	fmt.Println("==========================")
}

// handleSnapshot manages snapshot operations
func (r *REPL) handleSnapshot() {
	fmt.Println("Snapshot functionality not implemented yet")
}

// showHelp displays available commands
func (r *REPL) showHelp() {
	fmt.Println("Available commands:")
	fmt.Println("	PUT <key> <value>	- Store a key-value pair")
	fmt.Println("	GET <key>			- Retrieve a value by key")
	fmt.Println("	DEL <key>			- Delete a key-value pair")
	fmt.Println("	COUNT				- Show number of items in database")
	fmt.Println("	CLUSTER STATUS		- Show cluster status")
	fmt.Println("	CLUSTER NODES		- Show cluster nodes")
	fmt.Println("	INFO				- Show system information")
	fmt.Println("	SNAPSHOT			- Manage snapshots")
	fmt.Println("	CLEAR				- Clear the screen")
	fmt.Println("	HELP				- Show this help message")
	fmt.Println("	EXIT				- Exit the program")
	fmt.Println()
	fmt.Println("Note: All write operations are replicated via Raft consensus.")
}
