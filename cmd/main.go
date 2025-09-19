// Package main is the entry point for the KVDB application
// Purpose: Initialize and start the distributed key-value database

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/adevsh/kvdb/internal/raft"
	"github.com/adevsh/kvdb/internal/repl"
	"github.com/adevsh/kvdb/internal/storage"
)

func main() {
	// Parse command line flags
	var (
		nodeID = flag.Int("id", 1, "Node ID")
		// httpAddr  = flag.String("http", ":8080", "HTTP API address")
		raftAddr = flag.String("raft", ":9090", "Raft protocol address")
		dataDir  = flag.String("data-dir", "./data", "Data directory")
		cluster  = flag.String("cluster", "", "Cluster peers (id:addr,id:addr)")
		// bootstrap = flag.Bool("bootstrap", false, "Bootstrap the cluster")
	)

	flag.Parse()

	// Create data directory
	nodeDataDir := filepath.Join(*dataDir, fmt.Sprint("node%d", *nodeID))
	if err := os.MkdirAll(nodeDataDir, 0755); err != nil {
		fmt.Printf("failed to create data directory: %v\n", err)
		os.Exit(1)
	}

	// Parse cluster configuration
	peers := parseClusterConfig(*cluster, *nodeID)

	// Initialize storage
	storage, err := storage.NewStorage(nodeDataDir)
	if err != nil {
		fmt.Printf("failed to initialize storage: %v\n", err)
		os.Exit(1)
	}
	defer storage.Close()

	fmt.Printf("Storage initiazlied with %d items\n", storage.Count())

	// Initialize Raft node
	raftNode, err := raft.NewNode(*nodeID, getPeerIDs(peers), storage, nodeDataDir)
	if err != nil {
		fmt.Printf("failed to initialize raft node %v\n", err)
	}
	defer raftNode.Stop()

	// Initialize network transport
	transport := raft.NewTransport(raftNode, *raftAddr, peers)
	defer transport.Stop()

	// Connect to peers nodes
	if err := transport.ConnectToPeers(); err != nil {
		fmt.Printf("Warning: failed to connect to some peers: %v\n", err)
	}

	// Start network server
	if err := transport.Start(); err != nil {
		fmt.Printf("Failed to start network server: %v\n", err)
		os.Exit(1)
	}

	// Initialize REPL
	repl := repl.NewREPL(raftNode, storage)

	fmt.Printf("KVDB Node %d started\n", *nodeID)
	fmt.Printf("Data directory: %s\n", nodeDataDir)
	fmt.Printf("Raft address: %s\n", *raftAddr)
	fmt.Printf("Cluster peers: %v\n", getPeerIDs(peers))

	// Start REPL interface
	repl.Start()
}

// parseClusterConfig parses the cluster configuration string
func parseClusterConfig(clusterStr string, currentNodeID int) map[int]string {
	peers := make(map[int]string)

	if clusterStr == "" {
		return peers
	}

	entries := strings.Split(clusterStr, ",")
	for _, entry := range entries {
		parts := strings.Split(entry, ":")
		if len(parts) != 2 {
			fmt.Printf("Invalid cluster entry: %s\n", entry)
			continue
		}

		var nodeID int
		fmt.Scanf(parts[0], "%d", &nodeID)

		// Don't add current node to peers
		if nodeID != currentNodeID {
			peers[nodeID] = parts[1]
		}
	}

	return peers
}

// getPeerIDs returns a list of peer IDs from the peers map
func getPeerIDs(peers map[int]string) []int {
	ids := make([]int, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}

	return ids
}
