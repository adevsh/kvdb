# KVDB - Distributed Key-Value Database

A distributed key-value database built from scratch in Go, featuring Raft consensus, TCP-based communication, and crash recovery mechanisme.

## Current Status

*This is a learning project and not production-ready*

### Implemented Features

#### Core Functionality
- **Basic KV Operations**: PUT, GET, DELETE commands
- **REPL Interface**: Interactive command-line interface
- **In-Memory Storage**: Fast key-value storage with concurrent access

#### Persistence & Recovery
- **Write-Ahead Log (WAL)**: Append-only log for crash recovery
- **Snapshot System**: Periodic state snapshots for log compaction
- **Crash Recovery**: Automatic recovery from WAL and snapshots
- **Data Durability**: Guaranteed persistence of committed operations

#### Distributed Consensus
- **Raft Implementation**: Leader election and log replication
- **TCP Communication**: Custom binary protocol for node communication
- **Cluster Managemenet**: Multi-node coordination and failure handling
- **Leader Election**: Automatic leader failover and recovery

#### Network Layer
- **TCP Server/Client**: Custom protocol with message framing
- **Binary Protocol**: Efficient network communication
- **Connection Management**: Persistent connections between nodes
- **Message Routing**: Reliable message delivery between nodes

