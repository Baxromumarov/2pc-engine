# 2PC Engine - Distributed Two-Phase Commit System in Go

A networked distributed transaction system in Go that uses HTTP-based communication between nodes, implementing the Two-Phase Commit (2PC) protocol with master election and health monitoring.

## Features

- **Two-Phase Commit (2PC)**: Distributed transaction coordination
- **Master Election**: Deterministic election based on lexicographical address ordering
- **Health Monitoring**: Heartbeat-based node health checks
- **HTTP Communication**: RESTful API for node communication
- **CLI Tool**: Command-line interface for cluster management

## Project Structure

```
/cmd
    /master      - Master node entry point
    /node        - Slave node entry point
    /cli         - CLI tool for cluster management

/pkg
    /cluster     - Cluster management, election, and heartbeat
    /node        - Node implementation
    /transport   - HTTP server and client
    /protocol    - Message types and state constants
    /two_phase_commit - 2PC coordinator and participant
```

## Quick Start

Use the CLI helpers so the correct flags are passed.

```bash
# terminal 1 (master candidate + coordinator)
go run ./cmd/cli start-master --addr=localhost:8080 --nodes=localhost:8080,localhost:8081,localhost:8082

# terminal 2 (peer)
go run ./cmd/cli start-node --addr=localhost:8081 --nodes=localhost:8080,localhost:8081,localhost:8082

# terminal 3 (peer)
go run ./cmd/cli start-node --addr=localhost:8082 --nodes=localhost:8080,localhost:8081,localhost:8082
```

Then issue a transaction:

```bash
go run ./cmd/cli commit --master=localhost:8080 --payload='{"key": "value"}'
```

## CLI Commands

### Start a Node
```bash
go run ./cmd/cli start-node --addr=localhost:8081 --nodes=localhost:8080,localhost:8081,localhost:8082
```

### Check Node Health
```bash
go run ./cmd/cli health --addr=localhost:8081
```

### Check Cluster Status
```bash
go run ./cmd/cli status --nodes=localhost:8080,localhost:8081,localhost:8082
```

### Execute a Transaction
```bash
go run ./cmd/cli commit --master=localhost:8080 --payload='{"data": "example"}'
```

## HTTP API

Each node exposes the following endpoints:

### Health Check
```
GET /health
→ 200 {"status": "OK", "address": "...", "role": "MASTER|SLAVE"}
```

### Get Role
```
GET /role
→ 200 {"role": "MASTER|SLAVE", "address": "..."}
```

### Prepare (2PC Phase 1)
```
POST /prepare
Body: {"transaction_id": "...", "payload": {...}}
→ 200 {"status": "READY"}
→ 500 {"status": "ABORT", "error": "..."}
```

### Commit (2PC Phase 2)
```
POST /commit
Body: {"transaction_id": "..."}
→ 200 {"success": true}
```

### Abort
```
POST /abort
Body: {"transaction_id": "..."}
→ 200 {"success": true}
```

### Start Transaction (Master only)
```
POST /transaction
Body: {"payload": {...}}
→ 200 {"transaction_id": "...", "success": true, "message": "..."}
```

## 2PC Protocol Flow

1. **Prepare Phase**: Master sends `/prepare` to all alive slave nodes
2. **Vote Collection**: If all nodes return `READY`, proceed to commit
3. **Commit Phase**: Master sends `/commit` to all prepared nodes
4. **Abort Handling**: If any node returns `ABORT` or times out, master sends `/abort` to all prepared nodes

## Master Election

The system uses a deterministic election algorithm:

1. All alive nodes are sorted by address (lexicographically)
2. The node with the lowest address becomes master
3. Election triggers on:
   - System startup
   - Master failure detection
   - Node join/leave events
   - Any change in the lowest-alive node (role updates are propagated)

## Configuration

### Master Options
- `--addr`: Address to bind (default: `localhost:8080`)
- `--nodes`: Comma-separated list of all node addresses (include self so election can converge)
- `--heartbeat`: Heartbeat interval (default: `5s`)
- `--coord-timeout`: 2PC coordinator timeout (default: `10s`)

### Node Options
- `--addr`: Address to bind (default: `localhost:8081`)
- `--nodes`: Comma-separated list of all node addresses (include master and peers)
- `--heartbeat`: Heartbeat interval (default: `5s`)
- `--coord-timeout`: 2PC coordinator timeout used if this node is elected master (default: `10s`)

## Running Tests

```bash
go test ./... -v
```

## Architecture

```
┌─────────────┐     HTTP      ┌─────────────┐
│   Master    │◄─────────────►│   Node 1    │
│ (Coordinator)│              │ (Participant)│
└──────┬──────┘               └─────────────┘
       │
       │ HTTP                 ┌─────────────┐
       └─────────────────────►│   Node 2    │
                              │ (Participant)│
                              └─────────────┘
```

## License

MIT

Note: Description is written in AI
