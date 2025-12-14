# 2PC Engine - Distributed Two-Phase Commit System in Go

A networked distributed transaction system in Go that uses HTTP-based communication between nodes, implementing the Two-Phase Commit (2PC) protocol with master election and health monitoring.

## Features

- **Two-Phase Commit (2PC)**: Distributed transaction coordination with atomic commits
- **Master Participation**: Master node also participates in transactions (not just coordination)
- **Master Election**: Deterministic election based on lexicographical address ordering
- **Health Monitoring**: Heartbeat-based node health checks
- **Dynamic Node Management**: Add/remove nodes at runtime via API
- **HTTP Communication**: RESTful API for node communication
- **CLI Tool**: Command-line interface for cluster management
- **Resilience**: Optional HTTP retries/backoff for transport and heartbeats; clearer commit/abort error reporting

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
POSTGRES_DSN="postgres://user:pass@localhost:5432/2pc?sslmode=disable" \
go run ./cmd/cli start-master --addr=localhost:8080 --nodes=localhost:8080,localhost:8081,localhost:8082

# terminal 2 (peer)
POSTGRES_DSN="postgres://user:pass@localhost:5432/2pc?sslmode=disable" \
go run ./cmd/cli start-node --addr=localhost:8081 --nodes=localhost:8080,localhost:8081,localhost:8082

# terminal 3 (peer)
POSTGRES_DSN="postgres://user:pass@localhost:5432/2pc?sslmode=disable" \
go run ./cmd/cli start-node --addr=localhost:8082 --nodes=localhost:8080,localhost:8081,localhost:8082
```

Then issue a transaction:

```bash
go run ./cmd/cli commit --master=localhost:8080 --payload='{"table":"users","operation":"insert","values":{"id":1,"name":"Alice","email":"a@example.com"}}'
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
go run ./cmd/cli commit --master=localhost:8080 --payload='{"table":"users","operation":"update","values":{"name":"Alice"},"where":{"id":1}}'
```

## Reliability Notes

- **Retries**: HTTP client and heartbeat use a small retry/backoff for transient 5xx/transport failures. Enable/adjust via `transport.NewHTTPClient(timeout).WithRetry(maxRetries, retryDelay)`.
- **Error surfaces**: Coordinator aggregates commit/abort failures per node in responses/logs to aid debugging.
- **Crash recovery**: 2PC is sensitive to coordinator crashes; add a durable WAL/persistent log before production use.
- **Security**: Endpoints are unauthenticated in this demo. Add TLS and auth (API keys/mTLS) for real deployments.

## Dynamic Payload (Postgres)

Payloads are executed as parameterized SQL against Postgres during the prepare phase (inside an open DB transaction), then committed/aborted atomically across nodes.

Supported shape:
```json
{
  "table": "users",                  // required
  "operation": "insert" | "update",  // default insert (case-insensitive)
  "values": { "col": "val", ... },   // required
  "where":  { "col": "val", ... }    // required for update
}
```

Examples:
- Insert (operation defaults to insert):
  `{"table":"users","values":{"id":1,"name":"Alice","email":"a@example.com"}}`
- Update:
  `{"table":"users","operation":"update","values":{"name":"Alice"},"where":{"id":1}}`

Safety notes:
- Identifiers are strictly validated (alphanumeric, `_`, `-`); queries are parameterized.
- A metadata row is also recorded in `distributed_tx` with payload/status for auditing.

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

### Cluster Management

#### Get Cluster Nodes
```
GET /cluster/nodes
→ 200 {"master_addr": "...", "nodes": [{"address": "...", "role": "MASTER|SLAVE", "alive": true}]}
```

#### Join Cluster (New Node Registration)
```
POST /cluster/join
Body: {"address": "new-node:8080"}
→ 200 {"success": true, "master_addr": "...", "cluster_nodes": ["..."]}
```

#### Add Node to Cluster
```
POST /cluster/add
Body: {"address": "new-node:8080"}
→ 200 {"success": true}
```

## Dynamic Node Management

### Adding a New Node in Production

**Step 1:** Provision a new database for the node
```sql
CREATE DATABASE shard3;
CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255), balance DECIMAL(10,2));
```

**Step 2:** Start the new node with its database connection
```bash
POSTGRES_DSN="postgres://user:pass@db:5432/shard3?sslmode=disable" \
  ./node --addr=new-node:8080 --nodes=master:8080
```

**Step 3:** Register the node with the cluster
```bash
curl -X POST http://master:8080/cluster/join \
  -H "Content-Type: application/json" \
  -d '{"address":"new-node:8080"}'
```

**Step 4:** Verify the node was added
```bash
curl http://master:8080/cluster/nodes
```

### Architecture with Multiple Shards

```
┌─────────────────────────────────────────────────────────────┐
│                       2PC Cluster                           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Master (localhost:8080)  ──────► master_db                 │
│       │                                                     │
│       ├── Slave1 (localhost:8081) ──► shard1                │
│       ├── Slave2 (localhost:8082) ──► shard2                │
│       └── Slave3 (localhost:8083) ──► shard3 (dynamically   │
│                                              added)         │
└─────────────────────────────────────────────────────────────┘
```

## 2PC Protocol Flow

1. **Prepare Phase**: Master sends `/prepare` to all alive slave nodes AND prepares locally
2. **Vote Collection**: If all nodes (including master) return `READY`, proceed to commit
3. **Commit Phase**: Master commits locally AND sends `/commit` to all prepared nodes
4. **Abort Handling**: If any node returns `ABORT` or times out, master aborts locally AND sends `/abort` to all prepared nodes

**Note**: The master node participates in the transaction alongside slave nodes, ensuring data is written to all databases atomically.

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
- `--dsn`: Postgres DSN (optional if `POSTGRES_DSN` env var is set)

## Testing

Run all tests:
```bash
go test ./...
```

See coverage (per improvement plan for coordinator flow):
```bash
go test -coverpkg=./... ./pkg/two_phase_commit
```

### Node Options
- `--addr`: Address to bind (default: `localhost:8081`)
- `--nodes`: Comma-separated list of all node addresses (include master and peers)
- `--heartbeat`: Heartbeat interval (default: `5s`)
- `--coord-timeout`: 2PC coordinator timeout used if this node is elected master (default: `10s`)
- `--dsn`: Postgres DSN (optional if `POSTGRES_DSN` env var is set)

## Running Tests

```bash
go test ./... -v
```



## Architecture

```
┌─────────────┐     HTTP      ┌─────────────┐
│   Master    │◄─────────────►│   Node 1    │
│ (Coordinator│               │ (Participant)│
│  + Participant)             │              │
└──────┬──────┘               └──────┬───────┘
       │                             │
       │ HTTP                        │ Postgres
       │                             ▼
       │                      ┌─────────────┐
       │                      │   Shard 1   │
       │                      │  Database   │
       │                      └─────────────┘
       │
       │                      ┌─────────────┐
       └─────────────────────►│   Node 2    │
                              │ (Participant)│
                              └──────┬───────┘
                                     │
                                     │ Postgres
                                     ▼
                              ┌─────────────┐
                              │   Shard 2   │
                              │  Database   │
                              └─────────────┘
```

## License MIT

Note: Description is written in AI
