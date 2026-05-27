# Implementation: Bully Leader Election

## Theoretical Foundation

The **Bully Algorithm** (Garcia-Molina, 1982) is a classic distributed election algorithm where the node with the highest ID becomes the leader. Higher-ID nodes "bully" lower-ID ones out of elections.

### Protocol

1. **Election initiation** — Node P detects leader failure (heartbeat timeout):
   - P sends `ELECTION` to all nodes with ID > P
   - If no higher node responds within `ELECTION_TIMEOUT`: P declares itself leader, sends `COORDINATOR` to all nodes
   - If any higher node responds `ALIVE`: P defers; the higher node takes over

2. **Receiving ELECTION** — Node Q receives `ELECTION` from lower-ID sender:
   - Q responds `ALIVE`
   - Q starts its own election (sends `ELECTION` to IDs > Q)

3. **Receiving COORDINATOR** — Node records the sender as the new leader

4. **Heartbeat** — Leader periodically pings all nodes. Nodes trigger election if no heartbeat within `LEADER_TIMEOUT`.

### Properties
- **Safety:** At most one leader in a connected network
- **Liveness:** Eventually a new leader is elected (highest alive ID wins)
- **Complexity:** O(n) messages per election, O(n²) worst-case

---

## Architecture

```
                    ┌──────────────┐
                    │  PostgreSQL  │  (node registry)
                    └──────┬───────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ┌────▼─────┐      ┌────▼─────┐      ┌────▼─────┐
   │ Node-1   │◄────►│ Node-2   │◄────►│ Node-3   │
   │ ID=1     │ HTTP │ ID=2     │ HTTP │ ID=3     │
   │ :8081    │      │ :8082    │      │ :8083    │
   └──────────┘      └──────────┘      └──────────┘
        ▲                  ▲                  ▲
        └──────────────────┴──────────────────┘
           POST /election  POST /coordinator  POST /heartbeat
```

Inter-node communication uses HTTP POST. Nodes discover each other via the `PEERS` env var (comma-separated URLs). Node IDs are extracted from URL patterns (`node-{N}`).

---

## Files Changed

### 1. `src/election.py` — Core Algorithm

State (in-memory, thread-safe):
- `NODE_ID`, `PEERS` — from environment
- `current_leader_id`, `current_leader_url` — who is the leader
- `_last_heartbeat` — timestamp of last received heartbeat
- `_election_in_progress` — mutex flag

Key functions:
- `start_election()` — initiate Bully election, returns True if became leader
- `handle_election_message(sender_id)` — respond ALIVE, cascade election upward
- `handle_coordinator_message(leader_id, leader_url)` — record new leader
- `handle_heartbeat(leader_id)` — refresh heartbeat timestamp
- `declare_victory()` — announce self as leader to all peers
- `_wait_for_coordinator()` — timeout + retry if higher node fails after responding ALIVE

Background threads (3 daemon):
- `_leader_heartbeat_loop()` — leader sends heartbeat every `HEARTBEAT_INTERVAL` seconds
- `_heartbeat_monitor_loop()` — checks leader liveness, triggers election on timeout
- `_startup_election_check()` — one-shot: queries peers for existing leader, starts election if none

### 2. `src/app.py` — API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/election` | Receive election message. Body: `{"sender_id": int}` |
| POST | `/coordinator` | Receive coordinator announcement. Body: `{"leader_id": int, "leader_url": str}` |
| POST | `/heartbeat` | Receive leader heartbeat. Body: `{"leader_id": int}` |
| GET | `/leader` | Returns `{"leader_id": N, "leader_url": "..."}` or nulls |

Startup hook: `@app.on_event("startup")` calls `election.init_election()`
Shutdown hook: `@app.on_event("shutdown")` calls `election.shutdown()`

### 3. `docker-compose.yml` — Orchestration

4 services:
- `db` — PostgreSQL 16 Alpine
- `node-1`, `node-2`, `node-3` — FastAPI instances

Each node gets:
- `NODE_ID` = 1, 2, 3
- `PEERS` listing the other two nodes (e.g., `http://node-2:8080,http://node-3:8080`)
- `DATABASE_URL` pointing to `db:5432`
- External port mapping: 8081, 8082, 8083 → internal 8080

---

## Design Decisions

### HTTP transport (not message queue)
The project already uses FastAPI + `requests`. Hidden tests expect HTTP endpoints. No new infrastructure.

### Peer ID from URL regex (not API call)
Deterministic, zero-latency, avoids chicken-and-egg discovery problem. The docker-compose naming convention makes this reliable.

### In-memory state (not database)
Election is coordination, not persistence. Avoids DB coupling, distributed locks, and latency. `GET /leader` exposes state externally.

### `_wait_for_coordinator()` with retry
After deferring to a higher node, wait for COORDINATOR. If none arrives within `LEADER_TIMEOUT`, restart election. Handles cascading failures where the higher node crashes mid-election.

### Timeout values
- `ELECTION_TIMEOUT=3s` — per-request; generous for local Docker network
- `LEADER_TIMEOUT=10s` — ~2 missed heartbeats; balances detection speed vs false positives
- `HEARTBEAT_INTERVAL=5s` — frequent enough for fast detection, light on resources

### `threading` over `asyncio`
Background loops are I/O-bound but independent of the request cycle. Threads keep the election module testable outside FastAPI. `requests` (sync) is already a dependency. Thread overhead at 3 daemon threads is negligible.
