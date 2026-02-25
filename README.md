# Elasticsearch Distributed Systems — Study Project

A hands-on Java 21 study project covering every major distributed-systems concept required for the **Elasticsearch Distributed Systems** engineering role. Every class is extensively annotated with study notes, interview talking points, and direct references to real Elasticsearch internals.

---

## What's Inside

| Package | Class | Concept |
|---|---|---|
| `model` | `Node` | Node roles, quorum formula, master-eligible nodes |
| `model` | `ShardRouting` | Primary/replica model, ISR, seqNo, primaryTerm, shard lifecycle |
| `model` | `ClusterState` | Immutable state, two-phase commit publication, builder pattern |
| `service` | `RaftLeaderElection` | Raft consensus — terms, roles, voting, heartbeats, split-brain avoidance |
| `service` | `ShardAllocationService` | Allocation constraints, same-node exclusion, Murmur3 routing hash, ISR promotion |
| `service` | `ConcurrentIndexingService` | `StampedLock`, `LongAdder`, optimistic CAS, `Callable`-based replica fan-out |
| `util` | `TranslogWriter` | Write-ahead log, fsync strategy, CRC frames, crash-recovery replay |
| `util` | `AsyncNetworkChannel` | Netty request/response correlation, timeout scheduling, backpressure |
| `client` | `ElasticsearchClientFactory` | Official Java client lifecycle, connection pooling, auth |
| `config` | `ElasticsearchConfig` | Environment-based configuration record |
| — | `Main` | End-to-end wiring demo — runs the full lifecycle |

---

## Key Concepts Covered

### Raft Consensus (`RaftLeaderElection`)
- Three roles: **FOLLOWER → CANDIDATE → LEADER**
- Randomised election timeouts to prevent split-vote
- Term-based epoch: any message with a higher term causes an immediate step-down
- `knownLeaderId()` returns `Optional<String>` — explicit "no leader yet" contract
- `votingConfiguration()` returns the current Raft quorum membership
- Quorum and fault-tolerance formulas:

```
quorum         = ⌊N / 2⌋ + 1
faultTolerance = ⌊(N - 1) / 2⌋
```

### Shard Allocation (`ShardAllocationService`)
- Hard constraint: primary and replica of the same shard **never** on the same node
- ISR promotion on primary failure (replica with highest seqNo wins)
- Document routing via consistent hash:

```
targetShard = |Murmur3(routing_value)| % num_primary_shards
```

- Why shard counts are fixed at index creation time
- Disk threshold watermarking and rebalance throttling

### Concurrent Indexing (`ConcurrentIndexingService`)
- `StampedLock` — optimistic reads (no lock overhead on the hot path) with fallback to pessimistic read lock
- `AtomicLong` seqNo generator — single CAS instruction, lock-free
- `LongAdder` ops counter — stripe-sharded to eliminate CAS contention under parallel writes
- `Callable<Long>` replica tasks — return the replica's local checkpoint for global checkpoint advancement
- ISR replica discovery from live `ClusterState` routing table
- Optimistic concurrency control: `if_seq_no` + `if_primary_term`

### Translog (`TranslogWriter`)
- Write-ahead log appended **before** the client is acknowledged
- Frame format: `seqNo(8) | primaryTerm(8) | bodyLen(4) | body | CRC32(4)`
- `syncOnWrite=true` → `FileChannel.force()` per request (durability = request mode)
- `readOpsFrom(path, offset)` — crash-recovery replay using `RandomAccessFile.seek()` to jump directly to the Lucene commit offset
- CRC mismatch on trailing entry = partial write from a crash → safe truncation

### Async Networking (`AsyncNetworkChannel`)
- Netty's fire-and-forget + correlation map pattern
- Every request gets a unique `requestId`; a `CompletableFuture` is stored in a `ConcurrentHashMap`
- Response frame completes the future; timeout callback on the event loop cleans it up
- Mirrors `TransportService#sendRequest` + `PendingResponseHandlers` in ES source

### Cluster State (`ClusterState`)
- Fully immutable Java `record` with deep defensive copies
- Only the elected master produces a new state (via `Builder`)
- Version is monotonically increasing — stale states are silently ignored
- Two-phase commit: pre-publish to quorum → commit → nodes apply atomically

---

## Quick-Reference Cheat Sheet

```
# Cluster sizing
Quorum       : ⌊N/2⌋ + 1    (N = master-eligible nodes)
Fault tol.   : ⌊(N-1)/2⌋
Rec. sizes   : 1, 3, 5, 7  (odd — 4 buys nothing over 3)

# Document routing
shard = |Murmur3(routing_value)| % number_of_primary_shards

# Durability modes (index.translog.durability)
request  → fsync on every bulk request  (default, no data loss)
async    → fsync on interval (default 5s, up to 5s of data loss)

# Checkpoints
localCheckpoint   = highest seqNo this copy has processed consecutively
globalCheckpoint  = min(localCheckpoint) across all ISR members
                  = safe translog truncation point
```

---

## Build & Run

**Requirements:** Java 21+, Maven 3.9+

```bash
# Build
mvn clean package -DskipTests

# Run tests
mvn test

# Run the demo
mvn exec:java -Dexec.mainClass="com.elasticsearch.distributed.Main"
```

The demo output walks through:
1. 3-node cluster bootstrap
2. Raft election simulation
3. Index allocation (3 primaries × 1 replica)
4. Node failure + ISR promotion
5. Concurrent indexing with translog writes
6. Async inter-node request/response

---

## Project Structure

```
src/main/java/com/elasticsearch/distributed/
├── Main.java                          ← end-to-end wiring demo
├── client/
│   └── ElasticsearchClientFactory.java
├── config/
│   └── ElasticsearchConfig.java
├── model/
│   ├── ClusterState.java
│   ├── Node.java
│   └── ShardRouting.java
├── service/
│   ├── ConcurrentIndexingService.java
│   ├── RaftLeaderElection.java
│   └── ShardAllocationService.java
└── util/
    ├── AsyncNetworkChannel.java
    └── TranslogWriter.java
```

---

## Further Reading

- [Raft paper — Ongaro & Ousterhout (2014)](https://raft.github.io/raft.pdf)
- [Elasticsearch Zen2 design doc](https://github.com/elastic/elasticsearch/blob/main/docs/internal/Consensus.md)
- [ES source — transport layer](https://github.com/elastic/elasticsearch/tree/main/server/src/main/java/org/elasticsearch/transport)
- [ES source — IndexShard](https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/index/shard/IndexShard.java)
- [ES source — Translog](https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/index/translog/Translog.java)
- [Netty 4 user guide](https://netty.io/wiki/user-guide-for-4.x.html)
