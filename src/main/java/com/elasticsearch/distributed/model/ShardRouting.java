package com.elasticsearch.distributed.model;

import java.util.Objects;

/**
 * Describes the routing entry for a single Elasticsearch shard copy.
 *
 * <h2>Study Notes – Shards and Replication in Elasticsearch</h2>
 *
 * <p>
 * An Elasticsearch <em>index</em> is divided into N <b>primary shards</b>
 * (set at index creation, fixed thereafter) and M <b>replica shards</b> per
 * primary (adjustable at any time). Each shard is an autonomous Lucene index:
 * a self-contained, searchable unit of data.
 *
 * <h3>Primary / Replica Model</h3>
 * 
 * <pre>
 *   Index "products" – 3 primaries, 1 replica each (6 shards total)
 *
 *   Node-A : P0  R1
 *   Node-B : P1  R2
 *   Node-C : P2  R0
 *
 *   P = primary, R = replica of that shard id
 * </pre>
 * <p>
 * Rules enforced by the shard-allocation logic:
 * <ol>
 * <li>A primary and its replica(s) must NEVER be on the same node
 * (avoid losing both copies on a single hardware failure).</li>
 * <li>Only the primary accepts writes. The primary forwards each operation
 * to its in-sync replicas atomically before acknowledging the client.</li>
 * <li>If a primary fails, the master promotes one in-sync replica.
 * The <em>in-sync copies</em> (ISR) set is tracked in cluster state;
 * only ISR members are eligible for promotion.</li>
 * </ol>
 *
 * <h3>Sequence Numbers and Primary Terms</h3>
 * <p>
 * Elasticsearch 6.1+ replaced version-based replication with a
 * <em>primary-term / sequence-number</em> model (borrowed from Kafka):
 * <ul>
 * <li><b>primary term</b> – monotonically increasing integer, incremented
 * every time a new primary is elected. Acts as a logical epoch.</li>
 * <li><b>sequence number (seqNo)</b> – per-shard counter, assigned by the
 * primary to every indexing operation in order. Replicas apply ops in
 * seqNo order. Also used for optimistic concurrency control by clients:
 * {@code if_seq_no} + {@code if_primary_term}.</li>
 * </ul>
 * This model enables <em>local checkpoints</em> and <em>global
 * checkpoints</em>:
 * the global checkpoint is the highest seqNo that all active ISR members have
 * acknowledged, and it marks the safe point for translog truncation.
 *
 * <h3>Interview Talking Points</h3>
 * <ul>
 * <li>Why are primary shard counts fixed at creation? Because the routing
 * formula {@code shard = hash(routing_key) % num_primaries} would change
 * if you added primaries, making all existing documents un-findable.</li>
 * <li>How does Elasticsearch achieve "at-least-once" delivery to replicas?
 * The primary retries forwarding until the replica acknowledges. The
 * replica uses seqNo deduplication to make it idempotent.</li>
 * <li>What is a "zombie primary"? A node that was the primary, gets
 * partitioned off, and another primary is elected. The old primary
 * rejects writes because its primary term is stale (it sees a
 * {@code IllegalStateException: primary term too old}).</li>
 * </ul>
 *
 * @param index            Name of the Elasticsearch index.
 * @param shardId          Zero-based shard number within the index.
 * @param shardType        PRIMARY or REPLICA.
 * @param nodeId           ID of the node currently holding this shard copy.
 * @param state            Lifecycle state of the shard copy.
 * @param primaryTerm      Monotonically increasing integer – incremented on
 *                         each
 *                         new primary election for this shard.
 * @param globalCheckpoint
 *                         Highest sequence number acknowledged by ALL in-sync
 *                         replicas. Safe truncation point for the translog.
 * @param localCheckpoint
 *                         Highest consecutive sequence number this copy has
 *                         processed locally.
 */
public record ShardRouting(
        String index,
        int shardId,
        ShardType shardType,
        String nodeId,
        ShardState state,
        long primaryTerm,
        long globalCheckpoint,
        long localCheckpoint) {

    public ShardRouting {
        Objects.requireNonNull(index, "index must not be null");
        Objects.requireNonNull(shardType, "shardType must not be null");
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        if (shardId < 0)
            throw new IllegalArgumentException("shardId must be >= 0");
    }

    // ── Factory helpers ───────────────────────────────────────────────────────

    /** Creates an INITIALIZING primary shard in term 1. */
    public static ShardRouting newPrimary(String index, int shardId, String nodeId) {
        return new ShardRouting(index, shardId, ShardType.PRIMARY, nodeId,
                ShardState.INITIALIZING, 1L, -1L, -1L);
    }

    /** Creates an UNASSIGNED replica (no node yet). */
    public static ShardRouting unassignedReplica(String index, int shardId) {
        return new ShardRouting(index, shardId, ShardType.REPLICA, "UNASSIGNED",
                ShardState.UNASSIGNED, 0L, -1L, -1L);
    }

    // ── Derived helpers ───────────────────────────────────────────────────────

    public boolean isPrimary() {
        return shardType == ShardType.PRIMARY;
    }

    public boolean isAssigned() {
        return state != ShardState.UNASSIGNED;
    }

    public boolean isStarted() {
        return state == ShardState.STARTED;
    }

    /**
     * Produces a copy of this routing entry in the STARTED state.
     * Records are immutable; "mutation" returns a new instance.
     */
    public ShardRouting started() {
        return new ShardRouting(index, shardId, shardType, nodeId,
                ShardState.STARTED, primaryTerm, globalCheckpoint, localCheckpoint);
    }

    /**
     * Promotes a replica to primary by incrementing the primary term.
     * Only valid on a REPLICA in STARTED state.
     */
    public ShardRouting promoteToLastKnownPrimary() {
        if (shardType != ShardType.REPLICA || state != ShardState.STARTED) {
            throw new IllegalStateException(
                    "Can only promote a STARTED REPLICA, got: " + this);
        }
        return new ShardRouting(index, shardId, ShardType.PRIMARY, nodeId,
                ShardState.STARTED, primaryTerm + 1, globalCheckpoint, localCheckpoint);
    }

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Whether a shard copy is primary (accepts writes) or replica (read-only,
     * sync'd).
     */
    public enum ShardType {
        PRIMARY, REPLICA
    }

    /**
     * Lifecycle states a shard copy passes through.
     *
     * <pre>
     *   UNASSIGNED → INITIALIZING → STARTED → RELOCATING → (on new node) INITIALIZING → STARTED
     *                                      ↘ (failure) UNASSIGNED
     * </pre>
     */
    public enum ShardState {
        /**
         * No node assigned yet. The allocation service will attempt to find
         * a suitable node based on disk thresholds, shard counts, and placement
         * constraints (same-node avoidance for primary + replica pairs).
         */
        UNASSIGNED,

        /**
         * Assigned to a node, recovery in progress (peer-recovery from primary
         * or restore from snapshot).
         */
        INITIALIZING,

        /** Fully recovered, actively serving reads/writes. */
        STARTED,

        /**
         * Being moved to another node (rebalancing or explicit move API call).
         * Both source and target copies exist briefly; once the target reaches
         * STARTED, the source is removed.
         */
        RELOCATING
    }
}
