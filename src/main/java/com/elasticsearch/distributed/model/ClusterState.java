package com.elasticsearch.distributed.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An immutable snapshot of the entire cluster's state at a given version.
 *
 * <h2>Study Notes – Cluster State in Elasticsearch</h2>
 *
 * <p>
 * The <em>cluster state</em> is the single source of truth for everything
 * structural in a cluster: which nodes exist, what indices are configured,
 * and where every shard currently lives. Understanding cluster state is
 * fundamental to understanding almost every distributed aspect of
 * Elasticsearch.
 *
 * <h3>1. State Machine Model</h3>
 * <p>
 * The elected master is the <b>only</b> node that may publish a new cluster
 * state. It does so via a two-phase commit:
 * <ol>
 * <li><b>Pre-publish</b>: master sends the next state to all nodes and waits
 * for a quorum acknowledgement.</li>
 * <li><b>Commit</b>: once quorum acknowledges, master sends the "commit"
 * message. All nodes apply the new state atomically.</li>
 * </ol>
 * This is equivalent to the "log commit" phase in Raft; the cluster state
 * version number acts as the log index.
 *
 * <h3>2. Immutability Guarantee</h3>
 * <p>
 * Every {@code ClusterState} instance is immutable. Any change (node join,
 * shard move, index creation) produces a <em>new</em> {@code ClusterState}
 * with an incremented {@code version}. This makes cluster state diffs trivial
 * (compare versions / checksums) and eliminates entire classes of concurrency
 * bugs: concurrent readers never see a partially-updated state.
 * <p>
 * In real Elasticsearch this is implemented via a builder pattern similar to
 * the one shown in {@link #builder(ClusterState)}.
 *
 * <h3>3. The Routing Table</h3>
 * <p>
 * The routing table embedded in cluster state maps every index shard to its
 * current node(s). The allocation service ({@link
 * com.elasticsearch.distributed.service.ShardAllocationService}) reads the
 * routing table and node stats to produce the <em>next</em> routing table
 * (which becomes part of the next cluster state).
 *
 * <h3>4. Coordination Metadata</h3>
 * <p>
 * Includes the elected-master ID, voting configuration (the set of node IDs
 * that form the Raft quorum), and the last-committed configuration. Changing
 * the voting configuration itself requires a Raft-style commit to avoid
 * split-brain during membership changes (joint-consensus per Raft §6).
 *
 * <h3>Interview Talking Points</h3>
 * <ul>
 * <li>Why does ES serialize cluster state as a diff rather than the full
 * snapshot on every publication? Full state can be megabytes in large
 * clusters; incremental diffs keep inter-node traffic manageable.</li>
 * <li>What happens when a node receives a cluster state with a version
 * <em>older</em> than the one it already has? It silently ignores it –
 * cluster state is monotonically versioned.</li>
 * <li>What is the "cluster state lag"? A data node may temporarily apply
 * writes against a shard using a slightly stale routing table. The
 * primary-term + seqNo model (see {@link ShardRouting}) ensures
 * correctness even in this situation.</li>
 * </ul>
 *
 * @param version       Monotonically increasing version counter. Incremented
 *                      on every cluster state change.
 * @param masterNodeId  ID of the current elected master, or {@code null} if
 *                      no master has been elected yet (cluster is forming).
 * @param nodes         Immutable map: nodeId → {@link Node}.
 * @param routingTable  Immutable map: "index/shardId" → list of
 *                      {@link ShardRouting} entries (one primary + replicas).
 * @param indexSettings Per-index settings (number_of_shards,
 *                      number_of_replicas, etc.).
 */
public record ClusterState(
        long version,
        String masterNodeId,
        Map<String, Node> nodes,
        Map<String, List<ShardRouting>> routingTable,
        Map<String, Map<String, String>> indexSettings) {

    /** Sentinel value for "no master elected yet". */
    public static final long NO_MASTER = -1L;

    public ClusterState {
        Objects.requireNonNull(nodes, "nodes map must not be null");
        Objects.requireNonNull(routingTable, "routingTable must not be null");
        Objects.requireNonNull(indexSettings, "indexSettings must not be null");
        // Deep defensive copy – guarantees immutability
        nodes = Map.copyOf(nodes);
        // Each list in the routing table is also made unmodifiable
        var rtCopy = new HashMap<String, List<ShardRouting>>(routingTable.size());
        routingTable.forEach((k, v) -> rtCopy.put(k, List.copyOf(v)));
        routingTable = Collections.unmodifiableMap(rtCopy);
        indexSettings = Map.copyOf(indexSettings);
    }

    // ── Factory ───────────────────────────────────────────────────────────────

    /** Returns an empty initial cluster state (version 0, no master). */
    public static ClusterState empty() {
        return new ClusterState(0L, null, Map.of(), Map.of(), Map.of());
    }

    // ── Derived helpers ───────────────────────────────────────────────────────

    /** Returns {@code true} when a quorum of master-eligible nodes are known. */
    public boolean hasMaster() {
        return masterNodeId != null && nodes.containsKey(masterNodeId);
    }

    /**
     * Looks up the routing key used in the routing table map.
     * Real ES uses an {@code IndexRoutingTable} hierarchy; we flatten for brevity.
     */
    public static String routingKey(String index, int shardId) {
        return index + "/" + shardId;
    }

    /** Number of data nodes (nodes with the DATA role). */
    public long dataNodeCount() {
        return nodes.values().stream().filter(Node::isDataNode).count();
    }

    /** Number of master-eligible nodes (Raft voters). */
    public long masterEligibleCount() {
        return nodes.values().stream().filter(Node::isMasterEligible).count();
    }

    /**
     * Minimum quorum size for master elections.
     * Formula: ⌊masterEligibleCount / 2⌋ + 1
     * <p>
     * Example: 3 master-eligible nodes → quorum = 2.
     * This means the cluster tolerates 1 master failure without split-brain.
     */
    public long quorumSize() {
        return masterEligibleCount() / 2 + 1;
    }

    // ── Builder (immutable update pattern) ───────────────────────────────────

    /**
     * Returns a mutable builder pre-populated with this state's data.
     * Use this to create a next-version state (only the master does this).
     */
    public Builder builder() {
        return builder(this);
    }

    /** Returns a fresh builder seeded from an existing state. */
    public static Builder builder(ClusterState base) {
        return new Builder(base);
    }

    /**
     * Mutable builder – only the master node constructs new cluster states.
     * Separating the mutable builder from the immutable record mirrors the
     * pattern used in the real Elasticsearch codebase.
     */
    public static final class Builder {
        private long version;
        private String masterNodeId;
        private final Map<String, Node> nodes;
        private final Map<String, List<ShardRouting>> routingTable;
        private final Map<String, Map<String, String>> indexSettings;

        private Builder(ClusterState base) {
            this.version = base.version();
            this.masterNodeId = base.masterNodeId();
            this.nodes = new HashMap<>(base.nodes());
            this.routingTable = new HashMap<>();
            base.routingTable().forEach((k, v) -> this.routingTable.put(k, List.copyOf(v)));
            this.indexSettings = new HashMap<>(base.indexSettings());
        }

        public Builder masterNodeId(String id) {
            this.masterNodeId = id;
            return this;
        }

        public Builder addNode(Node n) {
            nodes.put(n.id(), n);
            return this;
        }

        public Builder removeNode(String nodeId) {
            nodes.remove(nodeId);
            return this;
        }

        public Builder putShardRoutings(String index, int shardId, List<ShardRouting> shards) {
            routingTable.put(ClusterState.routingKey(index, shardId), shards);
            return this;
        }

        public Builder putIndexSetting(String index, String key, String value) {
            indexSettings.computeIfAbsent(index, k -> new HashMap<>()).put(key, value);
            return this;
        }

        /** Increments the version and builds an immutable {@link ClusterState}. */
        public ClusterState build() {
            return new ClusterState(++version, masterNodeId, nodes, routingTable, indexSettings);
        }
    }
}
