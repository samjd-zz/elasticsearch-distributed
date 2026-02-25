package com.elasticsearch.distributed.model;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Represents a single node in an Elasticsearch cluster.
 *
 * <h2>Study Notes – Nodes in Elasticsearch</h2>
 * <p>
 * In a real Elasticsearch cluster every JVM process is a "node". Nodes self-
 * elect roles at start-up (or are given them via configuration). Key roles:
 * <ul>
 * <li><b>master-eligible</b> – may participate in the master election.
 * Elasticsearch uses a <em>quorum-based consensus algorithm</em>
 * (based on Raft, see
 * {@link com.elasticsearch.distributed.service.RaftLeaderElection})
 * to pick exactly one active master at a time.</li>
 * <li><b>data</b> – stores shards (primary and replica). A data node
 * handles indexing, search, and bulk operations locally.</li>
 * <li><b>coordinating-only</b> – routes requests, merges partial results.
 * Every node is <em>implicitly</em> a coordinating node.</li>
 * <li><b>ingest</b> – runs pre-index transformation pipelines.</li>
 * <li><b>remote_cluster_client</b> – handles cross-cluster
 * search/replication.</li>
 * </ul>
 *
 * <h2>Why Immutability Matters Here</h2>
 * <p>
 * The cluster state is published atomically to all nodes (see
 * {@link ClusterState}). Node descriptors must be <em>immutable</em> so
 * that a published state snapshot can never be mutated by a concurrent
 * request. Java {@code record}s give us immutability, structural equality,
 * and a compact canonical constructor for free.
 *
 * <h2>Interview Talking Points</h2>
 * <ul>
 * <li>Minimum master-eligible nodes for a quorum = ⌊N/2⌋ + 1 to avoid
 * split-brain (two independent clusters both believing they are the
 * master). Elasticsearch 7+ auto-computes this via the
 * {@code minimum_master_nodes} being replaced by the voting-configuration
 * exclusions API.</li>
 * <li>Node IDs are stable across restarts so shard allocation decisions
 * (e.g. "shard 0 primary must be on node X") survive a rolling
 * restart.</li>
 * </ul>
 *
 * @param id            Persistent unique identifier (UUID). Stable across
 *                      restarts as long as the data directory is intact.
 * @param name          Human-readable label (e.g. "es-node-1").
 * @param host          Hostname or IP used for inter-node transport.
 * @param transportPort Port used for the internal transport protocol
 *                      (default 9300). Cluster-internal comms, index/shard
 *                      operations, and cluster state publication all go
 *                      through this port.
 * @param httpPort      Port used for the REST API (default 9200).
 * @param roles         Immutable set of {@link NodeRole} values.
 * @param version       Elasticsearch version string running on this node.
 *                      Mixed-version clusters are supported for rolling
 *                      upgrades but the master always runs the newest version.
 */
public record Node(
        String id,
        String name,
        String host,
        int transportPort,
        int httpPort,
        Set<NodeRole> roles,
        String version) {

    /**
     * Compact canonical constructor – validates invariants.
     * Records call this before field assignment; validations here apply to
     * every construction path including deserialization frameworks.
     */
    public Node {
        Objects.requireNonNull(id, "Node id must not be null");
        Objects.requireNonNull(name, "Node name must not be null");
        Objects.requireNonNull(host, "Node host must not be null");
        Objects.requireNonNull(roles, "Node roles must not be null");
        // Defensive copy – prevents callers from mutating the set after creation
        roles = Set.copyOf(roles);
    }

    // ── Factory helpers ───────────────────────────────────────────────────────

    /**
     * Creates a fully-featured data + master-eligible node, which is the
     * default configuration for small clusters (≤ 10 nodes).
     */
    public static Node masterDataNode(String name, String host) {
        return new Node(
                UUID.randomUUID().toString(),
                name, host, 9300, 9200,
                Set.of(NodeRole.MASTER_ELIGIBLE, NodeRole.DATA, NodeRole.INGEST),
                "8.12.2");
    }

    /** Creates a dedicated master-eligible node (no data shards). */
    public static Node dedicatedMaster(String name, String host) {
        return new Node(
                UUID.randomUUID().toString(),
                name, host, 9300, 9200,
                Set.of(NodeRole.MASTER_ELIGIBLE),
                "8.12.2");
    }

    /** Creates a dedicated data node. */
    public static Node dedicatedData(String name, String host) {
        return new Node(
                UUID.randomUUID().toString(),
                name, host, 9300, 9200,
                Set.of(NodeRole.DATA, NodeRole.INGEST),
                "8.12.2");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Returns {@code true} if this node can vote in master elections. */
    public boolean isMasterEligible() {
        return roles.contains(NodeRole.MASTER_ELIGIBLE);
    }

    /** Returns {@code true} if this node holds data shards. */
    public boolean isDataNode() {
        return roles.contains(NodeRole.DATA);
    }

    /**
     * Transport address used by other nodes to open connections (Netty channel).
     */
    public String transportAddress() {
        return host + ":" + transportPort;
    }

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Node roles as understood by Elasticsearch 8.x.
     *
     * <p>
     * Keeping roles in an enum rather than arbitrary strings lets the
     * compiler catch typos and allows exhaustive {@code switch} expressions –
     * a Java 21 pattern-matching best practice.
     */
    public enum NodeRole {
        /** Can participate in master elections (Raft voter). */
        MASTER_ELIGIBLE,
        /** Stores primary/replica shards; handles bulk indexing & search. */
        DATA,
        /** Runs ingest pipelines (field transformations, grok parsing, etc.) */
        INGEST,
        /**
         * Coordinating-only: routes requests and merges results.
         * Every node is implicitly coordinating; this role disables data/master.
         */
        COORDINATING_ONLY,
        /** Participates in cross-cluster search and replication. */
        REMOTE_CLUSTER_CLIENT
    }
}
