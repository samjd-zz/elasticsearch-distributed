package com.elasticsearch.distributed.service;

import com.elasticsearch.distributed.model.ClusterState;
import com.elasticsearch.distributed.model.Node;
import com.elasticsearch.distributed.model.ShardRouting;
import com.elasticsearch.distributed.model.ShardRouting.ShardState;
import com.elasticsearch.distributed.model.ShardRouting.ShardType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Assigns and rebalances shards across the nodes of a cluster.
 *
 * <h2>Study Notes – Shard Allocation in Elasticsearch</h2>
 *
 * <p>
 * The shard allocator is one of the most complex subsystems in
 * Elasticsearch. It runs on the master node whenever the cluster
 * topology changes (node join/leave), an index is created/deleted, or
 * a shard fails. The allocator must satisfy a set of hard and soft
 * constraints:
 *
 * <h3>Hard Constraints (allocation filtering / awareness)</h3>
 * <ul>
 * <li><b>Same-node exclusion</b>: a primary and any of its replicas must
 * never reside on the same physical node. Violating this means a single
 * hardware failure could destroy both copies.</li>
 * <li><b>Rack / zone awareness</b>
 * ({@code cluster.routing.allocation.awareness})
 * extends the same logic across failure domains. In a 3-AZ deployment
 * you want at most one copy per AZ.</li>
 * <li><b>Allocation filtering</b> ({@code index.routing.allocation.include},
 * {@code exclude}, {@code require}) lets operators pin or exclude
 * specific nodes.</li>
 * </ul>
 *
 * <h3>Soft Constraints (balancing)</h3>
 * <ul>
 * <li><b>Shard count balancing</b>: distribute shards evenly by count so no
 * node is overloaded.</li>
 * <li><b>Write-load balancing</b>: in ES 8.x the allocator can factor in
 * index write-load (docs/s) to avoid hot-spotting.</li>
 * <li><b>Disk threshold router</b>
 * ({@code cluster.routing.allocation.disk.threshold_enabled}):
 * refuses to allocate to nodes above the high watermark (default 90%
 * disk used). Initiates relocation when the flood-stage watermark (95%)
 * is hit.</li>
 * </ul>
 *
 * <h3>Primary Recovery</h3>
 * <p>
 * When a primary shard is lost, the master must pick one in-sync replica
 * to promote. The In-Sync Replicas set (ISR) in the routing table's
 * {@code allocationId} list determines eligibility. A replica outside the
 * ISR may have stale operations, so the master must wait for a quorum of
 * ISR members to respond before promoting. The node must also pass a
 * "stale primary check" – it cannot be elected if a fresher copy exists.
 *
 * <h3>Consistent Hashing for Routing</h3>
 * <p>
 * Document routing:
 * {@code targetShard = Murmur3.hash(routingValue) % numPrimaries}.
 * The routing value defaults to {@code _id} but can be set per-document.
 * Custom routing allows co-locating related documents on the same shard,
 * reducing scatter-gather fan-out on queries.
 *
 * <h3>Rebalancing</h3>
 * <p>
 * After allocation, the allocator checks whether moving shards to other
 * nodes would improve balance. ES uses a "weight function" (configurable
 * floats for shard count vs. write load). Moves are throttled by
 * {@code cluster.routing.allocation.node_concurrent_recoveries} to avoid
 * overwhelming I/O.
 *
 * <h3>Interview Talking Points</h3>
 * <ul>
 * <li>What is a "dangling index"? An index known to data nodes (present in
 * their data directory) but not in cluster state. ES Zen2 auto-imports
 * them when a majority of nodes have not seen the index – avoids data
 * loss on accidental master failure with stale state.</li>
 * <li>What is shard "hot-spotting" and how do you fix it? Too many writes
 * routed to one shard (e.g. monotonically increasing IDs skip most
 * shards). Fix: use a random routing value, roll over to a new index,
 * or increase the number of primary shards (requires re-indexing because
 * the shard count is fixed post-creation).</li>
 * <li>Why does rebalancing trigger I/O? Moving a shard requires peer
 * recovery: the target node streams all Lucene segment files from the
 * source over the transport channel. Only the translog delta (ops since
 * the global checkpoint) needs to be replayed, not the entire shard.</li>
 * </ul>
 */
public final class ShardAllocationService {

    private static final Logger log = LoggerFactory.getLogger(ShardAllocationService.class);

    private final AtomicReference<ClusterState> clusterStateRef;

    public ShardAllocationService(AtomicReference<ClusterState> clusterStateRef) {
        this.clusterStateRef = clusterStateRef;
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Allocates all shards for a newly created index across the available
     * data nodes, applying the same-node-exclusion hard constraint.
     *
     * <p>
     * Steps:
     * <ol>
     * <li>Create {@code numPrimaries} primary shards, striped round-robin
     * across data nodes (simple even distribution).</li>
     * <li>For each primary, assign {@code numReplicas} replica shards to
     * <em>different</em> nodes (same-node exclusion).</li>
     * <li>Publish the updated routing table as the next cluster state.</li>
     * </ol>
     *
     * @param index        Index name to allocate.
     * @param numPrimaries Number of primary shards (fixed after this call).
     * @param numReplicas  Number of replicas per primary.
     */
    public ClusterState allocateNewIndex(String index, int numPrimaries, int numReplicas) {
        ClusterState current = clusterStateRef.get();
        List<Node> dataNodes = current.nodes().values().stream()
                .filter(Node::isDataNode)
                .sorted(Comparator.comparing(Node::id)) // deterministic ordering
                .collect(Collectors.toList());

        if (dataNodes.isEmpty()) {
            throw new IllegalStateException("No data nodes available for allocation");
        }
        if (numReplicas >= dataNodes.size()) {
            log.warn("Requested {} replicas but only {} data nodes available – replicas may be unassigned",
                    numReplicas, dataNodes.size());
        }

        ClusterState.Builder builder = current.builder()
                .putIndexSetting(index, "number_of_shards", String.valueOf(numPrimaries))
                .putIndexSetting(index, "number_of_replicas", String.valueOf(numReplicas));

        for (int shardId = 0; shardId < numPrimaries; shardId++) {
            List<ShardRouting> copies = allocateShard(index, shardId, numReplicas, dataNodes);
            builder.putShardRoutings(index, shardId, copies);
            copies.forEach(sr -> log.info("Allocated {}", formatShard(sr)));
        }

        ClusterState next = builder.build();
        clusterStateRef.set(next);
        log.info("Index '{}' allocated: {} primaries × {} replicas across {} nodes",
                index, numPrimaries, numReplicas, dataNodes.size());
        return next;
    }

    /**
     * Reacts to a node leaving the cluster: finds all shards on the departed
     * node and marks them unassigned so they can be re-allocated.
     *
     * <p>
     * In real ES, this triggers the allocation service reroute on the next
     * master cluster-state update cycle.
     *
     * @param departedNodeId ID of the node that left.
     */
    public ClusterState onNodeLeft(String departedNodeId) {
        ClusterState current = clusterStateRef.get();
        ClusterState.Builder builder = current.builder().removeNode(departedNodeId);

        Map<String, List<ShardRouting>> affectedShards = new HashMap<>();
        current.routingTable().forEach((key, shards) -> {
            boolean hasDepartedCopy = shards.stream()
                    .anyMatch(s -> departedNodeId.equals(s.nodeId()));
            if (!hasDepartedCopy)
                return;

            List<ShardRouting> updated = shards.stream()
                    .map(s -> departedNodeId.equals(s.nodeId())
                            ? unassignOrPromote(s, shards)
                            : s)
                    .collect(Collectors.toList());
            affectedShards.put(key, updated);
        });

        affectedShards.forEach((key, shards) -> {
            String[] parts = key.split("/");
            builder.putShardRoutings(parts[0], Integer.parseInt(parts[1]), shards);
        });

        ClusterState next = builder.build();
        clusterStateRef.set(next);
        log.warn("Node {} left – marked {} shard groups for re-allocation",
                departedNodeId, affectedShards.size());
        return next;
    }

    /**
     * Computes a document's target primary shard using the standard
     * Murmur3-hash-based routing formula.
     *
     * <pre>
     * targetShard = Math.abs(Murmur3.hash(routingValue)) % numPrimaries
     * </pre>
     *
     * <p>
     * Using a consistent hash ensures stateless routing: any coordinating
     * node can determine the target shard without consulting cluster state
     * per document.
     *
     * @param routingValue The routing value – defaults to the document {@code _id}.
     * @param numPrimaries Number of primary shards in the index (fixed at
     *                     creation).
     * @return Zero-based shard number.
     */
    public static int routeDocument(String routingValue, int numPrimaries) {
        // Simplified Murmur3-style hash using Java's built-in (not the exact ES
        // implementation)
        int hash = murmur3Hash(routingValue);
        return Math.abs(hash) % numPrimaries;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Allocates one shard (primary + replicas) applying same-node exclusion.
     */
    private List<ShardRouting> allocateShard(
            String index, int shardId, int numReplicas, List<Node> dataNodes) {

        List<ShardRouting> copies = new ArrayList<>();

        // Assign primary via round-robin
        Node primaryNode = dataNodes.get(shardId % dataNodes.size());
        copies.add(ShardRouting.newPrimary(index, shardId, primaryNode.id()).started());

        Set<String> usedNodes = new java.util.HashSet<>();
        usedNodes.add(primaryNode.id());

        // Assign replicas to remaining nodes
        int replicaIndex = 0;
        for (int r = 0; r < numReplicas; r++) {
            // Find next available node not already hosting this shard
            Node replicaNode = null;
            for (int attempt = 0; attempt < dataNodes.size(); attempt++) {
                Node candidate = dataNodes.get((shardId + 1 + replicaIndex + attempt) % dataNodes.size());
                if (!usedNodes.contains(candidate.id())) {
                    replicaNode = candidate;
                    break;
                }
            }
            replicaIndex++;

            if (replicaNode != null) {
                usedNodes.add(replicaNode.id());
                copies.add(new ShardRouting(index, shardId, ShardType.REPLICA,
                        replicaNode.id(), ShardState.STARTED,
                        1L, -1L, -1L));
            } else {
                // Cannot satisfy constraint – shard stays unassigned
                copies.add(ShardRouting.unassignedReplica(index, shardId));
                log.warn("Cannot place replica {} of shard {}/{} – no eligible nodes", r, index, shardId);
            }
        }
        return copies;
    }

    /**
     * When the node holding a shard copy departs:
     * <ul>
     * <li>If the departed copy was a REPLICA, mark it UNASSIGNED.</li>
     * <li>If the departed copy was the PRIMARY, promote the first STARTED
     * replica (ISR promotion, simplified).</li>
     * </ul>
     */
    private ShardRouting unassignOrPromote(ShardRouting departed, List<ShardRouting> allCopies) {
        if (departed.shardType() == ShardType.REPLICA) {
            return ShardRouting.unassignedReplica(departed.index(), departed.shardId());
        }
        // Primary departed – find a started replica to promote
        return allCopies.stream()
                .filter(s -> s.shardType() == ShardType.REPLICA && s.isStarted())
                .findFirst()
                .map(replica -> {
                    log.warn("Promoting replica on node {} to PRIMARY for {}/{}",
                            replica.nodeId(), departed.index(), departed.shardId());
                    return replica.promoteToLastKnownPrimary();
                })
                .orElseGet(() -> {
                    log.error("No in-sync replica available to promote for {}/{}! RED cluster!",
                            departed.index(), departed.shardId());
                    return ShardRouting.unassignedReplica(departed.index(), departed.shardId());
                });
    }

    private static String formatShard(ShardRouting sr) {
        return String.format("[%s/%d] %s → node:%s (%s)",
                sr.index(), sr.shardId(), sr.shardType(), sr.nodeId(), sr.state());
    }

    /**
     * Simplified Murmur3-inspired hash for study purposes.
     * Real Elasticsearch uses {@code org.elasticsearch.common.hash.MurmurHash3}.
     */
    private static int murmur3Hash(String key) {
        // Java's String#hashCode is not Murmur3 but serves as a placeholder here.
        // In production use the actual Murmur3 finalizer for better avalanche effect.
        int h = key.hashCode();
        // Murmur3 finalizer mix
        h ^= (h >>> 16);
        h *= 0x85ebca6b;
        h ^= (h >>> 13);
        h *= 0xc2b2ae35;
        h ^= (h >>> 16);
        return h;
    }
}
