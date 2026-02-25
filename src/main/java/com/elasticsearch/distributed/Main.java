package com.elasticsearch.distributed;

import com.elasticsearch.distributed.model.ClusterState;
import com.elasticsearch.distributed.model.Node;
import com.elasticsearch.distributed.service.ConcurrentIndexingService;
import com.elasticsearch.distributed.service.RaftLeaderElection;
import com.elasticsearch.distributed.service.ShardAllocationService;
import com.elasticsearch.distributed.util.AsyncNetworkChannel;
import com.elasticsearch.distributed.util.TranslogWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Application entry point – wires the distributed-systems study components.
 *
 * <h2>Elastic Senior Software Developer – Study Driver</h2>
 *
 * <p>
 * This class demonstrates the full lifecycle of the key concepts needed
 * for the Elasticsearch Distributed Systems team role:
 *
 * <ol>
 * <li><b>Cluster bootstrapping</b>: create a 3-node cluster (3 master-eligible
 * data nodes), satisfying the quorum requirement.</li>
 * <li><b>Raft leader election</b>: one node starts the election cycle;
 * see {@link com.elasticsearch.distributed.service.RaftLeaderElection}.</li>
 * <li><b>Shard allocation</b>: the elected master allocates an index's shards
 * across the available data nodes, enforcing same-node exclusion;
 * see
 * {@link com.elasticsearch.distributed.service.ShardAllocationService}.</li>
 * <li><b>Concurrent indexing</b>: documents are indexed with seqNo /
 * primaryTerm
 * assignment and async replica fan-out;
 * see
 * {@link com.elasticsearch.distributed.service.ConcurrentIndexingService}.</li>
 * <li><b>Translog durability</b>: each indexing op is appended to the
 * write-ahead
 * log before being acknowledged;
 * see {@link com.elasticsearch.distributed.util.TranslogWriter}.</li>
 * <li><b>Async transport (Netty pattern)</b>: inter-node requests use a
 * fire-and-forget + correlation-map pattern;
 * see {@link com.elasticsearch.distributed.util.AsyncNetworkChannel}.</li>
 * </ol>
 *
 * <h3>Quick-Reference: Key Formulas</h3>
 * 
 * <pre>
 *   Quorum         = ⌊N/2⌋ + 1   (N = master-eligible nodes)
 *   Fault tolerance= ⌊(N-1)/2⌋
 *   Shard routing  = |Murmur3(routing_value)| % num_primary_shards
 * </pre>
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        log.info("=== Elasticsearch Distributed Systems – Study Demo ===");

        // ── 1. Build initial cluster state ───────────────────────────────────
        log.info("\n--- 1. Cluster Bootstrap ---");

        Node node1 = Node.masterDataNode("es-node-1", "10.0.0.1");
        Node node2 = Node.masterDataNode("es-node-2", "10.0.0.2");
        Node node3 = Node.masterDataNode("es-node-3", "10.0.0.3");

        ClusterState initial = ClusterState.empty().builder()
                .addNode(node1).addNode(node2).addNode(node3)
                .build();

        AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initial);

        log.info("Cluster: {} nodes, quorum={}, faultTolerance={}",
                initial.nodes().size(),
                initial.quorumSize(),
                RaftLeaderElection.faultTolerance((int) initial.masterEligibleCount()));

        // ── 2. Raft leader election ───────────────────────────────────────────
        log.info("\n--- 2. Raft Leader Election ---");

        RaftLeaderElection raft = new RaftLeaderElection(node1.id(), clusterStateRef);
        raft.addPeer(node2);
        raft.addPeer(node3);

        // Simulate vote grants from both peers → quorum reached → node1 becomes leader
        // (In production this is driven by network messages over the Netty transport.)
        long candidateTerm = raft.currentTerm() + 1;
        boolean vote2 = raft.handleVoteRequest(node1.id(), candidateTerm);
        boolean vote3 = raft.handleVoteRequest(node1.id(), candidateTerm);
        log.info("Vote from node2={}, vote from node3={}", vote2, vote3);

        // Manually trigger the leader path for the demo
        raft.onVoteGranted(node2.id(), candidateTerm);
        raft.onVoteGranted(node3.id(), candidateTerm);

        log.info("Raft status: {}", raft.statusSummary());
        log.info("Cluster master: {}", clusterStateRef.get().masterNodeId());

        raft.stop();

        // ── 3. Shard allocation ──────────────────────────────────────────────
        log.info("\n--- 3. Shard Allocation (index: products, 3P × 1R) ---");

        ShardAllocationService allocator = new ShardAllocationService(clusterStateRef);
        ClusterState afterAllocation = allocator.allocateNewIndex("products", 3, 1);

        afterAllocation.routingTable().forEach((key, shards) -> {
            shards.forEach(sr -> log.info("  {} {} → node:{} [{}]",
                    key, sr.shardType(), sr.nodeId(), sr.state()));
        });

        // Document routing demo
        List.of("user-101", "order-202", "product-303", "review-404").forEach(id -> {
            int shard = ShardAllocationService.routeDocument(id, 3);
            log.info("  Document '{}' routes to shard {}", id, shard);
        });

        // ── 4. Simulate node failure + re-allocation ─────────────────────────
        log.info("\n--- 4. Node Failure Simulation ---");
        ClusterState afterFailure = allocator.onNodeLeft(node3.id());
        log.info("After node3 left: {} nodes remaining", afterFailure.nodes().size());

        // ── 5. Concurrent indexing with translog ─────────────────────────────
        log.info("\n--- 5. Concurrent Indexing + Translog ---");

        ConcurrentIndexingService indexing = new ConcurrentIndexingService("products/0", 1L);

        Path translogDir = Path.of(System.getProperty("java.io.tmpdir"), "es-study-translog");
        try (TranslogWriter translog = new TranslogWriter(translogDir, 1L, true)) {

            String[] docs = {
                    "{\"name\":\"Laptop\",\"price\":999}",
                    "{\"name\":\"Mouse\",\"price\":29}",
                    "{\"name\":\"Monitor\",\"price\":399}"
            };

            for (int i = 0; i < docs.length; i++) {
                String docId = "doc-" + i;
                ConcurrentIndexingService.IndexResult result = indexing.index(docId, docs[i]);
                // Append to translog BEFORE acknowledging to the client
                translog.append(result.seqNo(), result.primaryTerm(), docId, docs[i]);
                log.info("  Indexed: docId={} seqNo={} term={}",
                        result.docId(), result.seqNo(), result.primaryTerm());
            }

            log.info("  LocalCheckpoint={} MaxSeqNo={}",
                    indexing.localCheckpoint(), indexing.maxSeqNo());

            // Optimistic concurrency control demo
            var casResult = indexing.indexWithCAS("doc-0", "{\"name\":\"Laptop Pro\"}", 0L, 1L);
            casResult.ifPresentOrElse(
                    r -> log.info("  CAS success: seqNo={}", r.seqNo()),
                    () -> log.info("  CAS failed: version conflict (expected)"));
        }

        indexing.shutdown();

        // ── 6. Async network channel (Netty pattern) ─────────────────────────
        log.info("\n--- 6. Async Network Channel (Netty-style) ---");

        AsyncNetworkChannel channel = new AsyncNetworkChannel(node1.id());
        channel.registerHandler("indices:data/write/bulk[s]",
                req -> log.info("  [replica] Received bulk shard request: {}", req));

        // Simulate sending an inter-node request and receiving the response
        CompletableFuture<String> replication = channel.sendRequest(
                node2.id(), "indices:data/write/bulk[s]",
                "{\"seqNo\":10,\"ops\":[...]}", 5_000);

        // Simulate the response arriving from the remote node
        channel.onResponseReceived(1L, "{\"localCheckpoint\":10}");
        replication.whenComplete((resp, err) -> {
            if (err != null)
                log.error("  Replication error: {}", err.getMessage());
            else
                log.info("  Replication ack: {}", resp);
        });

        // Give async tasks a moment
        Thread.sleep(200);
        channel.shutdown();

        log.info("\n=== Study Demo Complete ===");
        log.info("Files to review:");
        log.info("  model/Node.java             – node roles, quorum math");
        log.info("  model/ShardRouting.java      – primary/replica, seqNo, primary term");
        log.info("  model/ClusterState.java      – immutable state, builder pattern, quorum");
        log.info("  service/RaftLeaderElection.java – Raft roles, terms, vote mechanics");
        log.info("  service/ShardAllocationService.java – allocation constraints, routing hash");
        log.info("  service/ConcurrentIndexingService.java – StampedLock, OCC, replica fan-out");
        log.info("  util/TranslogWriter.java     – WAL, fsync, global checkpoint");
        log.info("  util/AsyncNetworkChannel.java – Netty request/response correlation");
    }
}
