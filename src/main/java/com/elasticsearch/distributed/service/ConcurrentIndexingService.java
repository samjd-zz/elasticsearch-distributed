package com.elasticsearch.distributed.service;

import com.elasticsearch.distributed.model.ClusterState;
import com.elasticsearch.distributed.model.ShardRouting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.List;
import java.util.concurrent.locks.StampedLock;

/**
 * Demonstrates concurrent, consistent indexing across a single shard's
 * primary, mirroring the design of Elasticsearch's {@code IndexShard}.
 *
 * <h2>Study Notes – Concurrent Indexing in Elasticsearch</h2>
 *
 * <p>
 * A primary shard in Elasticsearch is the serialisation point for all
 * writes to a given shard. However, the primary itself is not single-threaded
 * — multiple bulk requests may arrive simultaneously and need careful
 * concurrency management.
 *
 * <h3>1. Sequence Numbers (seqNo) and Primary Terms</h3>
 * <p>
 * Every index/delete/update operation receives two values from the primary:
 * <ul>
 * <li><b>primaryTerm</b> – the current primary's "epoch". Monotonically
 * increasing. Stored in every Lucene document. An old primary (from
 * a network partition) will be rejected because its term is stale.</li>
 * <li><b>seqNo</b> – a per-primary counter assigned in the order operations
 * are processed. Used for:
 * <ol>
 * <li>Optimistic concurrency control ({@code if_seq_no} /
 * {@code if_primary_term}).</li>
 * <li>Replica ordering – replicas apply ops in seqNo order.</li>
 * <li>Translog checkpointing – global checkpoint = min(localCheckpoint)
 * across all ISR members.</li>
 * </ol>
 * </li>
 * </ul>
 *
 * <h3>2. The Indexing Memory Buffer</h3>
 * <p>
 * Before a document reaches Lucene it lands in the in-memory indexing
 * buffer. Elasticsearch periodically "refreshes" (default 1 s) which flushes
 * the buffer to a new Lucene segment (making docs visible to searches) but
 * does NOT fsync. A "flush" (fsync + translog rollover) is less frequent.
 *
 * <h3>3. StampedLock for Read/Write Concurrency</h3>
 * <p>
 * {@link StampedLock} (Java 8+) is chosen here because:
 * <ul>
 * <li>It supports optimistic reads – a reader can speculatively read without
 * acquiring any lock, then validate the stamp. If validation fails it
 * upgrades to a pessimistic read lock. This pattern is ideal for
 * cluster-state reads which are vastly more frequent than writes.</li>
 * <li>It avoids the write-starvation problem of {@code ReadWriteLock}.</li>
 * <li>It does not support re-entrant locking (unlike {@code ReentrantLock});
 * callers must not attempt to re-acquire while holding a stamp.</li>
 * </ul>
 *
 * <h3>4. Optimistic Concurrency Control (OCC)</h3>
 * <p>
 * Elasticsearch clients can send {@code if_seq_no=N & if_primary_term=P}
 * to implement compare-and-swap semantics. If the document's current seqNo
 * and primaryTerm don't match, the operation is rejected with a 409 Conflict.
 * This avoids lost-update races without pessimistic locking across machines.
 *
 * <h3>5. Replication Protocol (Primary-Backup)</h3>
 * <p>
 * After writing to the local Lucene + translog, the primary forwards the
 * operation (with assigned seqNo and primaryTerm) to each in-sync replica.
 * The primary awaits acknowledgement from all active ISR members before
 * returning to the client (when {@code wait_for_active_shards} = "all").
 * {@link CompletableFuture} is a natural model for this fan-out + join.
 *
 * <h3>6. Java 21 Virtual Threads</h3>
 * <p>
 * Elasticsearch's IndexShard uses OS threads + async callbacks via
 * the Netty event loop. Java 21 virtual threads ({@code Thread.ofVirtual()})
 * are an alternative model that can simplify the code while maintaining
 * throughput. This class demonstrates both: the {@link #indexAsync} method
 * submits to an {@link ExecutorService} that could be backed by virtual
 * threads.
 *
 * <h3>Interview Talking Points</h3>
 * <ul>
 * <li>Why is the primary the only writer? Avoids N→M coordination between
 * all replicas. Keeps the protocol simple: primary is the authoritative
 * seqNo generator; replicas are passive consumers.</li>
 * <li>What happens if a replica falls behind? It is removed from the ISR.
 * Once removed it catches up via a replay of the translog (from global
 * checkpoint) or a full peer-recovery if it's too far behind.</li>
 * <li>What is "write blocking" during primary relocation? When a primary is
 * relocating (moving to a new node), a "relocation handshake" transfers
 * the primary role. During the handshake, writes are briefly buffered
 * (not blocked at the client level, but queued internally).</li>
 * </ul>
 */
public final class ConcurrentIndexingService {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentIndexingService.class);

    // ── Sequence number counters ──────────────────────────────────────────────

    /**
     * Global sequence number generator for this shard.
     * {@link AtomicLong#getAndIncrement()} is a single CAS instruction on x86;
     * this gives us a lock-free, monotonically increasing counter.
     */
    private final AtomicLong seqNoGenerator = new AtomicLong(0);

    /**
     * Current primary term. Incremented when a new primary is elected.
     * Reads are frequent (every index op); writes are rare (failovers).
     */
    private volatile long primaryTerm;

    /**
     * Local checkpoint: the highest seqNo such that all ops ≤ seqNo have been
     * processed locally. In a real shard this is maintained in a
     * {@code CountedBitSet} to handle gaps (ops can complete out of order).
     *
     * <p>
     * Using {@link LongAdder} here is illustrative; in production the
     * local checkpoint tracker in ES uses a {@code FixedBitSet} overlay.
     */
    private final AtomicLong localCheckpoint = new AtomicLong(-1);

    // ── Concurrency ───────────────────────────────────────────────────────────

    /**
     * {@link StampedLock} guards shard-level state (primaryTerm, recovery mode).
     * Write lock on rare events (primary promotion, shard close).
     * Optimistic reads for every indexing operation.
     */
    private final StampedLock shardLock = new StampedLock();

    /**
     * Thread pool for async replication to replicas.
     * In production this is Netty's I/O thread pool.
     */
    private final ExecutorService replicationPool = Executors.newVirtualThreadPerTaskExecutor(); // Java 21 virtual
                                                                                                 // threads

    // ── Index state ───────────────────────────────────────────────────────────

    /** True while the shard is in recovery (peer-recovery or snapshot restore). */
    private volatile boolean recovering = false;

    private final String shardId; // e.g. "products/0"

    /**
     * Latest cluster state used to discover in-sync replica shard routing.
     * Updated by the master via {@link #updateClusterState(ClusterState)}.
     */
    private volatile ClusterState latestClusterState;

    /**
     * High-throughput operation counter.
     * {@link LongAdder} is preferred over {@code AtomicLong} for pure
     * increment-only metrics: it shards the counter across CPU stripes,
     * eliminating CAS contention under heavy concurrent write load. Call
     * {@link LongAdder#sum()} to read the approximate total.
     */
    private final LongAdder opsIndexedCounter = new LongAdder();

    public ConcurrentIndexingService(String shardId, long initialPrimaryTerm) {
        this.shardId = shardId;
        this.primaryTerm = initialPrimaryTerm;
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Indexes (or updates) a document on this primary shard.
     *
     * <p>
     * Steps:
     * <ol>
     * <li>Acquire an optimistic read stamp from the {@link StampedLock}.</li>
     * <li>Validate shard state (not recovering, not closed).</li>
     * <li>Generate a seqNo and primaryTerm snapshot atomically.</li>
     * <li>Write to the local Lucene buffer + translog
     * (simulated here by a log statement).</li>
     * <li>Replicate to all in-sync replicas asynchronously.</li>
     * <li>Advance the local checkpoint once the op is durable.</li>
     * </ol>
     *
     * @param docId    Document ID (used as routing key if custom routing is
     *                 absent).
     * @param document JSON source of the document.
     * @return {@link IndexResult} with the assigned seqNo and primaryTerm.
     * @throws IllegalStateException if the shard is not the active primary.
     */
    public IndexResult index(String docId, String document) {
        // Step 1: optimistic read of shard state
        long stamp = shardLock.tryOptimisticRead();
        boolean localRecovering = recovering;
        long localPrimaryTerm = primaryTerm;

        if (!shardLock.validate(stamp)) {
            // Optimistic read was invalidated – fall back to pessimistic read lock
            stamp = shardLock.readLock();
            try {
                localRecovering = recovering;
                localPrimaryTerm = primaryTerm;
            } finally {
                shardLock.unlockRead(stamp);
            }
        }

        if (localRecovering) {
            throw new IllegalStateException("Shard " + shardId + " is in recovery mode");
        }

        // Step 2: assign seqNo (atomic, no lock needed – AtomicLong CAS)
        long seqNo = seqNoGenerator.getAndIncrement();

        // Step 3: write to local Lucene + translog (simulated)
        log.debug("[{}] index doc={} seqNo={} term={}", shardId, docId, seqNo, localPrimaryTerm);
        // In production: engine.index(new Engine.Index(...seqNo, primaryTerm...))

        // Step 4: replicate asynchronously to ISR replicas
        long finalPrimaryTerm = localPrimaryTerm;
        CompletableFuture<Void> replicationFuture = replicateToReplicas(docId, document, seqNo, finalPrimaryTerm);

        // Step 5: advance local checkpoint after local write (not waiting for replicas
        // yet)
        advanceLocalCheckpoint(seqNo);
        // Increment the ops counter – LongAdder.increment() is wait-free
        opsIndexedCounter.increment();

        IndexResult result = new IndexResult(seqNo, localPrimaryTerm, docId);
        log.info("[{}] indexed doc={} → seqNo={} primaryTerm={}", shardId, docId, seqNo, localPrimaryTerm);

        // Replication is async; attach error handler
        replicationFuture.exceptionally(ex -> {
            log.error("[{}] Replication failed for seqNo={}: {}", shardId, seqNo, ex.getMessage());
            // In production: remove the failed node from ISR and continue
            return null;
        });

        return result;
    }

    /**
     * Performs a conditional (optimistic concurrency control) update.
     *
     * <p>
     * Equivalent to Elasticsearch's
     * {@code index(doc, if_seq_no=N, if_primary_term=P)}.
     * Returns {@link java.util.Optional#empty()} on a version conflict (HTTP 409).
     *
     * @param docId            Document ID.
     * @param document         New document source.
     * @param expectedSeqNo    The seqNo the caller last read (must be current).
     * @param expectedPrimTerm The primaryTerm the caller last read.
     * @return Result or empty on conflict.
     */
    public java.util.Optional<IndexResult> indexWithCAS(
            String docId, String document, long expectedSeqNo, long expectedPrimTerm) {

        // In a real implementation this check happens inside the Lucene engine
        // (InternalEngine#index) under a per-document Semaphore.
        // Here we simulate it with a simple version check:
        long currentSeqNo = seqNoGenerator.get() - 1; // last assigned
        if (currentSeqNo != expectedSeqNo || primaryTerm != expectedPrimTerm) {
            log.warn("[{}] OCC conflict doc={}: expected seqNo={}/term={}, actual seqNo={}/term={}",
                    shardId, docId, expectedSeqNo, expectedPrimTerm, currentSeqNo, primaryTerm);
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(index(docId, document));
    }

    /**
     * Advances the primary term (called by the master after electing this
     * shard as the new primary). Acquires the write lock to block concurrent
     * indexing ops while the term is updated.
     *
     * @param newPrimaryTerm The new term; must be greater than current.
     */
    public void updatePrimaryTerm(long newPrimaryTerm) {
        long stamp = shardLock.writeLock();
        try {
            if (newPrimaryTerm <= primaryTerm) {
                throw new IllegalArgumentException(
                        "New term " + newPrimaryTerm + " must exceed current term " + primaryTerm);
            }
            log.info("[{}] PrimaryTerm updated: {} → {}", shardId, primaryTerm, newPrimaryTerm);
            this.primaryTerm = newPrimaryTerm;
        } finally {
            shardLock.unlockWrite(stamp);
        }
    }

    /** Returns the current local checkpoint (highest processed seqNo). */
    public long localCheckpoint() {
        return localCheckpoint.get();
    }

    /** Returns the next unassigned sequence number (= total ops processed). */
    public long maxSeqNo() {
        return seqNoGenerator.get() - 1;
    }

    /**
     * Registers the latest cluster state so that {@link #replicateToReplicas}
     * can discover ISR replicas dynamically from the routing table.
     *
     * <p>
     * Called by the master whenever a new cluster state is published.
     * The {@code volatile} write is intentionally lock-free – the replication
     * path only needs an eventually-consistent view of the ISR; strict ordering
     * is enforced by the seqNo + primaryTerm protocol.
     *
     * @param clusterState The newly published cluster state.
     */
    public void updateClusterState(ClusterState clusterState) {
        this.latestClusterState = clusterState;
        log.debug("[{}] Cluster state updated to version {}", shardId, clusterState.version());
    }

    /**
     * Returns the total number of documents successfully indexed on this primary.
     * Backed by {@link LongAdder#sum()}, which is eventually consistent but
     * non-blocking even under heavy concurrent increments.
     */
    public long totalIndexedOps() {
        return opsIndexedCounter.sum();
    }

    public void shutdown() {
        replicationPool.shutdown();
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Simulates fan-out replication over virtual threads.
     *
     * <p>
     * In real Elasticsearch the primary sends a {@code BulkShardRequest}
     * over the Netty transport channel to each replica. Each replica applies
     * the op, updates its local checkpoint, and sends an acknowledgement.
     * The primary collects acks and updates the global checkpoint.
     */
    /**
     * Fans out the indexed operation to all in-sync replica shards.
     *
     * <p>
     * Replicas are discovered from the routing table in the latest
     * {@link ClusterState} (registered via {@link #updateClusterState}).
     * Each replica write is a {@link Callable}{@code <Long>} that returns
     * the replica's new local checkpoint after applying the op. The primary
     * can then advance the global checkpoint to
     * {@code min(allReplicaLocalCheckpoints)} – the highest seqNo all ISR
     * members have confirmed durably.
     *
     * <h4>Callable vs Runnable</h4>
     * {@code Runnable} is fire-and-forget (no return, no checked exception);
     * {@code Callable<V>} returns a typed result and may throw. Using
     * {@code Callable<Long>} here makes the replica-checkpoint contract
     * explicit in the type signature rather than relying on side-effects.
     */
    private CompletableFuture<Void> replicateToReplicas(
            String docId, String document, long seqNo, long term) {

        List<ShardRouting> isrReplicas = discoverISRReplicas();
        if (isrReplicas.isEmpty()) {
            log.debug("[{}] No started ISR replicas in cluster state – acking immediately", shardId);
            return CompletableFuture.completedFuture(null);
        }

        // Each Callable<Long> returns the replica's local checkpoint after applying the
        // op.
        CompletableFuture<?>[] futures = isrReplicas.stream()
                .map(replica -> {
                    Callable<Long> task = replicaWriteTask(replica, docId, seqNo, term);
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            return task.call(); // → replica's new local checkpoint (= seqNo here)
                        } catch (Exception ex) {
                            throw new java.util.concurrent.CompletionException(ex);
                        }
                    }, replicationPool);
                })
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(futures);
    }

    /**
     * Builds a {@link Callable}{@code <Long>} that represents a single replica
     * write operation.
     *
     * <p>
     * In production Elasticsearch this Callable would:
     * <ol>
     * <li>Serialise the op (seqNo, primaryTerm, source) into a wire frame.</li>
     * <li>Send it over the Netty transport channel to the replica's
     * {@code transportAddress()}.</li>
     * <li>Await the replica's {@code ReplicationResponse} (which carries
     * the replica's local checkpoint).</li>
     * <li>Return that checkpoint so the primary can advance the global
     * checkpoint: {@code globalCheckpoint = min(replicaCheckpoints)}.</li>
     * </ol>
     *
     * @param replica The {@link ShardRouting} identifying the target ISR member.
     * @param docId   Document ID being replicated.
     * @param seqNo   Sequence number assigned by the primary.
     * @param term    Primary term at time of indexing.
     * @return Callable that yields the replica's local checkpoint.
     */
    private static Callable<Long> replicaWriteTask(
            ShardRouting replica, String docId, long seqNo, long term) {
        return () -> {
            // In production: serialise, send via Netty to replica.transportAddress(), await
            // ack.
            log.debug("  replica={} applying seqNo={} term={} doc={}",
                    replica.nodeId(), seqNo, term, docId);
            return seqNo; // simplified: replica processes this op immediately in order
        };
    }

    /**
     * Looks up the in-sync replica {@link ShardRouting} entries for this shard
     * from the latest published {@link ClusterState}.
     *
     * <p>
     * Filters to STARTED replicas only — INITIALIZING replicas are not yet
     * in the ISR and must not receive forwarded ops until peer-recovery completes.
     */
    private List<ShardRouting> discoverISRReplicas() {
        ClusterState cs = latestClusterState;
        if (cs == null)
            return List.of();
        return cs.routingTable()
                .getOrDefault(shardId, List.of())
                .stream()
                .filter(sr -> !sr.isPrimary() && sr.isStarted())
                .toList();
    }

    /**
     * Advances the local checkpoint to {@code seqNo} if all prior operations
     * have been processed (no gaps). A gap means an earlier seqNo is still
     * in flight; the checkpoint cannot advance past the gap.
     *
     * <p>
     * In real ES, a {@code LocalCheckpointTracker} (a bitmap-based structure)
     * tracks which seqNos have completed and computes the prefix checkpoint.
     */
    private void advanceLocalCheckpoint(long seqNo) {
        // Simplified: assume no gaps for this in-order demo
        localCheckpoint.updateAndGet(current -> Math.max(current, seqNo));
    }

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Result returned to the client after a successful index operation.
     *
     * <p>
     * Maps to the {@code _seq_no} and {@code _primary_term} fields in the
     * Elasticsearch REST response. Clients should store these for OCC updates.
     *
     * @param seqNo       Sequence number assigned to this operation.
     * @param primaryTerm Primary term at the time of indexing.
     * @param docId       Document ID.
     */
    public record IndexResult(long seqNo, long primaryTerm, String docId) {
    }
}
