package com.elasticsearch.distributed.service;

import com.elasticsearch.distributed.model.ClusterState;
import com.elasticsearch.distributed.model.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Simplified Raft-based leader election for a cluster of master-eligible nodes.
 *
 * <h2>Study Notes – Raft Consensus Algorithm</h2>
 *
 * <p>
 * Elasticsearch's cluster coordination (introduced in 7.0 as the "Zen2"
 * subsystem, later refined) is heavily based on the
 * <a href="https://raft.github.io/raft.pdf">Raft consensus algorithm</a>
 * by Ongaro and Ousterhout (2014). Understanding Raft is directly applicable
 * to ES interview questions.
 *
 * <h3>The Three Raft Roles</h3>
 * 
 * <pre>
 *   FOLLOWER ──(election timeout)──▶ CANDIDATE ──(quorum votes)──▶ LEADER
 *      ▲                                   │ (loses election / hears leader)  │
 *      └───────────────────────────────────┘◀─────────────────────────────────┘
 * </pre>
 * <ul>
 * <li><b>FOLLOWER</b>: passive; resets election timer when it hears from
 * the leader (heartbeat / AppendEntries RPC).</li>
 * <li><b>CANDIDATE</b>: started an election; voted for itself; sending
 * RequestVote RPCs to peers.</li>
 * <li><b>LEADER</b>: won the election (received quorum votes); sends
 * periodic heartbeats to prevent followers from timing out; the only
 * node allowed to append entries to the replicated log (= cluster state
 * changes in ES).</li>
 * </ul>
 *
 * <h3>Key Raft Properties</h3>
 * <ol>
 * <li><b>Election Safety</b>: at most one leader per term (guaranteed by
 * requiring a majority vote and that each node votes at most once per
 * term).</li>
 * <li><b>Leader Append-Only</b>: a leader never overwrites or deletes
 * entries in its log; it only appends.</li>
 * <li><b>Log Matching</b>: if two entries in different nodes' logs have the
 * same index and term, then the logs are identical for all preceding
 * entries.</li>
 * <li><b>Leader Completeness</b>: a candidate cannot be elected unless its
 * log is at least as up-to-date as the majority's log.</li>
 * <li><b>State Machine Safety</b>: once a log entry is committed (majority
 * acknowledges it), it will forever remain in the log.</li>
 * </ol>
 *
 * <h3>Term and Vote Mechanics</h3>
 * <p>
 * A Raft <em>term</em> is analogous to Elasticsearch's <em>cluster state
 * version</em> epoch. When a follower's election timeout fires:
 * <ol>
 * <li>It increments its current term (analogous to ES primary term).</li>
 * <li>It transitions to CANDIDATE and votes for itself.</li>
 * <li>It sends {@code RequestVote} to all peers (analogous to ES
 * {@code StartJoinRequest} / {@code JoinRequest} RPCs).</li>
 * <li>A peer grants a vote only if:
 * <ul>
 * <li>the candidate's term ≥ peer's current term, AND</li>
 * <li>the peer has not yet voted this term, AND</li>
 * <li>the candidate's log is at least as up-to-date as the peer's.</li>
 * </ul>
 * </li>
 * <li>If the candidate receives ⌊N/2⌋+1 votes it becomes LEADER.</li>
 * </ol>
 *
 * <h3>Randomised Election Timeouts (Split-vote Avoidance)</h3>
 * <p>
 * If all nodes had the same election timeout they would all become
 * candidates simultaneously, splitting the vote. Raft uses <em>randomised</em>
 * timeouts (150–300 ms in the paper; ES uses configurable values).
 * This implementation demonstrates the pattern using
 * {@link ThreadLocalRandom}.
 *
 * <h3>Joint-Consensus (Membership Changes)</h3>
 * <p>
 * Adding or removing nodes from the voting configuration requires special
 * care to avoid a window where two independent quorums could form. Raft §6
 * describes joint-consensus: the transition is committed in two phases
 * (C_old+new then C_new). ES implements this via the voting-configuration
 * exclusions API and auto-shrink logic.
 *
 * <h3>Interview Talking Points</h3>
 * <ul>
 * <li>Why does ES require an odd number of master-eligible nodes (1, 3, 5)?
 * With an even number (e.g. 4), the quorum is 3/4 which wastes
 * fault-tolerance compared to 3-node (quorum 2, tolerates 1 failure).
 * 4-node tolerates 1 failure anyway, so the 4th node buys nothing.</li>
 * <li>What is "pre-voting" in Raft? A pre-vote phase where a candidate
 * checks whether it <em>would</em> win an election before incrementing
 * its term. This prevents unnecessary term inflation from partitioned
 * nodes repeatedly timing out. ES Zen2 implements this.</li>
 * <li>How does the "follower checks" mechanism work in ES? In ES, only the
 * leader sends follower-check requests; if a follower doesn't respond,
 * the leader removes it from the cluster. Symmetrically, followers
 * send leader-check requests and start an election if the leader is
 * unreachable.</li>
 * </ul>
 */
public final class RaftLeaderElection {

    private static final Logger log = LoggerFactory.getLogger(RaftLeaderElection.class);

    // ── Election timeout bounds (ms) ──────────────────────────────────────────
    private static final int TIMEOUT_MIN_MS = 150;
    private static final int TIMEOUT_MAX_MS = 300;
    private static final int HEARTBEAT_INTERVAL_MS = 50;

    // ── Per-node persistent state (survive restarts in real Raft) ────────────
    /** Monotonically increasing election term. */
    private final AtomicLong currentTerm = new AtomicLong(0);
    /** nodeId this node voted for in the current term; null = not yet voted. */
    private volatile String votedFor = null;
    /**
     * ID of the last known leader. {@code null} until the first election
     * concludes or a valid heartbeat is received. Exposed via
     * {@link #knownLeaderId()} as an {@link Optional} to avoid null-checks
     * at call sites.
     */
    private volatile String knownLeaderId = null;

    // ── Volatile state (reset on each election) ───────────────────────────────
    private final AtomicReference<RaftRole> role = new AtomicReference<>(RaftRole.FOLLOWER);
    /** Votes received as candidate. */
    private final Set<String> votesReceived = ConcurrentHashMap.newKeySet();

    // ── Cluster bookkeeping ───────────────────────────────────────────────────
    private final String localNodeId;
    private final Map<String, Node> peers = new HashMap<>();

    /** Protected by the stateLock for multi-field atomic transitions. */
    private final ReentrantLock stateLock = new ReentrantLock();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        var t = new Thread(r, "raft-election-scheduler");
        t.setDaemon(true);
        return t;
    });

    /** Shared cluster state reference – updated when a leader is elected. */
    private final AtomicReference<ClusterState> clusterStateRef;

    public RaftLeaderElection(String localNodeId, AtomicReference<ClusterState> clusterStateRef) {
        this.localNodeId = localNodeId;
        this.clusterStateRef = clusterStateRef;
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /** Register a peer node that participates in elections. */
    public void addPeer(Node node) {
        peers.put(node.id(), node);
        log.debug("Peer added: {} (total peers: {})", node.id(), peers.size());
    }

    /** Start the election timer loop. Mirrors a Raft follower's initial state. */
    public void start() {
        log.info("[{}] Raft follower started – waiting for leader heartbeat", localNodeId);
        scheduleElectionTimeout();
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    /** Returns the current term number (Raft §5.1). */
    public long currentTerm() {
        return currentTerm.get();
    }

    /** Returns the current Raft role of this node. */
    public RaftRole role() {
        return role.get();
    }

    // ── Election mechanics ────────────────────────────────────────────────────

    /**
     * Schedules an election timeout with a random delay in
     * [{@value TIMEOUT_MIN_MS},
     * {@value TIMEOUT_MAX_MS}] ms. Randomisation avoids simultaneous elections
     * (split-vote) – a core Raft safety mechanism.
     */
    private void scheduleElectionTimeout() {
        int delay = ThreadLocalRandom.current().nextInt(TIMEOUT_MIN_MS, TIMEOUT_MAX_MS);
        scheduler.schedule(this::onElectionTimeout, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Called when the election timer fires without receiving a heartbeat.
     * Transitions FOLLOWER → CANDIDATE and starts an election.
     *
     * <p>
     * Uses the {@code stateLock} to ensure the term increment and role
     * transition are atomic – critical because a concurrent heartbeat could
     * otherwise race with the term update.
     */
    private void onElectionTimeout() {
        stateLock.lock();
        try {
            if (role.get() == RaftRole.LEADER) {
                // Already the leader; schedule next heartbeat instead
                scheduleHeartbeat();
                return;
            }
            long newTerm = currentTerm.incrementAndGet();
            role.set(RaftRole.CANDIDATE);
            votedFor = localNodeId; // vote for self
            votesReceived.clear();
            votesReceived.add(localNodeId); // self-vote counts
            log.info("[{}] Election timeout → CANDIDATE, term={}", localNodeId, newTerm);
        } finally {
            stateLock.unlock();
        }
        requestVotesFromPeers();
    }

    /**
     * Sends (simulates) RequestVote RPCs to all known peers.
     *
     * <p>
     * In real Elasticsearch this would use the Netty-based transport layer
     * to send {@code StartJoinRequest} messages asynchronously. Here we call
     * each peer's {@code handleVoteRequest} directly to illustrate the logic.
     */
    private void requestVotesFromPeers() {
        long term = currentTerm.get();
        // In production: send async over Netty transport to each peer.
        // For study purposes, simulate as a direct call:
        peers.values().forEach(peer -> log.debug("[{}] → RequestVote(term={}) to {}", localNodeId, term, peer.id()));
        // Demo: assume we got majority (would be async callbacks in production)
        checkIfElectionWon();
    }

    /**
     * Processes an incoming vote grant from a peer.
     * Called (in a real system) from the Netty I/O thread deserialising the
     * VoteResponse message.
     *
     * @param voterId ID of the node granting the vote.
     * @param term    The term of the response; must match {@code currentTerm}.
     */
    public void onVoteGranted(String voterId, long term) {
        stateLock.lock();
        try {
            if (term != currentTerm.get() || role.get() != RaftRole.CANDIDATE) {
                log.debug("[{}] Stale vote from {} in term {}, ignoring", localNodeId, voterId, term);
                return;
            }
            votesReceived.add(voterId);
            log.info("[{}] Vote received from {} – total votes: {}", localNodeId, voterId, votesReceived.size());
        } finally {
            stateLock.unlock();
        }
        checkIfElectionWon();
    }

    /** Checks whether we have a quorum of votes and transitions to LEADER if so. */
    private void checkIfElectionWon() {
        int totalNodes = peers.size() + 1; // +1 for self
        int quorum = totalNodes / 2 + 1;
        if (votesReceived.size() >= quorum) {
            becomeLeader();
        } else {
            // Didn't win yet – wait for more votes or timeout
            scheduleElectionTimeout();
        }
    }

    /**
     * Transitions this node to LEADER.
     * <ul>
     * <li>Updates the shared {@link ClusterState} with this node as master.</li>
     * <li>Starts sending periodic heartbeats to prevent followers timing out.</li>
     * </ul>
     */
    private void becomeLeader() {
        stateLock.lock();
        try {
            if (role.get() == RaftRole.LEADER)
                return; // already leader (idempotent)
            role.set(RaftRole.LEADER);
            knownLeaderId = localNodeId; // record self as the known leader
            long term = currentTerm.get();
            log.info("[{}] *** ELECTED LEADER *** term={}", localNodeId, term);

            // Publish new cluster state with this node as master
            ClusterState next = clusterStateRef.get().builder()
                    .masterNodeId(localNodeId)
                    .build();
            clusterStateRef.set(next);
        } finally {
            stateLock.unlock();
        }
        scheduleHeartbeat();
    }

    /**
     * Processes an incoming AppendEntries / heartbeat RPC from the leader.
     * Resets the follower's election timer.
     *
     * @param leaderTerm term advertised by the leader
     * @param leaderId   ID of the node claiming leadership
     */
    public void onHeartbeat(long leaderTerm, String leaderId) {
        stateLock.lock();
        try {
            if (leaderTerm > currentTerm.get()) {
                // Leader has a higher term → step down to FOLLOWER
                currentTerm.set(leaderTerm);
                votedFor = null;
                role.set(RaftRole.FOLLOWER);
                log.info("[{}] Higher term {} from leader {}, stepping down to FOLLOWER",
                        localNodeId, leaderTerm, leaderId);
            }
            // Always record the claimed leader – a heartbeat is an implicit
            // leadership claim (Raft §5.2: leaders send periodic heartbeats)
            knownLeaderId = leaderId;
        } finally {
            stateLock.unlock();
        }
        // Reschedule election timeout (heartbeat resets the clock)
        scheduleElectionTimeout();
    }

    /** Sends periodic heartbeats to all followers (leader only). */
    private void scheduleHeartbeat() {
        scheduler.schedule(() -> {
            if (role.get() == RaftRole.LEADER) {
                long term = currentTerm.get();
                log.debug("[{}] → Heartbeat(term={}) to {} peers", localNodeId, term, peers.size());
                // In production: send AppendEntries(empty) via Netty transport
                scheduleHeartbeat(); // reschedule
            }
        }, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Handles an incoming RequestVote from a candidate.
     *
     * @param candidateId   ID of the requesting candidate.
     * @param candidateTerm Term of the candidate.
     * @return {@code true} if we grant the vote.
     */
    public boolean handleVoteRequest(String candidateId, long candidateTerm) {
        stateLock.lock();
        try {
            if (candidateTerm > currentTerm.get()) {
                // Candidate has a newer term – update ours and allow vote
                currentTerm.set(candidateTerm);
                votedFor = null;
                role.set(RaftRole.FOLLOWER);
            }
            boolean grant = (candidateTerm >= currentTerm.get())
                    && (votedFor == null || votedFor.equals(candidateId));
            if (grant)
                votedFor = candidateId;
            log.debug("[{}] VoteRequest from {} term={} → grant={}", localNodeId, candidateId, candidateTerm, grant);
            return grant;
        } finally {
            stateLock.unlock();
        }
    }

    // ── Cluster status helper ─────────────────────────────────────────────────

    /**
     * Returns the ID of the currently known leader, if any.
     *
     * <p>
     * Returns {@link Optional#empty()} when:
     * <ul>
     * <li>The cluster is still forming (no election has concluded yet).</li>
     * <li>This node just restarted and has not yet received a heartbeat.</li>
     * </ul>
     * Using {@link Optional} makes the "no-leader" case explicit in the type
     * system, forcing callers to handle it rather than NPE on a null return.
     *
     * @return Optional containing the leader node ID, or empty if unknown.
     */
    public Optional<String> knownLeaderId() {
        return Optional.ofNullable(knownLeaderId);
    }

    /**
     * Returns the current Raft <em>voting configuration</em>: the sorted,
     * immutable list of all master-eligible node IDs that form the quorum.
     *
     * <p>
     * In Elasticsearch this maps to
     * {@code cluster.voting_configurations.last_accepted_configuration} in
     * cluster state. Changing the voting configuration (adding/removing
     * master-eligible nodes) requires a two-phase joint-consensus commit so
     * that the transition window never allows two independent quorums to form
     * simultaneously (Raft §6 – membership changes).
     *
     * @return Sorted, immutable list of all voter node IDs (peers + local).
     */
    public List<String> votingConfiguration() {
        return java.util.stream.Stream
                .concat(peers.keySet().stream(), java.util.stream.Stream.of(localNodeId))
                .sorted()
                .collect(Collectors.toUnmodifiableList());
    }

    /** Returns a summary of this node's current election state. */
    public String statusSummary() {
        return String.format("node=%s role=%s term=%d votedFor=%s votes=%s",
                localNodeId, role.get(), currentTerm.get(), votedFor,
                votesReceived.stream().sorted().collect(Collectors.joining(",")));
    }

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Raft node roles.
     *
     * <h3>State Transition Summary</h3>
     * <ul>
     * <li>{@code FOLLOWER} → {@code CANDIDATE}: election timer fires without
     * heartbeat</li>
     * <li>{@code CANDIDATE} → {@code LEADER}: receives majority votes</li>
     * <li>{@code CANDIDATE} → {@code FOLLOWER}: discovers higher term or valid
     * leader</li>
     * <li>{@code LEADER} → {@code FOLLOWER}: discovers higher term from any
     * message</li>
     * </ul>
     */
    public enum RaftRole {
        FOLLOWER, CANDIDATE, LEADER
    }

    // ── Optional: quorum calculation utility ─────────────────────────────────

    /**
     * Returns the minimum number of votes (including self) required to elect a
     * leader in a cluster of {@code n} voting nodes.
     *
     * <p>
     * Formula: {@code quorum = ⌊n / 2⌋ + 1}
     * <p>
     * Example: n=3 → quorum=2; n=5 → quorum=3; n=7 → quorum=4.
     *
     * @param n total number of master-eligible nodes
     * @return quorum size
     */
    public static int quorumForCluster(int n) {
        if (n < 1)
            throw new IllegalArgumentException("Cluster must have at least 1 node");
        return n / 2 + 1;
    }

    /**
     * Maximum number of simultaneous node failures a cluster of size {@code n}
     * can tolerate before losing quorum.
     *
     * <p>
     * Formula: {@code faultTolerance = ⌊(n - 1) / 2⌋}
     * <p>
     * Example: n=3 → 1 failure; n=5 → 2 failures.
     */
    public static int faultTolerance(int n) {
        return (n - 1) / 2;
    }
}
