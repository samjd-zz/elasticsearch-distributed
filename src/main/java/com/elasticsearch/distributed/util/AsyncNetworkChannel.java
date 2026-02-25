package com.elasticsearch.distributed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Demonstrates Netty-style async event-driven networking patterns for
 * inter-node communication in an Elasticsearch cluster.
 *
 * <h2>Study Notes – Netty in Elasticsearch</h2>
 *
 * <p>
 * Elasticsearch uses <b>Netty 4</b> as its internal transport layer for
 * all inter-node communication: cluster state publishing, shard recovery,
 * cross-shard search, and bulk indexing replication. Understanding Netty's
 * programming model is a strong bonus for this role.
 *
 * <h3>1. Netty's Event-Driven Model</h3>
 * 
 * <pre>
 *   ┌─────────────────────────────────────────┐
 *   │            EventLoopGroup               │
 *   │   ┌──────────┐  ┌──────────┐            │
 *   │   │EventLoop │  │EventLoop │  …         │
 *   │   │(1 thread)│  │(1 thread)│            │
 *   │   └────┬─────┘  └────┬─────┘            │
 *   └────────┼─────────────┼──────────────────┘
 *            │             │
 *   ┌────────▼─────────────▼────────────────┐
 *   │          ChannelPipeline              │
 *   │  Decoder → Handler → Encoder         │
 *   └───────────────────────────────────────┘
 * </pre>
 * <ul>
 * <li><b>EventLoopGroup</b>: a pool of single-threaded event loops. Each
 * loop handles I/O events for a subset of channels. Default size:
 * {@code 2 × CPU cores}.</li>
 * <li><b>ChannelPipeline</b>: an ordered sequence of
 * {@code ChannelHandler}s. Inbound frames pass through decoders
 * (length-field framing → protobuf/custom decode) then reach the
 * application handler. Outbound messages travel the reverse path.</li>
 * <li><b>ChannelFuture / ChannelPromise</b>: Netty's equivalent of
 * {@link CompletableFuture}. Never block the event loop waiting on
 * these – attach a listener instead.</li>
 * </ul>
 *
 * <h3>2. ES Transport Layer</h3>
 * <p>
 * Key classes in {@code server/src/main/java/org/elasticsearch/transport/}:
 * <ul>
 * <li>{@code Netty4Transport} – the Netty-based implementation of
 * {@code Transport} interface.</li>
 * <li>{@code TcpChannel} – wraps a Netty {@code Channel} with ES
 * lifecycle (connect, close, bytes-written stats).</li>
 * <li>{@code OutboundHandler} – serialises {@code TransportRequest}
 * objects into wire frames and hands them to Netty for sending.</li>
 * <li>{@code InboundHandler} – parses incoming frames, dispatches to
 * the registered action handler (analogous to RPC registration).</li>
 * </ul>
 *
 * <h3>3. Wire Protocol Frame Layout</h3>
 * 
 * <pre>
 *   [4-byte marker: 'ES']
 *   [4-byte total message length]
 *   [8-byte request id]
 *   [1-byte status byte: request/response/error/compress flags]
 *   [4-byte version]
 *   [variable: action name (UTF-8)]
 *   [variable: serialised request/response body]
 * </pre>
 *
 * <h3>4. Backpressure and Memory Management</h3>
 * <p>
 * Elasticsearch uses Netty's {@code ByteBufAllocator} (pooled by default)
 * plus a custom {@code PageCacheRecycler} to avoid GC pressure on large
 * data streams (shard recovery, bulk indexing). Understanding netty's
 * reference counting ({@code ReferenceCounted}) is important; mishandling
 * reference counts causes memory leaks or double-free crashes.
 *
 * <h3>5. Async Request Pattern (studied here)</h3>
 * <p>
 * ES transport actions follow a request/response correlation pattern:
 * <ol>
 * <li>Sender assigns a unique {@code requestId} and stores a
 * {@code ResponseHandler} in a map, then fires-and-forgets the frame.</li>
 * <li>Remote side deserialises, processes, and sends back a response with
 * the same {@code requestId}.</li>
 * <li>Sender's Netty I/O thread receives the response frame, looks up
 * the {@code requestId} in the correlation map, and completes the
 * associated future/callback.</li>
 * <li>A timeout scheduled on the event loop cancels the future and removes
 * the entry from the correlation map if no response arrives in time.</li>
 * </ol>
 *
 * <h3>Interview Talking Points</h3>
 * <ul>
 * <li>Why does ES use Netty instead of Java NIO directly? Netty handles
 * edge cases in NIO (Selector wakeup bugs, epoll level vs edge trigger),
 * provides pooled buffer allocation, and has a mature pipeline model.</li>
 * <li>What is the "event loop pollution" anti-pattern? Blocking inside an
 * event-loop thread (e.g. calling a synchronous REST endpoint from a
 * Netty handler) starves other channels. ES solves this by dispatching
 * CPU-bound work to a thread pool ({@code ThreadPool.Names.SEARCH}, etc.)
 * and returning the result via a callback on the transport thread.</li>
 * <li>How does Elasticsearch handle backpressure? Netty's
 * {@code Channel.isWritable()} / high-water-mark mechanism signals when
 * the outbound buffer is full. ES checks this before sending and may
 * delay bulk requests to avoid OOMing the heap.</li>
 * </ul>
 */
public final class AsyncNetworkChannel {

    private static final Logger log = LoggerFactory.getLogger(AsyncNetworkChannel.class);

    /** Default request timeout in milliseconds. */
    private static final long DEFAULT_TIMEOUT_MS = 30_000;

    // ── Correlation map: requestId → pending response handler ────────────────
    // Simulates the OutboundLookup / PendingResponseHandlers map in ES transport.
    private final ConcurrentHashMap<Long, PendingRequest<?>> pendingRequests = new ConcurrentHashMap<>();

    private final AtomicLong requestIdGenerator = new AtomicLong(0);

    /** Simulates Netty's single-threaded event loop for timeouts. */
    private final ScheduledExecutorService eventLoop = Executors.newSingleThreadScheduledExecutor(r -> {
        var t = new Thread(r, "netty-event-loop-sim");
        t.setDaemon(true);
        return t;
    });

    private final String localNodeId;

    public AsyncNetworkChannel(String localNodeId) {
        this.localNodeId = localNodeId;
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Sends a request to a remote node and returns a {@link CompletableFuture}
     * that completes with the response (or fails on timeout/error).
     *
     * <p>
     * Mirrors the pattern in {@code TransportService#sendRequest}:
     * <ol>
     * <li>Generate a unique {@code requestId}.</li>
     * <li>Register a pending response holder in the correlation map.</li>
     * <li>Serialise and send the request over the Netty channel (simulated).</li>
     * <li>Schedule a timeout callback on the event loop.</li>
     * </ol>
     *
     * @param remoteNodeId Target node.
     * @param action       ES transport action name (e.g.
     *                     "indices:data/write/bulk[s]").
     * @param request      Request payload.
     * @param timeoutMs    Request timeout in milliseconds.
     * @return Future that completes with the response string.
     */
    public CompletableFuture<String> sendRequest(
            String remoteNodeId, String action, String request, long timeoutMs) {

        long requestId = requestIdGenerator.incrementAndGet();
        CompletableFuture<String> future = new CompletableFuture<>();
        PendingRequest<String> pending = new PendingRequest<>(requestId, action, future);
        pendingRequests.put(requestId, pending);

        log.debug("[{}] → sendRequest(id={} action={} to={})", localNodeId, requestId, action, remoteNodeId);

        // Simulate async send over the Netty channel (fire and forget)
        // In production: channel.writeAndFlush(serialise(requestId, action, request))
        // .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);

        // Schedule timeout (mirrors Netty's hashedWheelTimer / scheduledTask)
        eventLoop.schedule(() -> {
            PendingRequest<?> p = pendingRequests.remove(requestId);
            if (p != null) {
                log.warn("[{}] Request timed out: id={} action={} to={}",
                        localNodeId, requestId, action, remoteNodeId);
                p.future().completeExceptionally(
                        new TimeoutException("Request " + requestId + " timed out after " + timeoutMs + " ms"));
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);

        return future;
    }

    /**
     * Convenience overload with the default timeout.
     */
    public CompletableFuture<String> sendRequest(String remoteNodeId, String action, String request) {
        return sendRequest(remoteNodeId, action, request, DEFAULT_TIMEOUT_MS);
    }

    /**
     * Called by the Netty I/O thread when a response frame arrives from a
     * remote node. Completes the associated future and cleans up the
     * correlation map entry.
     *
     * @param requestId Correlation ID from the wire frame.
     * @param response  Decoded response payload.
     */
    @SuppressWarnings("unchecked")
    public void onResponseReceived(long requestId, String response) {
        PendingRequest<?> pending = pendingRequests.remove(requestId);
        if (pending == null) {
            // Already timed out or duplicate response – safe to ignore
            log.debug("Received response for unknown/expired requestId={}", requestId);
            return;
        }
        log.debug("[{}] ← response(id={} action={})", localNodeId, requestId, pending.action());
        ((PendingRequest<String>) pending).future().complete(response);
    }

    /**
     * Called by the Netty I/O thread when an error frame or channel exception
     * arrives.
     *
     * @param requestId Correlation ID.
     * @param cause     Root cause exception.
     */
    public void onError(long requestId, Exception cause) {
        PendingRequest<?> pending = pendingRequests.remove(requestId);
        if (pending != null) {
            pending.future().completeExceptionally(cause);
        }
    }

    /**
     * Registers a server-side action handler.
     *
     * <p>
     * Mirrors {@code TransportService#registerRequestHandler}. The handler
     * is invoked on a thread-pool thread (not the event-loop thread), so
     * blocking I/O is acceptable inside it.
     *
     * @param action  Action name.
     * @param handler {@code Consumer<String>} to process incoming requests.
     */
    public void registerHandler(String action, Consumer<String> handler) {
        log.info("[{}] Registered handler for action: {}", localNodeId, action);
        // In production: transport.registerRequestHandler(action,
        // threadPool.executor(Names.WRITE), ...)
    }

    /**
     * Returns the number of in-flight requests (useful for backpressure checks).
     */
    public int inFlightRequests() {
        return pendingRequests.size();
    }

    public void shutdown() {
        eventLoop.shutdownNow();
        // Cancel all pending futures
        pendingRequests.forEach((id, pending) -> pending.future().completeExceptionally(
                new IllegalStateException("Channel shutting down")));
        pendingRequests.clear();
    }

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Tracks a sent request pending a response.
     *
     * @param requestId Unique correlation ID.
     * @param action    ES transport action name.
     * @param future    Future to complete when the response arrives.
     */
    private record PendingRequest<T>(long requestId, String action, CompletableFuture<T> future) {
    }
}
