package com.elasticsearch.distributed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A simplified write-ahead translog (transaction log) for a single shard.
 *
 * <h2>Study Notes – Elasticsearch Translog</h2>
 *
 * <p>
 * Every write operation on a shard is appended to the <em>translog</em>
 * <em>before</em> it is acknowledged to the client. The translog is the
 * primary durability mechanism in Elasticsearch.
 *
 * <h3>1. Purpose of the Translog</h3>
 * <ul>
 * <li><b>Crash recovery</b>: Lucene commits (fsync) are expensive and
 * happen infrequently (default: every 5 minutes, or when the translog
 * exceeds 512 MB). Between commits, all changes live only in
 * Lucene's in-memory buffer and in the translog file. On restart,
 * Elasticsearch replays the translog from the last Lucene commit to
 * recover all un-committed operations.</li>
 * <li><b>Replica sync</b>: when a replica lags behind the primary, it
 * performs a "translog-based recovery" by requesting the ops between
 * its local checkpoint and the primary's global checkpoint. This is
 * far cheaper than a full file-based recovery (which streams all
 * Lucene segment files).</li>
 * <li><b>Global checkpoint advancement</b>: the translog is safely
 * truncated up to the global checkpoint – all ISR members have
 * confirmed ops ≤ globalCheckpoint, so those entries are no longer
 * needed for replica recovery.</li>
 * </ul>
 *
 * <h3>2. Fsync Strategy and Durability</h3>
 * <p>
 * The {@code index.translog.durability} setting controls when fsync is
 * called:
 * <ul>
 * <li>{@code request} (default): fsync after <em>every</em> bulk request.
 * Guarantees no data loss on primary failure at the cost of throughput.</li>
 * <li>{@code async}: fsync on a configurable interval (default 5 s).
 * Higher throughput but up to 5 s of data can be lost if the primary
 * node crashes before the next fsync.</li>
 * </ul>
 * This implementation calls
 * {@link FileChannel#force(boolean)} to demonstrate the synchronous path.
 *
 * <h3>3. Translog Generation</h3>
 * <p>
 * When a Lucene commit happens, the current translog file is "closed"
 * (no more writes) and a new <em>generation</em> file is opened. The
 * generation number is stored in the Lucene commit and used to identify
 * which translog files are needed for recovery vs. which can be deleted.
 *
 * <h3>4. On-Disk Format</h3>
 * <p>
 * Real ES translog format:
 * 
 * <pre>
 *   [4-byte header: magic + version]
 *   per-operation frames:
 *     [4-byte total frame length]
 *     [1-byte operation type: INDEX / DELETE / NO_OP]
 *     [8-byte primaryTerm]
 *     [8-byte seqNo]
 *     [4-byte body length]
 *     [body bytes: versioned document or delete marker]
 *     [4-byte CRC32 checksum]
 * </pre>
 * 
 * This implementation uses a simplified frame: {@code seqNo|body\n}.
 *
 * <h3>5. Key Classes in ES Source</h3>
 * <ul>
 * <li>{@code org.elasticsearch.index.translog.Translog} – main entry
 * point.</li>
 * <li>{@code TranslogWriter} – appends ops to the current generation.</li>
 * <li>{@code TranslogReader} – reads ops back for recovery replay.</li>
 * <li>{@code TranslogConfig} – per-index configuration (path, fsync mode).</li>
 * </ul>
 *
 * <h3>Interview Talking Points</h3>
 * <ul>
 * <li>Why does the translog use a CRC per entry? Partial writes can corrupt
 * the trailing entry on crash. CRC lets the reader detect truncation
 * and ignore the incomplete entry safely.</li>
 * <li>What is "translog recovery" vs "peer recovery"? Translog recovery
 * replays only recent ops from the log (fast); peer recovery streams
 * the full Lucene index from another node (used when the local copy is
 * absent or too stale).</li>
 * <li>What happens to the translog when the global checkpoint advances?
 * All translog entries with seqNo ≤ globalCheckpoint are no longer
 * needed for replica recovery. The translog trimmer deletes the
 * corresponding generation files.</li>
 * </ul>
 */
public final class TranslogWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(TranslogWriter.class);

    /** Magic bytes at the start of every translog generation file. */
    private static final int MAGIC = 0x454C5354; // "ELST" in ASCII

    // ── File I/O ────────────────────────────────────────────────────────────

    private final Path translogFile;
    private final FileChannel channel;
    private final ReentrantLock writeLock = new ReentrantLock();

    // ── State ───────────────────────────────────────────────────────────────

    private final long generation;
    private final AtomicLong lastWrittenSeqNo = new AtomicLong(-1);
    private long bytesWritten = 0;

    /** Maximum generation file size before rolling over (simplified threshold). */
    private static final long MAX_BYTES = 512L * 1024 * 1024; // 512 MB

    // ── Durability mode ─────────────────────────────────────────────────────

    /** When true, {@link FileChannel#force} is called after every write. */
    private final boolean syncOnWrite;

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Opens (or creates) a translog generation file for the given shard.
     *
     * @param shardDataDir Directory where the translog files live.
     * @param generation   Monotonically increasing generation number (0, 1, 2, …).
     * @param syncOnWrite  {@code true} for
     *                     {@code index.translog.durability=request}.
     */
    public TranslogWriter(Path shardDataDir, long generation, boolean syncOnWrite)
            throws IOException {
        this.generation = generation;
        this.syncOnWrite = syncOnWrite;
        this.translogFile = shardDataDir.resolve("translog-" + generation + ".tlog");

        Files.createDirectories(shardDataDir);
        this.channel = FileChannel.open(translogFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        // Write magic header for new files
        if (Files.size(translogFile) == 0) {
            writeHeader();
        }
        log.info("TranslogWriter opened: generation={} file={} sync={}", generation, translogFile, syncOnWrite);
    }

    // ── Public API ───────────────────────────────────────────────────────────

    /**
     * Appends an indexing operation to the translog.
     *
     * <p>
     * Frame format (simplified):
     * 
     * <pre>
     *   [8-byte seqNo][8-byte primaryTerm][4-byte body length][body bytes][4-byte CRC32]
     * </pre>
     *
     * @param seqNo       Sequence number assigned by the primary.
     * @param primaryTerm Current primary term.
     * @param docId       Document ID (part of the body for simplicity).
     * @param source      JSON document source.
     * @throws IOException on I/O failure.
     */
    public void append(long seqNo, long primaryTerm, String docId, String source)
            throws IOException {
        byte[] body = buildBody(seqNo, primaryTerm, docId, source);
        int crc = crc32(body);

        // Frame: seqNo(8) + primaryTerm(8) + bodyLen(4) + body + crc(4)
        ByteBuffer frame = ByteBuffer.allocate(8 + 8 + 4 + body.length + 4);
        frame.putLong(seqNo);
        frame.putLong(primaryTerm);
        frame.putInt(body.length);
        frame.put(body);
        frame.putInt(crc);
        frame.flip();

        writeLock.lock();
        try {
            while (frame.hasRemaining()) {
                bytesWritten += channel.write(frame);
            }
            if (syncOnWrite) {
                channel.force(false); // metadata=false: only data pages, not inode
            }
            lastWrittenSeqNo.set(seqNo);
            log.debug("Translog append gen={} seqNo={} term={} doc={} ({} bytes)",
                    generation, seqNo, primaryTerm, docId, body.length);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Forces an fsync of the translog file.
     * Called in {@code async} durability mode on each interval tick, or
     * explicitly before a Lucene commit.
     *
     * @throws IOException on I/O failure.
     */
    public void sync() throws IOException {
        channel.force(false);
        log.debug("Translog synced gen={} bytesWritten={}", generation, bytesWritten);
    }

    /**
     * Trims translog entries with {@code seqNo ≤ globalCheckpoint}.
     *
     * <p>
     * In production ES, old translog <em>generation</em> files are deleted
     * once every seqNo in them is ≤ globalCheckpoint. A single file stores
     * many ops, so trimming is file-granular (delete entire generation).
     * This method is a stub illustrating the concept.
     *
     * @param globalCheckpoint The current cluster global checkpoint.
     */
    public void trimToGlobalCheckpoint(long globalCheckpoint) {
        log.info("Translog trim: gen={} globalCheckpoint={} lastWritten={}",
                generation, globalCheckpoint, lastWrittenSeqNo.get());
        // If the entire generation is ≤ globalCheckpoint AND the generation is
        // not the current one, the file could be deleted.
        if (lastWrittenSeqNo.get() <= globalCheckpoint) {
            log.info("Generation {} is fully below globalCheckpoint – eligible for deletion", generation);
        }
    }

    /** Returns whether this generation has exceeded the size threshold. */
    public boolean shouldRollOver() {
        return bytesWritten >= MAX_BYTES;
    }

    /** Returns the generation number of this writer. */
    public long generation() {
        return generation;
    }

    /** Returns the last seqNo written to this generation. */
    public long lastWrittenSeqNo() {
        return lastWrittenSeqNo.get();
    }

    /**
     * Reads back all operations from a translog generation file starting at
     * {@code startOffset}, using {@link RandomAccessFile} so the caller can
     * seek to an arbitrary position.
     *
     * <p>
     * This is the core of <em>translog-based crash recovery</em>:
     * <ol>
     * <li>On startup, Elasticsearch reads the last Lucene commit's user-data
     * map, which contains the translog generation number and the file
     * offset of the first unprocessed operation.</li>
     * <li>It calls this method (seek to that offset) to skip already-committed
     * ops and replay only the delta since the last commit.</li>
     * <li>Each replayed op is applied to the in-memory Lucene buffer; once
     * all ops are replayed, Elasticsearch performs a new commit and
     * opens the shard for normal operation.</li>
     * </ol>
     *
     * <p>
     * {@link RandomAccessFile#seek(long)} enables direct positioning to the
     * stored offset without reading through the entire file header — critical
     * for large generation files (up to {@value MAX_BYTES} bytes).
     *
     * <p>
     * A CRC mismatch on the trailing entry indicates a partial write caused
     * by a crash mid-frame; that entry is silently dropped (same behaviour as
     * real Elasticsearch's {@code Translog.recoverFromFiles}).
     *
     * @param translogFile Path to the {@code .tlog} generation file.
     * @param startOffset  Byte offset to begin reading; pass {@code 8L} to
     *                     start right after the file header, or the value
     *                     stored in the Lucene commit metadata.
     * @return List of decoded operation strings in write order.
     * @throws IOException on I/O failure.
     */
    public static List<String> readOpsFrom(Path translogFile, long startOffset)
            throws IOException {
        List<String> ops = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(translogFile.toFile(), "r")) {
            raf.seek(startOffset);
            while (raf.getFilePointer() < raf.length()) {
                try {
                    long seqNo = raf.readLong();
                    long primaryTerm = raf.readLong();
                    int bodyLen = raf.readInt();
                    byte[] body = new byte[bodyLen];
                    raf.readFully(body);
                    int storedCrc = raf.readInt();
                    int computedCrc = crc32(body);
                    if (storedCrc != computedCrc) {
                        // CRC mismatch → partial write at end of file (crash during write).
                        // Truncate logical read here; real ES also physically truncates the file.
                        log.warn("CRC mismatch at file offset {} – stopping replay (partial write)",
                                raf.getFilePointer());
                        break;
                    }
                    String op = new String(body, StandardCharsets.UTF_8);
                    ops.add(op);
                    log.debug("Recovery replay: seqNo={} term={} op={}", seqNo, primaryTerm, op);
                } catch (java.io.EOFException eof) {
                    break; // clean end-of-file
                }
            }
        }
        log.info("Translog recovery: replayed {} ops from {} at offset {}",
                ops.size(), translogFile.getFileName(), startOffset);
        return ops;
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            channel.force(true); // final sync including metadata
            channel.close();
            log.info("TranslogWriter closed: generation={} bytesWritten={}", generation, bytesWritten);
        } finally {
            writeLock.unlock();
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    private void writeHeader() throws IOException {
        ByteBuffer header = ByteBuffer.allocate(8);
        header.putInt(MAGIC);
        header.putInt(1); // version 1
        header.flip();
        channel.write(header);
        bytesWritten += 8;
    }

    private static byte[] buildBody(long seqNo, long primaryTerm, String docId, String source) {
        String entry = seqNo + "|" + primaryTerm + "|" + docId + "|" + source;
        return entry.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Computes a simple CRC-32 checksum so readers can detect partial writes.
     * Real ES uses {@code java.util.zip.CRC32} on the full frame buffer.
     */
    private static int crc32(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
