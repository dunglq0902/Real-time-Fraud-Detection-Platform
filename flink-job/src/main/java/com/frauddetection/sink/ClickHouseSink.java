package com.frauddetection.sink;

import com.frauddetection.model.DecisionResult;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ClickHouse Batch Sink with Flink Checkpoint integration and async flush.
 *
 * <p>
 * <b>Checkpoint-Safe Design:</b> The buffer is NOT part of the Flink operator's
 * regular processing state. This implementation uses
 * {@link CheckpointedFunction} to:
 * <ul>
 * <li>{@code snapshotState}: persist buffer + inflight records into
 * ListState</li>
 * <li>{@code initializeState}: restore buffer from ListState on recovery</li>
 * </ul>
 *
 * <p>
 * <b>Latency Fix:</b> HTTP flush now runs on a dedicated background thread
 * instead of blocking the Flink operator thread. This prevents backpressure
 * from propagating upstream when ClickHouse is slow or unresponsive.
 * {@code snapshotState()} no longer calls {@code flushBatch()}, avoiding
 * checkpoint stalls from ClickHouse I/O.
 *
 * <p>
 * Combined with ClickHouse ReplacingMergeTree(decided_at), this achieves
 * near exactly-once semantics (duplicates from retry are auto-deduplicated).
 */
public class ClickHouseSink extends RichSinkFunction<DecisionResult> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseSink.class);

    private static final DateTimeFormatter DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final String tableName;
    private final int maxBatchSize;
    private final long flushIntervalMs;
    private final String clickhouseHost;
    private final int clickhousePort;
    private final String clickhouseUser;
    private final String clickhousePassword;

    /** In-memory buffer — flushed to ClickHouse on batch full or time trigger. */
    private transient List<DecisionResult> buffer;
    private transient long lastFlushTime;
    private transient long totalInserted;

    /** Checkpoint state — used to persist buffer across failures. */
    private transient ListState<DecisionResult> checkpointState;

    /**
     * Dedicated flush thread — HTTP I/O runs here, not on the Flink operator
     * thread.
     */
    private transient ExecutorService flushExecutor;

    /** Guard to prevent overlapping flushes (thread-safe CAS). */
    private transient AtomicBoolean flushing;

    /** Records currently being flushed — tracked for checkpoint safety. */
    private transient volatile List<DecisionResult> inflightRecords;

    public ClickHouseSink(String tableName, int maxBatchSize, long flushIntervalMs,
            String host, int port, String user, String password) {
        this.tableName = tableName;
        this.maxBatchSize = maxBatchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.clickhouseHost = host;
        this.clickhousePort = port;
        this.clickhouseUser = user;
        this.clickhousePassword = password;
    }

    // ── SinkFunction ──────────────────────────────────────

    @Override
    public void invoke(DecisionResult value, Context context) throws Exception {
        synchronized (this) {
            if (buffer == null) {
                buffer = new ArrayList<>();
                lastFlushTime = System.currentTimeMillis();
            }
            buffer.add(value);
        }

        long now = System.currentTimeMillis();
        boolean shouldFlush;
        synchronized (this) {
            shouldFlush = buffer.size() >= maxBatchSize
                    || (now - lastFlushTime) >= flushIntervalMs;
        }

        if (shouldFlush) {
            flushBatchAsync();
        }
    }

    // ── CheckpointedFunction ──────────────────────────────

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Persist buffer to checkpoint state WITHOUT flushing to ClickHouse.
        // This avoids blocking the checkpoint with synchronous HTTP I/O.
        // ClickHouse uses ReplacingMergeTree so duplicate inserts after recovery
        // are automatically deduplicated.
        checkpointState.clear();
        synchronized (this) {
            if (buffer != null && !buffer.isEmpty()) {
                for (DecisionResult record : buffer) {
                    checkpointState.add(record);
                }
            }
        }
        // Also persist inflight records (currently being flushed by background thread)
        // to prevent data loss if crash occurs during HTTP flush.
        List<DecisionResult> inflight = this.inflightRecords;
        if (inflight != null) {
            for (DecisionResult record : inflight) {
                checkpointState.add(record);
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<DecisionResult> descriptor = new ListStateDescriptor<>(
                "clickhouse-sink-buffer",
                TypeInformation.of(DecisionResult.class));
        checkpointState = context.getOperatorStateStore().getListState(descriptor);

        buffer = new ArrayList<>();
        lastFlushTime = System.currentTimeMillis();
        totalInserted = 0;
        flushing = new AtomicBoolean(false);
        inflightRecords = null;

        // Single-thread executor for async HTTP flush
        flushExecutor = Executors.newSingleThreadExecutor(
                r -> {
                    Thread t = new Thread(r, "ch-flush-" + tableName);
                    t.setDaemon(true);
                    return t;
                });

        // Restore buffer from checkpoint on recovery
        if (context.isRestored()) {
            int restoredCount = 0;
            for (DecisionResult record : checkpointState.get()) {
                buffer.add(record);
                restoredCount++;
            }
            if (restoredCount > 0) {
                LOG.info("Restored {} records from checkpoint state", restoredCount);
            }
        }
    }

    // ── Async Batch Flush ─────────────────────────────────

    /**
     * Submit a flush task to the background executor.
     * Takes a snapshot of the current buffer and clears it immediately,
     * so the operator thread is never blocked by HTTP I/O.
     */
    private void flushBatchAsync() {
        if (!flushing.compareAndSet(false, true)) {
            return; // Previous flush still in progress — atomic check-and-set
        }

        final List<DecisionResult> snapshot;
        synchronized (this) {
            if (buffer == null || buffer.isEmpty()) {
                flushing.set(false);
                return;
            }
            snapshot = new ArrayList<>(buffer);
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();
        }

        // Track inflight records so snapshotState() can persist them for checkpoint
        // safety.
        this.inflightRecords = snapshot;

        flushExecutor.submit(() -> {
            try {
                doHttpFlush(snapshot);
                totalInserted += snapshot.size();
                if (totalInserted % 2000 == 0) {
                    LOG.info("ClickHouse [{}]: {} records inserted", tableName, totalInserted);
                }
            } catch (Exception e) {
                LOG.error("Async ClickHouse flush error: {}", e.getMessage());
                // Re-add failed records for retry, but cap to prevent unbounded growth
                synchronized (ClickHouseSink.this) {
                    if (buffer.size() + snapshot.size() <= maxBatchSize * 3) {
                        buffer.addAll(0, snapshot);
                    } else {
                        LOG.warn("Dropped {} records due to persistent ClickHouse errors", snapshot.size());
                    }
                }
            } finally {
                inflightRecords = null;
                flushing.set(false);
            }
        });
    }

    /**
     * Perform the actual HTTP POST to ClickHouse. Runs on the flush executor
     * thread.
     */
    private void doHttpFlush(List<DecisionResult> records) throws Exception {
        StringBuilder body = new StringBuilder();
        for (DecisionResult record : records) {
            body.append(toJsonRow(record)).append("\n");
        }

        String query = "INSERT INTO " + tableName + " FORMAT JSONEachRow";
        String urlStr = String.format("http://%s:%d/?query=%s&user=%s&password=%s",
                clickhouseHost, clickhousePort,
                URLEncoder.encode(query, StandardCharsets.UTF_8.name()),
                clickhouseUser, clickhousePassword);

        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.toString().getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = conn.getResponseCode();
        if (responseCode != 200) {
            throw new RuntimeException("ClickHouse flush failed with HTTP status " + responseCode);
        }

        conn.disconnect();
    }

    /**
     * Convert a DecisionResult to a JSON row string for ClickHouse JSONEachRow
     * format.
     */
    private String toJsonRow(DecisionResult r) {
        return String.format(
                "{\"transaction_id\":\"%s\",\"account_id\":\"%s\",\"card_id\":\"%s\"," +
                        "\"event_time\":\"%s\",\"amount\":%.2f,\"event_type\":\"%s\"," +
                        "\"channel\":\"%s\",\"latitude\":%.6f,\"longitude\":%.6f," +
                        "\"merchant_id\":\"%s\",\"status\":\"%s\",\"ml_score\":%.6f," +
                        "\"rule_triggered\":\"%s\",\"is_fraud\":%d,\"ground_truth_is_fraud\":%d,\"decision\":\"%s\"," +
                        "\"decision_source\":\"%s\",\"combined_score\":%.6f," +
                        "\"produced_at\":\"%s\",\"decided_at\":\"%s\"}",
                safe(r.getTransactionId()), safe(r.getAccountId()), safe(r.getCardId()),
                msToDateTime(r.getEventTime()), r.getAmount(), safe(r.getEventType()),
                safe(r.getChannel()), r.getLatitude(), r.getLongitude(),
                safe(r.getMerchantId()), safe(r.getStatus()), r.getMlScore(),
                safe(r.getRuleTriggered()), r.getIsFraud(), r.getGroundTruthIsFraud(), safe(r.getDecision()),
                safe(r.getDecisionSource()), r.getCombinedScore(),
                msToDateTime(r.getProducedAt()), msToDateTime(r.getDecidedAt()));
    }

    private static String msToDateTime(long ms) {
        if (ms <= 0) {
            return Instant.now().atZone(ZoneOffset.UTC).format(DT_FORMATTER);
        }
        return Instant.ofEpochMilli(ms).atZone(ZoneOffset.UTC).format(DT_FORMATTER);
    }

    private static String safe(String s) {
        if (s == null)
            return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    /**
     * Shutdown the flush executor gracefully.
     */
    @Override
    public void close() throws Exception {
        if (flushExecutor != null && !flushExecutor.isShutdown()) {
            // Try to flush remaining buffer before shutdown
            synchronized (this) {
                if (buffer != null && !buffer.isEmpty()) {
                    try {
                        doHttpFlush(new ArrayList<>(buffer));
                        buffer.clear();
                    } catch (Exception e) {
                        LOG.warn("Final flush failed during close: {}", e.getMessage());
                    }
                }
            }
            flushExecutor.shutdown();
            if (!flushExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }
        }
    }
}
