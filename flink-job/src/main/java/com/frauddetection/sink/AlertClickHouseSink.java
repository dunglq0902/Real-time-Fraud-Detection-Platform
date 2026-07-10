package com.frauddetection.sink;

import com.frauddetection.model.FraudAlert;

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
 * ClickHouse sink for fraud_alerts table with async flush.
 *
 * <p>Same checkpoint-safe pattern as {@link ClickHouseSink}, with non-blocking
 * HTTP flush on a dedicated thread. {@code snapshotState()} persists buffer
 * and inflight records to checkpoint state without attempting a flush,
 * preventing checkpoint stalls.
 *
 * <p><b>Latency Fix:</b> HTTP timeout reduced from 10s to 3s. Flush runs
 * on a background thread instead of blocking the operator.
 */
public class AlertClickHouseSink extends RichSinkFunction<FraudAlert> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AlertClickHouseSink.class);

    private static final DateTimeFormatter DT_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final String clickhouseHost;
    private final int clickhousePort;
    private final String clickhouseUser;
    private final String clickhousePassword;

    private transient List<FraudAlert> buffer;
    private transient long lastFlushTime;
    private transient ListState<FraudAlert> checkpointState;

    /** Dedicated flush thread — HTTP I/O runs here, not on the Flink operator thread. */
    private transient ExecutorService flushExecutor;

    /** Guard to prevent overlapping flushes (thread-safe CAS). */
    private transient AtomicBoolean flushing;

    /** Records currently being flushed — tracked for checkpoint safety. */
    private transient volatile List<FraudAlert> inflightRecords;

    private static final int MAX_BATCH_SIZE = 100;
    private static final long FLUSH_INTERVAL_MS = 3000;

    public AlertClickHouseSink(String host, int port, String user, String password) {
        this.clickhouseHost = host;
        this.clickhousePort = port;
        this.clickhouseUser = user;
        this.clickhousePassword = password;
    }

    @Override
    public void invoke(FraudAlert value, Context context) throws Exception {
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
            shouldFlush = buffer.size() >= MAX_BATCH_SIZE
                    || (now - lastFlushTime) >= FLUSH_INTERVAL_MS;
        }

        if (shouldFlush) {
            flushBatchAsync();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Persist buffer to checkpoint state WITHOUT flushing to ClickHouse.
        // This avoids blocking the checkpoint with synchronous HTTP I/O.
        checkpointState.clear();
        synchronized (this) {
            if (buffer != null && !buffer.isEmpty()) {
                for (FraudAlert alert : buffer) {
                    checkpointState.add(alert);
                }
            }
        }
        // Also persist inflight records (currently being flushed by background thread)
        List<FraudAlert> inflight = this.inflightRecords;
        if (inflight != null) {
            for (FraudAlert alert : inflight) {
                checkpointState.add(alert);
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<FraudAlert> descriptor = new ListStateDescriptor<>(
                "alert-clickhouse-buffer",
                TypeInformation.of(FraudAlert.class)
        );
        checkpointState = context.getOperatorStateStore().getListState(descriptor);

        buffer = new ArrayList<>();
        lastFlushTime = System.currentTimeMillis();
        flushing = new AtomicBoolean(false);
        inflightRecords = null;

        // Single-thread executor for async HTTP flush
        flushExecutor = Executors.newSingleThreadExecutor(
                r -> {
                    Thread t = new Thread(r, "ch-flush-alerts");
                    t.setDaemon(true);
                    return t;
                });

        if (context.isRestored()) {
            for (FraudAlert alert : checkpointState.get()) {
                buffer.add(alert);
            }
        }
    }

    // ── Async Batch Flush ─────────────────────────────────

    /**
     * Submit a flush task to the background executor.
     */
    private void flushBatchAsync() {
        if (!flushing.compareAndSet(false, true)) {
            return;
        }

        final List<FraudAlert> snapshot;
        synchronized (this) {
            if (buffer == null || buffer.isEmpty()) {
                flushing.set(false);
                return;
            }
            snapshot = new ArrayList<>(buffer);
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();
        }

        // Track inflight records so snapshotState() can persist them for checkpoint safety.
        this.inflightRecords = snapshot;

        flushExecutor.submit(() -> {
            try {
                doHttpFlush(snapshot);
            } catch (Exception e) {
                LOG.error("Async Alert ClickHouse flush error: {}", e.getMessage());
                synchronized (AlertClickHouseSink.this) {
                    if (buffer.size() + snapshot.size() <= 500) {
                        buffer.addAll(0, snapshot);
                    } else {
                        LOG.warn("Dropped {} alert records due to persistent errors", snapshot.size());
                    }
                }
            } finally {
                inflightRecords = null;
                flushing.set(false);
            }
        });
    }

    /**
     * Perform the actual HTTP POST to ClickHouse. Runs on the flush executor thread.
     */
    private void doHttpFlush(List<FraudAlert> records) throws Exception {
        StringBuilder body = new StringBuilder();
        for (FraudAlert a : records) {
            body.append(toJsonRow(a)).append("\n");
        }

        String query = "INSERT INTO fraud_detection.fraud_alerts FORMAT JSONEachRow";
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
            throw new RuntimeException("Alert ClickHouse flush failed: HTTP " + responseCode);
        }

        conn.disconnect();
    }

    private String toJsonRow(FraudAlert a) {
        return String.format(
                "{\"alert_id\":\"%s\",\"transaction_id\":\"%s\",\"account_id\":\"%s\"," +
                "\"alert_type\":\"%s\",\"alert_source\":\"%s\",\"severity\":\"%s\"," +
                "\"score\":%.4f,\"details\":\"%s\",\"matched_events\":\"%s\"," +
                "\"event_time\":\"%s\"}",
                safe(a.getAlertId()), safe(a.getTransactionId()), safe(a.getAccountId()),
                safe(a.getAlertType()), safe(a.getAlertSource()), safe(a.getSeverity()),
                a.getScore(), safe(a.getDetails()), safe(a.getMatchedEvents()),
                msToDateTime(a.getEventTime())
        );
    }

    private static String msToDateTime(long ms) {
        if (ms <= 0) {
            return Instant.now().atZone(ZoneOffset.UTC).format(DT_FORMATTER);
        }
        return Instant.ofEpochMilli(ms).atZone(ZoneOffset.UTC).format(DT_FORMATTER);
    }

    private static String safe(String s) {
        if (s == null) return "";
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
            synchronized (this) {
                if (buffer != null && !buffer.isEmpty()) {
                    try {
                        doHttpFlush(new ArrayList<>(buffer));
                        buffer.clear();
                    } catch (Exception e) {
                        LOG.warn("Final alert flush failed during close: {}", e.getMessage());
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
