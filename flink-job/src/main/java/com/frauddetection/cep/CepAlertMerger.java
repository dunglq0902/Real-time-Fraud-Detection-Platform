package com.frauddetection.cep;

import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.Transaction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Merges CEP alerts back into the main transaction stream with synchronization.
 *
 * <p>
 * <b>Fix for Race Condition:</b> Because the main transaction stream reaches
 * this operator faster than the CEP branch (which performs pattern matching), a
 * transaction could be emitted as APPROVE before its CEP alert arrives.
 *
 * <p>
 * <b>Solution — Buffer & Delay with Timer:</b>
 * <ol>
 * <li>{@code processElement1}: Buffer each transaction in {@code MapState} and
 * register a processing-time timer ({@value #BUFFER_DELAY_MS}ms)</li>
 * <li>{@code processElement2}: When a CEP alert arrives, look up its matching
 * buffered transaction by {@code transactionId} and attach the alert directly.
 * If the transaction has already been emitted, generate a dummy LATE_ALERT transaction.
 * If not yet buffered, store in orphanAlerts.</li>
 * <li>{@code onTimer}: Emit all transactions that have waited ≥
 * {@value #BUFFER_DELAY_MS}ms and mark them as emitted in emittedTxIds.</li>
 * </ol>
 *
 * <p>
 * <b>Performance fix:</b> Orphan alerts now use {@code MapState<String, FraudAlert>}
 * keyed by transactionId (O(1) lookup) with TTL=30s auto-cleanup.
 * All states have TTL to prevent memory leaks.
 */
public class CepAlertMerger extends KeyedCoProcessFunction<String, Transaction, FraudAlert, Transaction> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CepAlertMerger.class);

    /**
     * How long to hold a transaction before emitting, giving CEP time to catch up.
     * Reduced from 1000ms → 200ms → 50ms → 10ms — orphan alert mechanism handles
     * late arrivals reliably, and lower delay reduces cascading latency under
     * backpressure. At 500 events/s, 50ms holds ~25 transactions worth of delay;
     * 10ms reduces this to ~5 while still catching most CEP alerts in-time.
     */
    private static final long BUFFER_DELAY_MS = 10;

    /** Buffered transactions: transactionId → Transaction. TTL = 60s. */
    private transient MapState<String, Transaction> bufferedTxns;

    /**
     * Timer map: triggerTimestamp (ms) → comma-separated list of transactionIds.
     * TTL = 60s.
     */
    private transient MapState<Long, String> timerTxIds;

    /**
     * Orphan alerts: "txId|alertType" → FraudAlert. TTL = 30s for auto-cleanup.
     * Uses composite key to support multiple alerts per transaction.
     */
    private transient MapState<String, FraudAlert> orphanAlerts;

    /**
     * Secondary index for orphan alerts: transactionId → comma-separated orphan keys.
     * Enables O(1) lookup in processElement1 instead of scanning all orphan entries.
     * TTL = 30s (same as orphanAlerts).
     */
    private transient MapState<String, String> orphanIndex;

    /** Emitted transactions: transactionId → Transaction. TTL = 60s. */
    private transient MapState<String, Transaction> emittedTxns;

    @Override
    public void open(Configuration parameters) {
        // TTL config for buffered transactions and timer state (60s safety net)
        StateTtlConfig bufferTtl = StateTtlConfig.newBuilder(Time.seconds(60))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupIncrementally(10, true)
                .build();

        // TTL config for orphan alerts (30s — alerts older than this are stale)
        StateTtlConfig orphanTtl = StateTtlConfig.newBuilder(Time.seconds(30))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupIncrementally(10, true)
                .build();

        MapStateDescriptor<String, Transaction> txDesc = new MapStateDescriptor<>(
                "cep-buffered-txns",
                TypeInformation.of(String.class),
                TypeInformation.of(Transaction.class));
        txDesc.enableTimeToLive(bufferTtl);
        bufferedTxns = getRuntimeContext().getMapState(txDesc);

        MapStateDescriptor<Long, String> timerDesc = new MapStateDescriptor<>(
                "cep-timer-txids",
                Types.LONG,
                Types.STRING);
        timerDesc.enableTimeToLive(bufferTtl);
        timerTxIds = getRuntimeContext().getMapState(timerDesc);

        MapStateDescriptor<String, FraudAlert> orphanDesc = new MapStateDescriptor<>(
                "cep-orphan-alerts",
                TypeInformation.of(String.class),
                TypeInformation.of(FraudAlert.class));
        orphanDesc.enableTimeToLive(orphanTtl);
        orphanAlerts = getRuntimeContext().getMapState(orphanDesc);

        MapStateDescriptor<String, String> orphanIdxDesc = new MapStateDescriptor<>(
                "cep-orphan-index",
                Types.STRING,
                Types.STRING);
        orphanIdxDesc.enableTimeToLive(orphanTtl);
        orphanIndex = getRuntimeContext().getMapState(orphanIdxDesc);

        MapStateDescriptor<String, Transaction> emittedDesc = new MapStateDescriptor<>(
                "cep-emitted-txns",
                TypeInformation.of(String.class),
                TypeInformation.of(Transaction.class));
        emittedDesc.enableTimeToLive(bufferTtl);
        emittedTxns = getRuntimeContext().getMapState(emittedDesc);
    }

    // ── Main stream: buffer transaction and wait for CEP ─────
    @Override
    public void processElement1(Transaction tx, Context ctx, Collector<Transaction> out) throws Exception {
        String txId = tx.getTransactionId();

        // O(1) orphan lookup using secondary index
        List<FraudAlert> existing = tx.getCepAlerts() != null ? new ArrayList<>(tx.getCepAlerts()) : new ArrayList<>();
        boolean foundOrphan = false;
        String orphanKeys = orphanIndex.get(txId);
        if (orphanKeys != null) {
            for (String orphanKey : orphanKeys.split(",")) {
                FraudAlert orphan = orphanAlerts.get(orphanKey);
                if (orphan != null) {
                    existing.add(orphan);
                    orphanAlerts.remove(orphanKey);
                    foundOrphan = true;
                    LOG.debug("Pre-attached orphan alert [{}] to tx {}", orphan.getAlertType(), txId);
                }
            }
            orphanIndex.remove(txId);
        }
        if (foundOrphan) {
            tx.setCepAlerts(existing);
            tx.setCepFlagged(true);
        }

        // Buffer the transaction
        bufferedTxns.put(txId, tx);
        long triggerTime = ctx.timerService().currentProcessingTime() + BUFFER_DELAY_MS;

        String existingIds = timerTxIds.get(triggerTime);
        if (existingIds == null) {
            timerTxIds.put(triggerTime, txId);
        } else {
            timerTxIds.put(triggerTime, existingIds + "," + txId);
        }

        // Register a processing-time timer to emit after delay
        ctx.timerService().registerProcessingTimeTimer(triggerTime);
    }

    // ── CEP stream: attach alert to buffered transaction ─────
    @Override
    public void processElement2(FraudAlert alert, Context ctx, Collector<Transaction> out) throws Exception {
        String alertTxId = alert.getTransactionId();

        // Try to attach to the buffered transaction
        Transaction tx = bufferedTxns.get(alertTxId);
        if (tx != null) {
            if (hasAlertType(tx, alert)) {
                return;
            }
            // Found it! Attach the alert directly to the correct transaction
            List<FraudAlert> alerts = tx.getCepAlerts() != null ? new ArrayList<>(tx.getCepAlerts())
                    : new ArrayList<>();
            alerts.add(alert);
            tx.setCepAlerts(alerts);
            tx.setCepFlagged(true);
            bufferedTxns.put(alertTxId, tx); // update in state
            LOG.debug("Attached CEP alert [{}] to buffered tx {}", alert.getAlertType(), alertTxId);
        } else if (emittedTxns.contains(alertTxId)) {
            // Transaction already emitted -> retrieve the original transaction
            Transaction origTx = emittedTxns.get(alertTxId);
            if (origTx != null) {
                if (hasAlertType(origTx, alert)) {
                    return;
                }
                emitLateAlert(origTx, alert, out);
            }
        } else {
            // Transaction not yet buffered -> store as orphan (TTL = 30s)
            // Use composite key txId|alertType to avoid overwriting multiple alerts
            String orphanKey = alertTxId + "|" + alert.getAlertType();
            orphanAlerts.put(orphanKey, alert);
            // Update secondary index for O(1) lookup in processElement1
            String existingKeys = orphanIndex.get(alertTxId);
            if (existingKeys == null || !containsKey(existingKeys, orphanKey)) {
                orphanIndex.put(alertTxId,
                        existingKeys == null ? orphanKey : existingKeys + "," + orphanKey);
            }
            LOG.debug("Buffered orphan CEP alert [{}] for tx {}", alert.getAlertType(), alertTxId);
        }
    }

    private void emitLateAlert(Transaction origTx, FraudAlert alert, Collector<Transaction> out) throws Exception {
        // Use copy constructor instead of manual field-by-field copy
        Transaction enrichedTx = new Transaction(origTx);

        List<FraudAlert> alerts = enrichedTx.getCepAlerts() != null
                ? new ArrayList<>(enrichedTx.getCepAlerts()) : new ArrayList<>();
        if (hasAlertType(enrichedTx, alert)) {
            return;
        }
        alerts.add(alert);
        enrichedTx.setCepAlerts(alerts);
        enrichedTx.setCepFlagged(true);
        enrichedTx.setLateAlert(true); // Mark as late alert

        out.collect(enrichedTx);
        // Also update stored transaction in emittedTxns state to contain the latest CEP alerts
        emittedTxns.put(origTx.getTransactionId(), enrichedTx);
        LOG.warn("Emitted late CEP alert [{}] for tx {} retaining original transaction details",
                alert.getAlertType(), alert.getTransactionId());
    }

    // ── Timer: emit transactions that have waited long enough ─
    private boolean hasAlertType(Transaction tx, FraudAlert alert) {
        if (tx == null || alert == null || tx.getCepAlerts() == null) {
            return false;
        }
        String alertType = alert.getAlertType();
        for (FraudAlert existing : tx.getCepAlerts()) {
            if (existing != null && alertType != null && alertType.equals(existing.getAlertType())) {
                return true;
            }
        }
        return false;
    }

    private boolean containsKey(String csvKeys, String key) {
        if (csvKeys == null || key == null) {
            return false;
        }
        for (String existing : csvKeys.split(",")) {
            if (key.equals(existing)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Transaction> out) throws Exception {
        String txIdsStr = timerTxIds.get(timestamp);
        if (txIdsStr == null) {
            return; // Nothing to emit for this exact timestamp
        }

        String[] txIds = txIdsStr.split(",");
        for (String txId : txIds) {
            Transaction tx = bufferedTxns.get(txId);
            if (tx != null) {
                out.collect(tx);
                bufferedTxns.remove(txId);
                // Mark as emitted so late-arriving CEP alerts know they are late
                emittedTxns.put(txId, tx);
            }
        }
        timerTxIds.remove(timestamp);
    }
}
