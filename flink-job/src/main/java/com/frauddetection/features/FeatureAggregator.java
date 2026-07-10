package com.frauddetection.features;

import com.frauddetection.model.AggregateFeatures;
import com.frauddetection.model.Transaction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Feature Aggregator — computes per-account aggregate features using Flink
 * keyed state.
 *
 * <p>
 * <b>State Design:</b> Uses {@code MapState} with {@code StateTtlConfig}
 * for bounded state growth and automatic cleanup of inactive accounts.
 *
 * <p>
 * <b>Performance Optimization:</b> Replaced O(N) full scan of txHistory with
 * O(1)
 * pre-computed incremental counters. Each transaction only updates counters and
 * registers
 * cleanup timers, instead of iterating over all historical entries.
 *
 * <p>
 * <b>Latency Fix:</b> Switched from event-time timers to processing-time
 * timers.
 * Event-time timers only fire when the watermark advances past the timer
 * timestamp,
 * which stalls when throughput is low or uneven — causing unbounded state
 * growth,
 * checkpoint failures, and the >2 minute latency. Processing-time timers fire
 * based on wall-clock time, ensuring counters are decremented reliably.
 *
 * <p>
 * Features computed:
 * <ul>
 * <li>txCount1h: transactions in last 1 hour</li>
 * <li>txCount24h: transactions in last 24 hours</li>
 * <li>avgAmount24h: average amount in last 24 hours</li>
 * <li>amountDeviation: standard deviation of current amount from 24h
 * average</li>
 * <li>timeSinceLastMs: milliseconds since last transaction</li>
 * </ul>
 */
public class FeatureAggregator extends KeyedProcessFunction<String, Transaction, Transaction> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FeatureAggregator.class);

    private static final long ONE_HOUR_MS = 60 * 60 * 1000L;
    private static final long TWENTY_FOUR_HOURS_MS = 24 * 60 * 60 * 1000L;

    // ── Pre-computed incremental counters (O(1) per transaction) ──

    /** Transaction count within 1h window. */
    private transient ValueState<Integer> count1hState;

    /** Transaction count within 24h window. */
    private transient ValueState<Integer> count24hState;

    /** Sum of amounts within 24h window. */
    private transient ValueState<Double> sumAmount24hState;

    /** Sum of squared amounts within 24h window (for std dev calculation). */
    private transient ValueState<Double> sumSquared24hState;

    /** 1h timer index: timerTimestamp → comma-separated amounts. TTL = 2h. */
    private transient MapState<Long, String> timer1hIndex;

    /** 24h timer index: timerTimestamp → comma-separated amounts. TTL = 25h. */
    private transient MapState<Long, String> timer24hIndex;

    /** Last transaction timestamp per account. */
    private transient ValueState<Long> lastTxTime;

    /** Last transaction latitude per account. */
    private transient ValueState<Double> lastLatitudeState;

    /** Last transaction longitude per account. */
    private transient ValueState<Double> lastLongitudeState;

    @Override
    public void open(Configuration parameters) {
        // TTL config for counter states (25h = 24h window + 1h buffer)
        // cleanupIncrementally: checks 10 entries per state access for expired entries
        // This provides continuous cleanup instead of only during full snapshots
        StateTtlConfig counterTtl = StateTtlConfig.newBuilder(Time.hours(25))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupIncrementally(10, true)
                .build();

        // Counter states — now with TTL to auto-cleanup inactive accounts
        ValueStateDescriptor<Integer> count1hDesc = new ValueStateDescriptor<>("feat-count-1h", Types.INT);
        count1hDesc.enableTimeToLive(counterTtl);
        count1hState = getRuntimeContext().getState(count1hDesc);

        ValueStateDescriptor<Integer> count24hDesc = new ValueStateDescriptor<>("feat-count-24h", Types.INT);
        count24hDesc.enableTimeToLive(counterTtl);
        count24hState = getRuntimeContext().getState(count24hDesc);

        ValueStateDescriptor<Double> sumAmtDesc = new ValueStateDescriptor<>("feat-sum-amount-24h", Types.DOUBLE);
        sumAmtDesc.enableTimeToLive(counterTtl);
        sumAmount24hState = getRuntimeContext().getState(sumAmtDesc);

        ValueStateDescriptor<Double> sumSqDesc = new ValueStateDescriptor<>("feat-sum-squared-24h", Types.DOUBLE);
        sumSqDesc.enableTimeToLive(counterTtl);
        sumSquared24hState = getRuntimeContext().getState(sumSqDesc);

        // 1h timer index with shorter TTL = 2h
        StateTtlConfig ttl1h = StateTtlConfig.newBuilder(Time.hours(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupIncrementally(10, true)
                .build();
        MapStateDescriptor<Long, String> timer1hDesc = new MapStateDescriptor<>(
                "feat-timer-1h", Types.LONG, Types.STRING);
        timer1hDesc.enableTimeToLive(ttl1h);
        timer1hIndex = getRuntimeContext().getMapState(timer1hDesc);

        // 24h timer index with TTL = 25h
        StateTtlConfig ttl24h = StateTtlConfig.newBuilder(Time.hours(25))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupIncrementally(10, true)
                .build();
        MapStateDescriptor<Long, String> timer24hDesc = new MapStateDescriptor<>(
                "feat-timer-24h", Types.LONG, Types.STRING);
        timer24hDesc.enableTimeToLive(ttl24h);
        timer24hIndex = getRuntimeContext().getMapState(timer24hDesc);

        ValueStateDescriptor<Long> lastTxDesc = new ValueStateDescriptor<>("last-tx-time", Types.LONG);
        lastTxDesc.enableTimeToLive(counterTtl);
        lastTxTime = getRuntimeContext().getState(lastTxDesc);

        ValueStateDescriptor<Double> lastLatDesc = new ValueStateDescriptor<>("last-tx-latitude", Types.DOUBLE);
        lastLatDesc.enableTimeToLive(counterTtl);
        lastLatitudeState = getRuntimeContext().getState(lastLatDesc);

        ValueStateDescriptor<Double> lastLonDesc = new ValueStateDescriptor<>("last-tx-longitude", Types.DOUBLE);
        lastLonDesc.enableTimeToLive(counterTtl);
        lastLongitudeState = getRuntimeContext().getState(lastLonDesc);
    }

    @Override
    public void processElement(Transaction tx, Context ctx, Collector<Transaction> out) throws Exception {
        try {
            // Bypass late alert transactions (they are dummy transaction wrappers, not real events)
            if (tx.isLateAlert()) {
                tx.setAggFeatures(new AggregateFeatures(0, 0, 0, 0, 0, 0.0));
                out.collect(tx);
                return;
            }

            // ── Short-circuit: skip feature computation for CRITICAL fraud ──
            // Transactions already confirmed as fraud by Rules/CEP (CRITICAL) do not
            // need ML features. Skipping also prevents fraud transactions from
            // polluting the account's behavioral profile (24h average, frequency).
            if (tx.hasCriticalAlert()) {
                LOG.debug("Short-circuit FeatureAggregator for CRITICAL tx {} (account {})",
                        tx.getTransactionId(), tx.getAccountId());
                tx.setAggFeatures(new AggregateFeatures(0, 0, 0, 0, 0, 0.0));
                out.collect(tx);
                return;
            }

            long now = tx.getTimestamp();
            if (now <= 0) {
                now = System.currentTimeMillis();
            }
            double amount = tx.getAmount();

            // Read current counters (O(1))
            int count1h = count1hState.value() != null ? count1hState.value() : 0;
            int count24h = count24hState.value() != null ? count24hState.value() : 0;
            double sumAmount24h = sumAmount24hState.value() != null ? sumAmount24hState.value() : 0.0;
            double sumSquared24h = sumSquared24hState.value() != null ? sumSquared24hState.value() : 0.0;

            // Compute features BEFORE updating counters (reflects state before this tx)
            double avgAmount24h = count24h > 0 ? sumAmount24h / count24h : 0.0;

            double amountDeviation = 0.0;
            if (count24h > 1) {
                double variance = (sumSquared24h / count24h) - (avgAmount24h * avgAmount24h);
                double stdDev = Math.sqrt(Math.max(0, variance));
                amountDeviation = stdDev > 0 ? (amount - avgAmount24h) / stdDev : 0.0;
            }

            Long lastTime = lastTxTime.value();
            long timeSinceLast = lastTime != null ? Math.max(0, now - lastTime) : 0;

            // Compute distance from last transaction
            Double lastLat = lastLatitudeState.value();
            Double lastLon = lastLongitudeState.value();
            double distanceKm = 0.0;
            if (lastLat != null && lastLon != null && tx.getLatitude() != 0.0 && tx.getLongitude() != 0.0) {
                distanceKm = com.frauddetection.utils.HaversineUtils.distanceKm(
                        lastLat, lastLon, tx.getLatitude(), tx.getLongitude());
            }

            // Attach features to transaction
            tx.setAggFeatures(new AggregateFeatures(
                    count1h,
                    count24h,
                    Math.round(avgAmount24h * 100.0) / 100.0,
                    Math.round(amountDeviation * 10000.0) / 10000.0,
                    timeSinceLast,
                    Math.round(distanceKm * 100.0) / 100.0));

            // ── Update counters incrementally (O(1)) ──
            count1h++;
            count24h++;
            sumAmount24h += amount;
            sumSquared24h += amount * amount;

            count1hState.update(count1h);
            count24hState.update(count24h);
            sumAmount24hState.update(sumAmount24h);
            sumSquared24hState.update(sumSquared24h);

            if (lastTime == null || now > lastTime) {
                lastTxTime.update(now);
            }

            if (tx.getLatitude() != 0.0 && tx.getLongitude() != 0.0) {
                lastLatitudeState.update(tx.getLatitude());
                lastLongitudeState.update(tx.getLongitude());
            }

            // Register PROCESSING-TIME timers to decrement counters when entries exit
            // windows.
            // Using processing-time instead of event-time because event-time timers only
            // fire
            // when the watermark advances — which stalls under low/uneven throughput,
            // causing
            // unbounded state growth and the >2min latency bottleneck.
            long procNow = ctx.timerService().currentProcessingTime();
            // Align timers to the next second boundary to group transactions and avoid
            // timer explosion
            long timer1h = ((procNow + ONE_HOUR_MS) / 1000 + 1) * 1000;
            long timer24h = ((procNow + TWENTY_FOUR_HOURS_MS) / 1000 + 1) * 1000;

            // Append amount to timer index (handles same-timestamp collisions)
            String existing1h = timer1hIndex.get(timer1h);
            timer1hIndex.put(timer1h, existing1h == null
                    ? String.valueOf(amount)
                    : existing1h + "," + amount);

            String existing24h = timer24hIndex.get(timer24h);
            timer24hIndex.put(timer24h, existing24h == null
                    ? String.valueOf(amount)
                    : existing24h + "," + amount);

            ctx.timerService().registerProcessingTimeTimer(timer1h);
            ctx.timerService().registerProcessingTimeTimer(timer24h);

            out.collect(tx);

        } catch (Exception e) {
            LOG.error("Feature aggregator error for account {}: {}",
                    tx.getAccountId(), e.getMessage());
            // Still emit with empty features to avoid dropping transactions
            if (tx.getAggFeatures() == null) {
                tx.setAggFeatures(new AggregateFeatures(0, 0, 0, 0, 0, 0.0));
            }
            out.collect(tx);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Transaction> out) throws Exception {
        // Process 1h expirations: decrement count1h for each amount at this timestamp
        String amounts1h = timer1hIndex.get(timestamp);
        if (amounts1h != null) {
            for (String amtStr : amounts1h.split(",")) {
                Integer c = count1hState.value();
                if (c != null && c > 0) {
                    count1hState.update(c - 1);
                }
            }
            timer1hIndex.remove(timestamp);
        }

        // Process 24h expirations: decrement count24h, sumAmount, sumSquared
        String amounts24h = timer24hIndex.get(timestamp);
        if (amounts24h != null) {
            for (String amtStr : amounts24h.split(",")) {
                try {
                    double amount = Double.parseDouble(amtStr.trim());
                    Integer c = count24hState.value();
                    if (c != null && c > 0) {
                        count24hState.update(c - 1);
                    }
                    Double s = sumAmount24hState.value();
                    if (s != null) {
                        sumAmount24hState.update(Math.max(0, s - amount));
                    }
                    Double sq = sumSquared24hState.value();
                    if (sq != null) {
                        sumSquared24hState.update(Math.max(0, sq - amount * amount));
                    }
                } catch (NumberFormatException ignored) {
                }
            }
            timer24hIndex.remove(timestamp);
        }
    }
}
