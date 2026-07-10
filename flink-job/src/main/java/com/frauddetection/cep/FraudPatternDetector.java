package com.frauddetection.cep;

import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.Transaction;
import com.frauddetection.utils.HaversineUtils;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * CEP Pattern Detector using Flink CEP API.
 *
 * <p>
 * Defines and applies 4 fraud patterns on a keyed transaction stream:
 * <ol>
 * <li><b>Account Takeover (ATO)</b>: 3+ LOGIN_FAILED → LOGIN_SUCCESS → TRANSFER
 * within 10 min (CRITICAL)</li>
 * <li><b>Micro Transactions (Smurfing)</b>: 5+ small txns (amount &lt; $10)
 * within 2 min (HIGH)</li>
 * <li><b>Velocity Attack</b>: 3+ rapid medium-value TRANSFER/PAYMENT events
 * within a few seconds (HIGH/CRITICAL)</li>
 * <li><b>Impossible Travel</b>: 2 financial txns from locations &gt;150km apart
 * within 15 minutes (CRITICAL)</li>
 * </ol>
 *
 * <p>
 * All patterns use event time with bounded out-of-orderness watermark (5s).
 *
 * <p><b>Improvements over original:</b>
 * <ul>
 *   <li>Patterns use explicit skip strategies to balance duplicate control and event-level coverage</li>
 *   <li>Dynamic scoring scales with severity (failed count, txn count, travel distance)</li>
 *   <li>Thresholds are configurable via environment variables</li>
 *   <li>Geo validation uses proper coordinate range checks</li>
 *   <li>Micro Transactions includes merchant diversity awareness</li>
 * </ul>
 */
public class FraudPatternDetector {

    private static final Logger LOG = LoggerFactory.getLogger(FraudPatternDetector.class);

    /**
     * Financial event types that count for Micro Transactions and Impossible
     * Travel.
     */
    private static final Set<String> FINANCIAL_EVENT_TYPES = Set.of("TRANSFER", "PAYMENT", "WITHDRAWAL");

    // ── Configurable thresholds (from environment variables) ──

    /** Minimum failed logins to trigger ATO. Default: 3 */
    private final int atoMinFailedLogins;

    /** ATO time window in minutes. Default: 10 */
    private final long atoWindowMinutes;

    /** Minimum micro transactions to trigger alert. Default: 5 */
    private final int microMinTransactions;

    /** Maximum amount per transaction for micro pattern. Default: $10.0 */
    private final double microMaxAmount;

    /** Micro transactions time window in minutes. Default: 2 */
    private final long microWindowMinutes;

    /** Minimum velocity transactions to trigger alert. Default: 3 */
    private final int velocityMinTransactions;

    /** Minimum amount per velocity transaction. Default: $100.0 */
    private final double velocityMinAmount;

    /** Maximum amount per velocity transaction. Default: $2000.0 */
    private final double velocityMaxAmount;

    /** Velocity transactions time window in seconds. Default: 2 */
    private final long velocityWindowSeconds;

    /** Minimum distance (km) to trigger Impossible Travel alert. Default: 150 */
    private final double impossibleTravelDistanceKm;

    /** Impossible travel time window in minutes. Default: 15 */
    private final long travelWindowMinutes;

    /**
     * Create a FraudPatternDetector with thresholds loaded from environment
     * variables, falling back to sensible defaults.
     *
     * <p>Supported environment variables:
     * <ul>
     *   <li>{@code CEP_ATO_MIN_FAILED_LOGINS} — default 3</li>
     *   <li>{@code CEP_ATO_WINDOW_MINUTES} — default 10</li>
     *   <li>{@code CEP_MICRO_MIN_TRANSACTIONS} — default 5</li>
     *   <li>{@code CEP_MICRO_MAX_AMOUNT} — default 10.0</li>
     *   <li>{@code CEP_MICRO_WINDOW_MINUTES} — default 2</li>
     *   <li>{@code CEP_TRAVEL_DISTANCE_KM} — default 150.0</li>
     *   <li>{@code CEP_TRAVEL_WINDOW_MINUTES} — default 15</li>
     * </ul>
     */
    public FraudPatternDetector() {
        this.atoMinFailedLogins = intEnv("CEP_ATO_MIN_FAILED_LOGINS", 3);
        this.atoWindowMinutes = longEnv("CEP_ATO_WINDOW_MINUTES", 10);
        this.microMinTransactions = intEnv("CEP_MICRO_MIN_TRANSACTIONS", 5);
        this.microMaxAmount = doubleEnv("CEP_MICRO_MAX_AMOUNT", 10.0);
        this.microWindowMinutes = longEnv("CEP_MICRO_WINDOW_MINUTES", 2);
        this.velocityMinTransactions = intEnv("CEP_VELOCITY_MIN_TRANSACTIONS", 3);
        this.velocityMinAmount = doubleEnv("CEP_VELOCITY_MIN_AMOUNT", 100.0);
        this.velocityMaxAmount = doubleEnv("CEP_VELOCITY_MAX_AMOUNT", 2000.0);
        this.velocityWindowSeconds = longEnv("CEP_VELOCITY_WINDOW_SECONDS", 2);
        this.impossibleTravelDistanceKm = doubleEnv("CEP_TRAVEL_DISTANCE_KM", 150.0);
        this.travelWindowMinutes = longEnv("CEP_TRAVEL_WINDOW_MINUTES", 15);

        LOG.info("CEP thresholds: ATO(minFailed={}, window={}min), Micro(minTxn={}, maxAmt=${}, window={}min), "
                + "Velocity(minTxn={}, amount=${}-${}, window={}s), Travel(distKm={}, window={}min)",
                atoMinFailedLogins, atoWindowMinutes,
                microMinTransactions, microMaxAmount, microWindowMinutes,
                velocityMinTransactions, velocityMinAmount, velocityMaxAmount, velocityWindowSeconds,
                impossibleTravelDistanceKm, travelWindowMinutes);
    }

    /**
     * Apply all CEP patterns to the keyed stream and return a unified alert
     * stream.
     *
     * @param keyedStream the transaction stream keyed by accountId
     * @return DataStream of FraudAlerts from all patterns, unioned together
     */
    public DataStream<FraudAlert> detectAll(KeyedStream<Transaction, String> keyedStream) {
        DataStream<FraudAlert> atoAlerts = detectAccountTakeover(keyedStream);
        DataStream<FraudAlert> microAlerts = detectMicroTransactions(keyedStream);
        DataStream<FraudAlert> velocityAlerts = detectVelocityTransactions(keyedStream);
        DataStream<FraudAlert> travelAlerts = detectImpossibleTravel(keyedStream);

        LOG.info("CEP Pattern 1 (Account Takeover) registered");
        LOG.info("CEP Pattern 2 (Micro Transactions) registered");
        LOG.info("CEP Pattern 3 (Velocity Attack) registered");
        LOG.info("CEP Pattern 4 (Impossible Travel) registered");

        return atoAlerts.union(microAlerts).union(velocityAlerts).union(travelAlerts);
    }

    // ═══════════════════════════════════════════════════════
    // Pattern 1: Account Takeover (ATO)
    // 3+ LOGIN_FAILED → LOGIN_SUCCESS → TRANSFER within 10 min
    // Severity: Dynamic (HIGH→CRITICAL based on failedCount)
    //
    // skipPastLastEvent: after a match, skip all matched events
    //   so they are not reused in overlapping matches.
    // ═══════════════════════════════════════════════════════

    private DataStream<FraudAlert> detectAccountTakeover(
            KeyedStream<Transaction, String> keyedStream) {

        final int minFailed = this.atoMinFailedLogins;
        final long windowMin = this.atoWindowMinutes;

        Pattern<Transaction, ?> atoPattern = Pattern
                .<Transaction>begin("failed", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "LOGIN_FAILED".equals(tx.getEventType());
                    }
                })
                .times(minFailed, 10)
                .greedy()
                .followedBy("success")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "LOGIN_SUCCESS".equals(tx.getEventType());
                    }
                })
                .followedBy("transfer")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "TRANSFER".equals(tx.getEventType());
                    }
                })
                .within(Time.minutes(windowMin));

        PatternStream<Transaction> patternStream = CEP.pattern(keyedStream, atoPattern);

        return patternStream.select(new PatternSelectFunction<Transaction, FraudAlert>() {
            @Override
            public FraudAlert select(Map<String, List<Transaction>> pattern) {
                List<Transaction> failedLogins = pattern.get("failed");
                Transaction transfer = pattern.get("transfer").get(0);
                int failedCount = failedLogins.size();

                // Dynamic scoring: base 0.80 + 0.03 per failed login, capped at 0.99
                double score = Math.min(0.99, 0.80 + failedCount * 0.03);
                // Dynamic severity: CRITICAL when 7+ failed logins
                String severity = failedCount >= 7 ? "CRITICAL" : "HIGH";

                return new FraudAlert(
                        transfer.getTransactionId(),
                        transfer.getAccountId(),
                        "ACCOUNT_TAKEOVER",
                        "CEP",
                        severity,
                        score,
                        String.format("Brute force: %d failed logins followed by transfer of $%.2f (score=%.2f)",
                                failedCount, transfer.getAmount(), score),
                        String.format("LOGIN_FAILED x%d -> LOGIN_SUCCESS -> TRANSFER", failedCount),
                        transfer.getTimestamp());
            }
        });
    }

    // ═══════════════════════════════════════════════════════
    // Pattern 2: Micro Transactions (Carding / Smurfing)
    // 5+ financial transactions under $10 within 2 minutes
    // Severity: Dynamic (MEDIUM→HIGH→CRITICAL based on count & merchant diversity)
    //
    // skipPastLastEvent: after a match, skip all matched events
    //   so they are not reused in overlapping matches.
    // NO .greedy(): emit immediately when 5th event arrives
    //   instead of waiting for the 2-min window to expire.
    // ═══════════════════════════════════════════════════════

    private DataStream<FraudAlert> detectMicroTransactions(
            KeyedStream<Transaction, String> keyedStream) {

        final int minTxn = this.microMinTransactions;
        final double maxAmt = this.microMaxAmount;
        final long windowMin = this.microWindowMinutes;

        Pattern<Transaction, ?> microPattern = Pattern
                .<Transaction>begin("micro", AfterMatchSkipStrategy.skipToNext())
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return FINANCIAL_EVENT_TYPES.contains(tx.getEventType())
                                && tx.getAmount() < maxAmt;
                    }
                })
                .times(minTxn)
                .within(Time.minutes(windowMin));

        PatternStream<Transaction> patternStream = CEP.pattern(keyedStream, microPattern);

        return patternStream.flatSelect(new PatternFlatSelectFunction<Transaction, FraudAlert>() {
            @Override
            public void flatSelect(Map<String, List<Transaction>> pattern, Collector<FraudAlert> out) {
                List<Transaction> microTxns = pattern.get("micro");
                int count = microTxns.size();
                double totalAmount = microTxns.stream().mapToDouble(Transaction::getAmount).sum();

                // Merchant diversity: count distinct non-null merchant IDs
                long distinctMerchants = microTxns.stream()
                        .map(Transaction::getMerchantId)
                        .filter(Objects::nonNull)
                        .distinct()
                        .count();

                // Dynamic scoring: base 0.75 + 0.03 per transaction, capped at 0.98
                double score = Math.min(0.98, 0.75 + count * 0.03);

                // Dynamic severity based on count AND merchant diversity:
                // - 1 merchant → MEDIUM (could be normal shopping at one store)
                // - 2+ merchants, count < 10 → HIGH
                // - 3+ merchants OR count >= 10 → CRITICAL (clear smurfing)
                String severity;
                if (count >= 10 || distinctMerchants >= 3) {
                    severity = "CRITICAL";
                } else if (distinctMerchants <= 1) {
                    severity = "MEDIUM";
                } else {
                    severity = "HIGH";
                }

                String details = String.format("%d small transactions under $%.0f in %d mins, total: $%.2f, "
                        + "distinct merchants: %d (score=%.2f)",
                        count, maxAmt, windowMin, totalAmount, distinctMerchants, score);
                String matchedEvents = String.format("TRANSFER/PAYMENT < $%.0f x%d within %d minutes",
                        maxAmt, count, windowMin);

                for (Transaction tx : microTxns) {
                    out.collect(new FraudAlert(
                            tx.getTransactionId(),
                            tx.getAccountId(),
                            "MICRO_TRANSACTIONS",
                            "CEP",
                            severity,
                            score,
                            details,
                            matchedEvents,
                            tx.getTimestamp()));
                }
            }
        });
    }

    // ═══════════════════════════════════════════════════════
    // Pattern 3: Velocity Attack
    // 3+ medium-value TRANSFER/PAYMENT events within a very short window.
    private DataStream<FraudAlert> detectVelocityTransactions(
            KeyedStream<Transaction, String> keyedStream) {

        final int minTxn = this.velocityMinTransactions;
        final double minAmt = this.velocityMinAmount;
        final double maxAmt = this.velocityMaxAmount;
        final long windowSec = this.velocityWindowSeconds;

        Pattern<Transaction, ?> velocityPattern = Pattern
                .<Transaction>begin("velocity", AfterMatchSkipStrategy.skipToNext())
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return ("TRANSFER".equals(tx.getEventType()) || "PAYMENT".equals(tx.getEventType()))
                                && tx.getAmount() >= minAmt
                                && tx.getAmount() <= maxAmt;
                    }
                })
                .times(minTxn)
                .within(Time.seconds(windowSec));

        PatternStream<Transaction> patternStream = CEP.pattern(keyedStream, velocityPattern);

        return patternStream.flatSelect(new PatternFlatSelectFunction<Transaction, FraudAlert>() {
            @Override
            public void flatSelect(Map<String, List<Transaction>> pattern, Collector<FraudAlert> out) {
                List<Transaction> velocityTxns = pattern.get("velocity");
                int count = velocityTxns.size();
                double totalAmount = velocityTxns.stream().mapToDouble(Transaction::getAmount).sum();

                long firstTs = velocityTxns.stream().mapToLong(Transaction::getTimestamp).min().orElse(0L);
                long lastTs = velocityTxns.stream().mapToLong(Transaction::getTimestamp).max().orElse(firstTs);
                long spanMs = Math.max(0L, lastTs - firstTs);

                double score = Math.min(0.95, 0.70 + count * 0.05);
                String severity = count >= 5 || spanMs <= 1000 ? "CRITICAL" : "HIGH";

                String details = String.format("%d rapid medium-value transactions in %.2fs, total: $%.2f "
                        + "(amount range $%.0f-$%.0f, score=%.2f)",
                        count, spanMs / 1000.0, totalAmount, minAmt, maxAmt, score);
                String matchedEvents = String.format("TRANSFER/PAYMENT $%.0f-$%.0f x%d within %d seconds",
                        minAmt, maxAmt, count, windowSec);

                for (Transaction tx : velocityTxns) {
                    out.collect(new FraudAlert(
                            tx.getTransactionId(),
                            tx.getAccountId(),
                            "VELOCITY_ATTACK",
                            "CEP",
                            severity,
                            score,
                            details,
                            matchedEvents,
                            tx.getTimestamp()));
                }
            }
        });
    }

    // Pattern 4: Impossible Travel
    // 2 financial transactions from locations >150km apart within 15 minutes
    // Severity: Dynamic (HIGH→CRITICAL based on distance)
    //
    // skipPastLastEvent: after a match, skip all matched events
    //   so they are not reused in overlapping matches.
    // ═══════════════════════════════════════════════════════

    private DataStream<FraudAlert> detectImpossibleTravel(
            KeyedStream<Transaction, String> keyedStream) {

        final double distThresholdKm = this.impossibleTravelDistanceKm;
        final long windowMin = this.travelWindowMinutes;

        Pattern<Transaction, ?> travelPattern = Pattern
                .<Transaction>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return FINANCIAL_EVENT_TYPES.contains(tx.getEventType())
                                && isValidCoordinate(tx.getLatitude(), tx.getLongitude());
                    }
                })
                .followedBy("second")
                .where(new IterativeCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx, IterativeCondition.Context<Transaction> ctx)
                            throws Exception {
                        if (!FINANCIAL_EVENT_TYPES.contains(tx.getEventType())) {
                            return false;
                        }
                        if (!isValidCoordinate(tx.getLatitude(), tx.getLongitude())) {
                            return false;
                        }
                        // Check distance against the first matched event inside the pattern
                        for (Transaction first : ctx.getEventsForPattern("first")) {
                            if (!isValidCoordinate(first.getLatitude(), first.getLongitude())) {
                                continue;
                            }
                            double distance = HaversineUtils.distanceKm(
                                    first.getLatitude(), first.getLongitude(),
                                    tx.getLatitude(), tx.getLongitude());
                            if (distance >= distThresholdKm) {
                                return true;
                            }
                        }
                        return false;
                    }
                })
                .within(Time.minutes(windowMin));

        PatternStream<Transaction> patternStream = CEP.pattern(keyedStream, travelPattern);

        return patternStream.select(new PatternSelectFunction<Transaction, FraudAlert>() {
            @Override
            public FraudAlert select(Map<String, List<Transaction>> pattern) {
                Transaction first = pattern.get("first").get(0);
                Transaction second = pattern.get("second").get(0);

                double distance = HaversineUtils.distanceKm(
                        first.getLatitude(), first.getLongitude(),
                        second.getLatitude(), second.getLongitude());

                // Dynamic scoring: base 0.80 + distance/10000, capped at 0.99
                double score = Math.min(0.99, 0.80 + distance / 10000.0);
                // Dynamic severity: CRITICAL when distance >= 2000km
                String severity = distance >= 2000.0 ? "CRITICAL" : "HIGH";

                return new FraudAlert(
                        second.getTransactionId(),
                        second.getAccountId(),
                        "IMPOSSIBLE_TRAVEL",
                        "CEP",
                        severity,
                        score,
                        String.format("Location jump of %.0f km from [%.4f,%.4f] to [%.4f,%.4f] (score=%.2f)",
                                distance,
                                first.getLatitude(), first.getLongitude(),
                                second.getLatitude(), second.getLongitude(),
                                score),
                        String.format("GEO_ANOMALY: %.0f km location jump within %d minutes",
                                distance, windowMin),
                        second.getTimestamp());
            }
        });
    }

    // ═══════════════════════════════════════════════════════
    // Helper methods
    // ═══════════════════════════════════════════════════════

    /**
     * Validate that coordinates are within valid geographic range.
     * Rejects (0,0) because the Gulf of Guinea has no ATM/POS terminals,
     * and our location parser defaults to (0,0) for missing/invalid data.
     *
     * @param lat latitude in degrees
     * @param lon longitude in degrees
     * @return true if coordinates represent a valid, non-default location
     */
    private static boolean isValidCoordinate(double lat, double lon) {
        return lat >= -90.0 && lat <= 90.0
                && lon >= -180.0 && lon <= 180.0
                && !(lat == 0.0 && lon == 0.0);
    }

    private static int intEnv(String key, int defaultValue) {
        String val = System.getenv(key);
        if (val != null && !val.isBlank()) {
            try {
                return Integer.parseInt(val.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid integer for env var {}: '{}', using default {}", key, val, defaultValue);
            }
        }
        return defaultValue;
    }

    private static long longEnv(String key, long defaultValue) {
        String val = System.getenv(key);
        if (val != null && !val.isBlank()) {
            try {
                return Long.parseLong(val.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid long for env var {}: '{}', using default {}", key, val, defaultValue);
            }
        }
        return defaultValue;
    }

    private static double doubleEnv(String key, double defaultValue) {
        String val = System.getenv(key);
        if (val != null && !val.isBlank()) {
            try {
                return Double.parseDouble(val.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid double for env var {}: '{}', using default {}", key, val, defaultValue);
            }
        }
        return defaultValue;
    }
}
