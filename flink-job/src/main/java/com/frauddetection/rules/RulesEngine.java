package com.frauddetection.rules;

import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.FraudRule;
import com.frauddetection.model.RuleViolation;
import com.frauddetection.model.Transaction;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Adaptive Rules Engine using Flink Broadcast State Pattern.
 *
 * <p>
 * Dynamic rules from Kafka are broadcast to all task managers and stored in
 * {@link MapStateDescriptor}. Each transaction is evaluated against all active
 * rules.
 *
 * <p>
 * Supported operators: GREATER_THAN, LESS_THAN, EQUALS, IN, NOT_IN
 * <p>
 * Optional: time_window constraint (start_hour, end_hour)
 */
public class RulesEngine extends KeyedBroadcastProcessFunction<String, Transaction, String, Transaction> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RulesEngine.class);

    private final MapStateDescriptor<String, String> rulesStateDesc;
    private transient com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    /**
     * Local cache of parsed FraudRule objects, avoiding JSON deserialization per
     * transaction.
     * Updated in processBroadcastElement(), read in processElement().
     */
    private transient Map<String, FraudRule> ruleCache;

    public RulesEngine(MapStateDescriptor<String, String> rulesStateDesc) {
        this.rulesStateDesc = rulesStateDesc;
    }

    private com.fasterxml.jackson.databind.ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
        }
        return objectMapper;
    }

    // ── Broadcast side: receive rule updates ──────────────
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<Transaction> out) throws Exception {
        try {
            FraudRule rule = getObjectMapper().readValue(value, FraudRule.class);
            String ruleId = rule.getRuleId();

            var state = ctx.getBroadcastState(rulesStateDesc);

            // Version check: only update if newer
            String existingJson = state.get(ruleId);
            if (existingJson != null) {
                FraudRule existing = getObjectMapper().readValue(existingJson, FraudRule.class);
                if (rule.getVersion() <= existing.getVersion()) {
                    return; // Skip older version
                }
            }

            state.put(ruleId, value);

            // Update local cache to avoid re-parsing on every processElement call
            if (ruleCache == null) {
                ruleCache = new HashMap<>();
            }
            ruleCache.put(ruleId, rule);

            LOG.info("Rule updated: {} v{} - {}", ruleId, rule.getVersion(), rule.getRuleName());

        } catch (Exception e) {
            LOG.error("Error processing broadcast rule: {}", e.getMessage());
        }
    }

    // ── Main side: evaluate transaction against rules ─────
    @Override
    public void processElement(Transaction tx, ReadOnlyContext ctx, Collector<Transaction> out) throws Exception {
        // Lazy rebuild cache from broadcast state if empty (e.g. after restart)
        if (ruleCache == null || ruleCache.isEmpty()) {
            ruleCache = new HashMap<>();
            var state = ctx.getBroadcastState(rulesStateDesc);
            for (Map.Entry<String, String> entry : state.immutableEntries()) {
                try {
                    ruleCache.put(entry.getKey(),
                            getObjectMapper().readValue(entry.getValue(), FraudRule.class));
                } catch (Exception ignored) {
                }
            }
        }

        List<RuleViolation> violations = new ArrayList<>();
        List<FraudAlert> alertObjects = new ArrayList<>();

        // Iterate over cached rules — no JSON deserialization per transaction
        for (FraudRule rule : ruleCache.values()) {
            try {
                if (!rule.isActive()) {
                    continue;
                }

                RuleViolation violation = evaluateRule(tx, rule);
                if (violation != null) {
                    violations.add(violation);

                    // Build full alert object
                    FraudAlert alert = new FraudAlert(
                            tx.getTransactionId(),
                            tx.getAccountId(),
                            violation.getRuleName() != null ? violation.getRuleName() : "RULE_VIOLATION",
                            "RULES_ENGINE",
                            violation.getSeverity(),
                            violation.getScore(),
                            violation.getDetails(),
                            String.format("Rule %s matched", violation.getRuleId()),
                            tx.getTimestamp());
                    alertObjects.add(alert);
                }
            } catch (Exception e) {
                LOG.error("Error evaluating rule: {}", e.getMessage());
            }
        }

        tx.setRuleAlerts(violations);
        tx.setRuleAlertObjects(alertObjects);
        tx.setRuleFlagged(!violations.isEmpty());

        out.collect(tx);
    }

    /**
     * Evaluate a single rule against a transaction.
     *
     * @return RuleViolation if rule matches, null otherwise
     */
    private RuleViolation evaluateRule(Transaction tx, FraudRule rule) {
        String field = rule.getField();
        String operator = rule.getOperator();
        Object txValue = getTransactionFieldValue(tx, field);

        if (txValue == null) {
            return null;
        }

        boolean matched = false;

        switch (operator) {
            case "GREATER_THAN":
                try {
                    matched = toDouble(txValue) > rule.getThresholdAsDouble();
                } catch (Exception e) {
                    return null;
                }
                break;

            case "LESS_THAN":
                try {
                    matched = toDouble(txValue) < rule.getThresholdAsDouble();
                } catch (Exception e) {
                    return null;
                }
                break;

            case "EQUALS":
                if (txValue instanceof Number) {
                    try {
                        matched = Math.abs(toDouble(txValue) - rule.getThresholdAsDouble()) < 0.000001;
                    } catch (Exception e) {
                        matched = String.valueOf(txValue).equals(String.valueOf(rule.getThreshold()));
                    }
                } else {
                    matched = String.valueOf(txValue).equals(String.valueOf(rule.getThreshold()));
                }
                break;

            case "IN":
                matched = rule.getThresholdAsList().contains(String.valueOf(txValue));
                break;

            case "NOT_IN":
                matched = !rule.getThresholdAsList().contains(String.valueOf(txValue));
                break;

            default:
                LOG.warn("Unknown operator: {}", operator);
                return null;
        }

        // Check time window constraint if present
        if (matched && rule.getTimeWindow() != null) {
            Map<String, Integer> tw = rule.getTimeWindow();
            int startHour = tw.getOrDefault("start_hour", 0);
            int endHour = tw.getOrDefault("end_hour", 24);

            if (tx.getTimestamp() > 0) {
                int hour = Instant.ofEpochMilli(tx.getTimestamp())
                        .atZone(java.time.ZoneId.of("Asia/Ho_Chi_Minh"))
                        .getHour();
                boolean inWindow;
                if (startHour <= endHour) {
                    inWindow = (hour >= startHour && hour < endHour);
                } else {
                    inWindow = (hour >= startHour || hour < endHour);
                }
                if (!inWindow) {
                    matched = false;
                }
            }
        }

        if (matched) {
            double score = "CRITICAL".equals(rule.getSeverity()) ? 0.8 : 0.6;
            return new RuleViolation(
                    rule.getRuleId(),
                    rule.getRuleName(),
                    rule.getSeverity(),
                    score,
                    String.format("Rule %s: %s %s %s (actual: %s)",
                            rule.getRuleId(), field, operator, rule.getThreshold(), txValue));
        }

        return null;
    }

    /**
     * Extract a field value from a transaction by field name.
     */
    private Object getTransactionFieldValue(Transaction tx, String field) {
        switch (field) {
            case "amount":
                return tx.getAmount();
            case "event_type":
                return tx.getEventType();
            case "channel":
                return tx.getChannel();
            case "merchant_id":
                return tx.getMerchantId();
            case "status":
                return tx.getStatus();
            case "card_id":
                return tx.getCardId();
            case "account_id":
                return tx.getAccountId();
            default:
                return null;
        }
    }

    private double toDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(String.valueOf(value));
    }
}
