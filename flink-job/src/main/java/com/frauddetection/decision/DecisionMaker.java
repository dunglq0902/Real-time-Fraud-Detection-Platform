package com.frauddetection.decision;

import com.frauddetection.model.DecisionResult;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.RuleViolation;
import com.frauddetection.model.Transaction;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Decision Engine — hierarchical fraud decision: Rules &gt; CEP &gt; ML.
 *
 * <p>
 * Decision mapping (per specification):
 * <ul>
 * <li>Hard-rule match (CRITICAL) =&gt; BLOCK</li>
 * <li>Severe CEP pattern (CRITICAL) =&gt; BLOCK</li>
 * <li>Suspicious CEP pattern (non-critical) =&gt; ALERT</li>
 * <li>ML score &gt; 0.85 =&gt; BLOCK</li>
 * <li>ML score &gt; 0.5 =&gt; ALERT</li>
 * <li>Rule flagged (non-critical) =&gt; ALERT</li>
 * <li>No anomaly =&gt; APPROVE</li>
 * </ul>
 */
public class DecisionMaker implements FlatMapFunction<Transaction, DecisionResult> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DecisionMaker.class);

    /** ML thresholds */
    private static final double ML_BLOCK_THRESHOLD = 0.85;
    private static final double ML_ALERT_THRESHOLD = 0.5;

    @Override
    public void flatMap(Transaction tx, Collector<DecisionResult> out) throws Exception {
        try {
            boolean cepFlagged = tx.isCepFlagged();
            boolean ruleFlagged = tx.isRuleFlagged();
            double mlScore = tx.getMlScore();

            List<FraudAlert> cepAlerts = tx.getCepAlerts();
            List<RuleViolation> ruleAlerts = tx.getRuleAlerts();

            // Calculate combined risk score
            double maxCepScore = cepAlerts.stream()
                    .mapToDouble(FraudAlert::getScore)
                    .max().orElse(0.0);
            double maxRuleScore = ruleAlerts.stream()
                    .mapToDouble(RuleViolation::getScore)
                    .max().orElse(0.0);
            double combinedScore = Math.max(mlScore, Math.max(maxCepScore, maxRuleScore));

            // CEP severity classification
            boolean hasCriticalCep = cepAlerts.stream()
                    .anyMatch(a -> "CRITICAL".equals(a.getSeverity()));
            boolean hasSuspiciousCep = cepFlagged && !hasCriticalCep;

            // Rules severity classification
            boolean hasCriticalRule = ruleAlerts.stream()
                    .anyMatch(r -> "CRITICAL".equals(r.getSeverity()));

            // ── Hierarchical Decision Logic ──
            String decision;
            String decisionSource;
            int isFraud;

            // Layer 1 (Highest Priority): Rules Engine
            if (hasCriticalRule) {
                decision = "BLOCK";
                decisionSource = "RULES_ENGINE";
                isFraud = 1;
            }
            // Layer 2: CEP Engine
            else if (hasCriticalCep) {
                decision = "BLOCK";
                decisionSource = "CEP_ENGINE";
                isFraud = 1;
            } else if (hasSuspiciousCep) {
                decision = "ALERT";
                decisionSource = "CEP_ENGINE";
                isFraud = 1;
            }
            // Layer 3 (Lowest): ML Scoring
            else if (mlScore > ML_BLOCK_THRESHOLD) {
                decision = "BLOCK";
                decisionSource = "ML_SCORING";
                isFraud = 1;
            } else if (mlScore > ML_ALERT_THRESHOLD) {
                decision = "ALERT";
                decisionSource = "ML_SCORING";
                isFraud = 1;
            }
            // Layer 4: Non-critical rules
            else if (ruleFlagged) {
                decision = "ALERT";
                decisionSource = "RULES_ENGINE";
                isFraud = 1;
            }
            // All clear
            else {
                decision = "APPROVE";
                decisionSource = "NONE";
                isFraud = 0;
            }

            // Build output record
            DecisionResult result = new DecisionResult();
            result.setTransactionId(tx.getTransactionId());
            result.setAccountId(tx.getAccountId());
            result.setCardId(tx.getCardId() != null ? tx.getCardId() : "");
            result.setEventTime(tx.getTimestamp());
            result.setAmount(tx.getAmount());
            result.setEventType(tx.getEventType());
            result.setChannel(tx.getChannel() != null ? tx.getChannel() : "");
            result.setLatitude(tx.getLatitude());
            result.setLongitude(tx.getLongitude());
            result.setMerchantId(tx.getMerchantId() != null ? tx.getMerchantId() : "");
            result.setStatus("COMPLETED");
            result.setMlScore(mlScore);
            result.setRuleTriggered(
                    ruleAlerts.stream()
                            .map(RuleViolation::getRuleId)
                            .collect(Collectors.joining(", ")));
            result.setIsFraud(isFraud);
            result.setGroundTruthIsFraud(tx.getIsFraud());
            result.setDecision(decision);
            result.setDecisionSource(decisionSource);
            result.setCombinedScore(combinedScore);
            result.setProducedAt(tx.getProducedAt());
            result.setDecidedAt(System.currentTimeMillis());

            out.collect(result);

        } catch (Exception e) {
            LOG.error("Decision maker error for tx {}: {}", tx.getTransactionId(), e.getMessage());
        }
    }
}
