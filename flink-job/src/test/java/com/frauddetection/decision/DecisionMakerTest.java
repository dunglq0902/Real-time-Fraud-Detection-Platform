package com.frauddetection.decision;

import com.frauddetection.model.DecisionResult;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.RuleViolation;
import com.frauddetection.model.Transaction;

import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for DecisionMaker.
 * Verifies the hierarchical decision logic: Rules > CEP > ML.
 */
public class DecisionMakerTest {

    private final DecisionMaker decisionMaker = new DecisionMaker();

    // ── Helper to create a base transaction ───────────────
    private Transaction createBaseTx() {
        Transaction tx = new Transaction();
        tx.setTransactionId("TX-001");
        tx.setAccountId("ACC-001");
        tx.setCardId("CARD-001");
        tx.setTimestamp(System.currentTimeMillis());
        tx.setProducedAt(System.currentTimeMillis());
        tx.setAmount(100.0);
        tx.setEventType("TRANSFER");
        tx.setChannel("ONLINE");
        tx.setStatus("COMPLETED");
        tx.setCepAlerts(new ArrayList<>());
        tx.setRuleAlerts(new ArrayList<>());
        tx.setRuleAlertObjects(new ArrayList<>());
        return tx;
    }

    // ── Collector for capturing output ────────────────────
    private static class TestCollector implements Collector<DecisionResult> {
        List<DecisionResult> results = new ArrayList<>();

        @Override
        public void collect(DecisionResult record) {
            results.add(record);
        }

        @Override
        public void close() {}
    }

    // ═══════════════════════════════════════════════════════
    // Test 1: Clean transaction → APPROVE
    // ═══════════════════════════════════════════════════════
    @Test
    public void testCleanTransaction_ShouldApprove() throws Exception {
        Transaction tx = createBaseTx();
        TestCollector collector = new TestCollector();

        decisionMaker.flatMap(tx, collector);

        assertEquals(1, collector.results.size());
        DecisionResult result = collector.results.get(0);
        assertEquals("APPROVE", result.getDecision());
        assertEquals("NONE", result.getDecisionSource());
        assertEquals(0, result.getIsFraud());
        assertEquals(0, result.getGroundTruthIsFraud());
    }

    // ═══════════════════════════════════════════════════════
    // Test 2: Critical rule → BLOCK
    // ═══════════════════════════════════════════════════════
    @Test
    public void testGroundTruthLabelIsPreservedSeparately() throws Exception {
        Transaction tx = createBaseTx();
        tx.setIsFraud(1);
        TestCollector collector = new TestCollector();

        decisionMaker.flatMap(tx, collector);

        DecisionResult result = collector.results.get(0);
        assertEquals("APPROVE", result.getDecision());
        assertEquals(0, result.getIsFraud());
        assertEquals(1, result.getGroundTruthIsFraud());
    }

    @Test
    public void testCriticalRule_ShouldBlock() throws Exception {
        Transaction tx = createBaseTx();
        tx.setRuleFlagged(true);
        List<RuleViolation> rules = new ArrayList<>();
        rules.add(new RuleViolation("RULE_001", "High Amount", "CRITICAL", 0.9, "amount > 10000"));
        tx.setRuleAlerts(rules);

        TestCollector collector = new TestCollector();
        decisionMaker.flatMap(tx, collector);

        DecisionResult result = collector.results.get(0);
        assertEquals("BLOCK", result.getDecision());
        assertEquals("RULES_ENGINE", result.getDecisionSource());
        assertEquals(1, result.getIsFraud());
    }

    // ═══════════════════════════════════════════════════════
    // Test 3: Critical CEP alert → BLOCK
    // ═══════════════════════════════════════════════════════
    @Test
    public void testCriticalCep_ShouldBlock() throws Exception {
        Transaction tx = createBaseTx();
        tx.setCepFlagged(true);
        List<FraudAlert> cepAlerts = new ArrayList<>();
        cepAlerts.add(new FraudAlert("TX-001", "ACC-001", "ACCOUNT_TAKEOVER",
                "CEP", "CRITICAL", 0.95, "3 failed logins", "ATO pattern", 123456789L));
        tx.setCepAlerts(cepAlerts);

        TestCollector collector = new TestCollector();
        decisionMaker.flatMap(tx, collector);

        DecisionResult result = collector.results.get(0);
        assertEquals("BLOCK", result.getDecision());
        assertEquals("CEP_ENGINE", result.getDecisionSource());
        assertEquals(1, result.getIsFraud());
    }

    // ═══════════════════════════════════════════════════════
    // Test 4: Non-critical CEP alert → ALERT (not BLOCK)
    // ═══════════════════════════════════════════════════════
    @Test
    public void testSuspiciousCep_ShouldAlert() throws Exception {
        Transaction tx = createBaseTx();
        tx.setCepFlagged(true);
        List<FraudAlert> cepAlerts = new ArrayList<>();
        cepAlerts.add(new FraudAlert("TX-001", "ACC-001", "MICRO_TRANSACTIONS",
                "CEP", "HIGH", 0.85, "5 micro txns", "Smurfing pattern", 123456789L));
        tx.setCepAlerts(cepAlerts);

        TestCollector collector = new TestCollector();
        decisionMaker.flatMap(tx, collector);

        DecisionResult result = collector.results.get(0);
        assertEquals("ALERT", result.getDecision());
        assertEquals("CEP_ENGINE", result.getDecisionSource());
    }

    // ═══════════════════════════════════════════════════════
    // Test 5: Very high ML score (>0.85) → BLOCK
    // ═══════════════════════════════════════════════════════
    @Test
    public void testHighMlScore_ShouldBlock() throws Exception {
        Transaction tx = createBaseTx();
        tx.setMlScore(0.9);
        tx.setMlAvailable(true);

        TestCollector collector = new TestCollector();
        decisionMaker.flatMap(tx, collector);

        DecisionResult result = collector.results.get(0);
        assertEquals("BLOCK", result.getDecision());
        assertEquals("ML_SCORING", result.getDecisionSource());
        assertEquals(1, result.getIsFraud());
    }

    // ═══════════════════════════════════════════════════════
    // Test 6: Moderate ML score (0.5 < score ≤ 0.85) → ALERT
    // ═══════════════════════════════════════════════════════
    @Test
    public void testModerateMlScore_ShouldAlert() throws Exception {
        Transaction tx = createBaseTx();
        tx.setMlScore(0.6);
        tx.setMlAvailable(true);

        TestCollector collector = new TestCollector();
        decisionMaker.flatMap(tx, collector);

        DecisionResult result = collector.results.get(0);
        assertEquals("ALERT", result.getDecision());
        assertEquals("ML_SCORING", result.getDecisionSource());
    }

    // ═══════════════════════════════════════════════════════
    // Test 7: Low ML score (≤ 0.5) → APPROVE
    // ═══════════════════════════════════════════════════════
    @Test
    public void testLowMlScore_ShouldApprove() throws Exception {
        Transaction tx = createBaseTx();
        tx.setMlScore(0.3);
        tx.setMlAvailable(true);

        TestCollector collector = new TestCollector();
        decisionMaker.flatMap(tx, collector);

        DecisionResult result = collector.results.get(0);
        assertEquals("APPROVE", result.getDecision());
        assertEquals("NONE", result.getDecisionSource());
    }

    // ═══════════════════════════════════════════════════════
    // Test 8: Rules > CEP priority — both flagged, rules wins
    // ═══════════════════════════════════════════════════════
    @Test
    public void testRulesTakesPriorityOverCep() throws Exception {
        Transaction tx = createBaseTx();

        // CEP flagged (non-critical)
        tx.setCepFlagged(true);
        List<FraudAlert> cepAlerts = new ArrayList<>();
        cepAlerts.add(new FraudAlert("TX-001", "ACC-001", "MICRO_TRANSACTIONS",
                "CEP", "HIGH", 0.85, "5 micro txns", "Smurfing", 123456789L));
        tx.setCepAlerts(cepAlerts);

        // Rule flagged (critical)
        tx.setRuleFlagged(true);
        List<RuleViolation> rules = new ArrayList<>();
        rules.add(new RuleViolation("RULE_001", "Blacklisted", "CRITICAL", 0.95, "blacklist match"));
        tx.setRuleAlerts(rules);

        TestCollector collector = new TestCollector();
        decisionMaker.flatMap(tx, collector);

        DecisionResult result = collector.results.get(0);
        assertEquals("BLOCK", result.getDecision());
        assertEquals("RULES_ENGINE", result.getDecisionSource());
    }

    // ═══════════════════════════════════════════════════════
    // Test 9: decidedAt is populated
    // ═══════════════════════════════════════════════════════
    @Test
    public void testDecidedAtIsPopulated() throws Exception {
        Transaction tx = createBaseTx();
        long before = System.currentTimeMillis();

        TestCollector collector = new TestCollector();
        decisionMaker.flatMap(tx, collector);

        long after = System.currentTimeMillis();
        DecisionResult result = collector.results.get(0);
        assertTrue("decidedAt should be between before and after",
                result.getDecidedAt() >= before && result.getDecidedAt() <= after);
    }

    // ═══════════════════════════════════════════════════════
    // Test 10: Non-critical rule → ALERT (not BLOCK)
    // ═══════════════════════════════════════════════════════
    @Test
    public void testNonCriticalRule_ShouldAlert() throws Exception {
        Transaction tx = createBaseTx();
        tx.setRuleFlagged(true);
        List<RuleViolation> rules = new ArrayList<>();
        rules.add(new RuleViolation("RULE_002", "Night Activity", "MEDIUM", 0.5, "time window 0-5"));
        tx.setRuleAlerts(rules);

        TestCollector collector = new TestCollector();
        decisionMaker.flatMap(tx, collector);

        DecisionResult result = collector.results.get(0);
        assertEquals("ALERT", result.getDecision());
        assertEquals("RULES_ENGINE", result.getDecisionSource());
    }
}
