package com.frauddetection.rules;

import com.frauddetection.model.FraudRule;
import com.frauddetection.model.Transaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for FraudRule POJO — verifies JSON parsing and threshold helpers.
 * (RulesEngine itself requires Flink runtime; these tests verify the rule model logic.)
 */
public class RulesEngineModelTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testParseRuleFromJson() throws Exception {
        String json = "{" +
                "\"rule_id\":\"RULE_001\"," +
                "\"rule_name\":\"High Amount\"," +
                "\"field\":\"amount\"," +
                "\"operator\":\"GREATER_THAN\"," +
                "\"threshold\":10000.0," +
                "\"severity\":\"CRITICAL\"," +
                "\"version\":1," +
                "\"active\":true" +
                "}";

        FraudRule rule = mapper.readValue(json, FraudRule.class);

        assertEquals("RULE_001", rule.getRuleId());
        assertEquals("High Amount", rule.getRuleName());
        assertEquals("amount", rule.getField());
        assertEquals("GREATER_THAN", rule.getOperator());
        assertEquals(10000.0, rule.getThresholdAsDouble(), 0.001);
        assertEquals("CRITICAL", rule.getSeverity());
        assertEquals(1, rule.getVersion());
        assertTrue(rule.isActive());
    }

    @Test
    public void testParseRuleWithTimeWindow() throws Exception {
        String json = "{" +
                "\"rule_id\":\"RULE_002\"," +
                "\"rule_name\":\"Night Activity\"," +
                "\"field\":\"amount\"," +
                "\"operator\":\"GREATER_THAN\"," +
                "\"threshold\":5000," +
                "\"severity\":\"HIGH\"," +
                "\"version\":1," +
                "\"active\":true," +
                "\"time_window\":{\"start_hour\":0,\"end_hour\":5}" +
                "}";

        FraudRule rule = mapper.readValue(json, FraudRule.class);

        assertNotNull(rule.getTimeWindow());
        assertEquals(0, (int) rule.getTimeWindow().get("start_hour"));
        assertEquals(5, (int) rule.getTimeWindow().get("end_hour"));
    }

    @Test
    public void testParseRuleWithListThreshold() throws Exception {
        String json = "{" +
                "\"rule_id\":\"RULE_003\"," +
                "\"rule_name\":\"Channel Whitelist\"," +
                "\"field\":\"channel\"," +
                "\"operator\":\"NOT_IN\"," +
                "\"threshold\":[\"ATM\",\"POS\"]," +
                "\"severity\":\"MEDIUM\"," +
                "\"version\":1," +
                "\"active\":true" +
                "}";

        FraudRule rule = mapper.readValue(json, FraudRule.class);

        assertEquals("NOT_IN", rule.getOperator());
        List<String> thresholdList = rule.getThresholdAsList();
        assertEquals(2, thresholdList.size());
        assertTrue(thresholdList.contains("ATM"));
        assertTrue(thresholdList.contains("POS"));
    }

    @Test
    public void testInactiveRule() throws Exception {
        String json = "{" +
                "\"rule_id\":\"RULE_004\"," +
                "\"rule_name\":\"Disabled Rule\"," +
                "\"field\":\"amount\"," +
                "\"operator\":\"GREATER_THAN\"," +
                "\"threshold\":1000," +
                "\"severity\":\"LOW\"," +
                "\"version\":2," +
                "\"active\":false" +
                "}";

        FraudRule rule = mapper.readValue(json, FraudRule.class);
        assertFalse(rule.isActive());
    }

    @Test
    public void testTransactionFieldAccess() {
        Transaction tx = new Transaction();
        tx.setTransactionId("TX-001");
        tx.setAccountId("ACC-001");
        tx.setCardId("CARD-001");
        tx.setAmount(5000.0);
        tx.setEventType("TRANSFER");
        tx.setChannel("ONLINE");
        tx.setMerchantId("MERCHANT_001");
        tx.setStatus("COMPLETED");

        assertEquals("TX-001", tx.getTransactionId());
        assertEquals(5000.0, tx.getAmount(), 0.001);
        assertEquals("TRANSFER", tx.getEventType());
        assertEquals("ONLINE", tx.getChannel());
    }

    @Test
    public void testTransactionLocationParsing() {
        Transaction tx = new Transaction();
        tx.setLocation("{\"latitude\": 10.762622, \"longitude\": 106.660172}");
        tx.parseLocation();

        assertEquals(10.762622, tx.getLatitude(), 0.0001);
        assertEquals(106.660172, tx.getLongitude(), 0.0001);
    }

    @Test
    public void testTransactionNullLocation() {
        Transaction tx = new Transaction();
        tx.setLocation(null);
        tx.parseLocation();

        assertEquals(0.0, tx.getLatitude(), 0.001);
        assertEquals(0.0, tx.getLongitude(), 0.001);
    }
}
