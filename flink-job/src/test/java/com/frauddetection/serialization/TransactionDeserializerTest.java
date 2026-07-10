package com.frauddetection.serialization;

import com.frauddetection.model.Transaction;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for TransactionDeserializer.
 * Verifies JSON → Transaction POJO mapping matches the data simulator schema.
 */
public class TransactionDeserializerTest {

    private final TransactionDeserializer deserializer = new TransactionDeserializer();

    @Test
    public void testDeserializeValidTransaction() throws Exception {
        String json = "{" +
                "\"transaction_id\":\"txn_abc123\"," +
                "\"account_id\":\"acc_001\"," +
                "\"card_id\":\"card_001\"," +
                "\"timestamp\":1700000000000," +
                "\"produced_at\":1700000000100," +
                "\"amount\":1500.50," +
                "\"event_type\":\"TRANSFER\"," +
                "\"channel\":\"ONLINE\"," +
                "\"location\":\"{\\\"latitude\\\": 10.762, \\\"longitude\\\": 106.660}\"," +
                "\"status\":\"COMPLETED\"," +
                "\"merchant_id\":\"merchant_xyz\"," +
                "\"is_fraud\":0" +
                "}";

        Transaction tx = deserializer.deserialize(json.getBytes());

        assertNotNull(tx);
        assertEquals("txn_abc123", tx.getTransactionId());
        assertEquals("acc_001", tx.getAccountId());
        assertEquals("card_001", tx.getCardId());
        assertEquals(1700000000000L, tx.getTimestamp());
        assertEquals(1700000000100L, tx.getProducedAt());
        assertEquals(1500.50, tx.getAmount(), 0.001);
        assertEquals("TRANSFER", tx.getEventType());
        assertEquals("ONLINE", tx.getChannel());
        assertEquals("COMPLETED", tx.getStatus());
        assertEquals("merchant_xyz", tx.getMerchantId());
        assertEquals(0, tx.getIsFraud());
        // Location should be parsed
        assertEquals(10.762, tx.getLatitude(), 0.01);
        assertEquals(106.660, tx.getLongitude(), 0.01);
    }

    @Test
    public void testDeserializeFraudTransaction() throws Exception {
        String json = "{" +
                "\"transaction_id\":\"txn_fraud_001\"," +
                "\"account_id\":\"acc_002\"," +
                "\"timestamp\":1700000000000," +
                "\"amount\":50000.00," +
                "\"event_type\":\"WITHDRAWAL\"," +
                "\"is_fraud\":1" +
                "}";

        Transaction tx = deserializer.deserialize(json.getBytes());

        assertNotNull(tx);
        assertEquals("txn_fraud_001", tx.getTransactionId());
        assertEquals(1, tx.getIsFraud());
        assertEquals(50000.00, tx.getAmount(), 0.001);
    }

    @Test
    public void testDeserializeWithExtraFields() throws Exception {
        // Unknown fields should be ignored (@JsonIgnoreProperties)
        String json = "{" +
                "\"transaction_id\":\"txn_001\"," +
                "\"account_id\":\"acc_001\"," +
                "\"timestamp\":1700000000000," +
                "\"amount\":100.0," +
                "\"event_type\":\"LOGIN_SUCCESS\"," +
                "\"unknown_field\":\"should be ignored\"" +
                "}";

        Transaction tx = deserializer.deserialize(json.getBytes());

        assertNotNull(tx);
        assertEquals("txn_001", tx.getTransactionId());
    }

    @Test
    public void testDeserializeInvalidJson() throws Exception {
        // Should return null for invalid JSON
        Transaction tx = deserializer.deserialize("not json".getBytes());
        assertNull(tx);
    }

    @Test
    public void testEnrichmentFieldsDefaultValues() throws Exception {
        String json = "{\"transaction_id\":\"txn_001\",\"account_id\":\"acc_001\"," +
                "\"timestamp\":1700000000000,\"amount\":100.0,\"event_type\":\"TRANSFER\"}";

        Transaction tx = deserializer.deserialize(json.getBytes());

        assertNotNull(tx);
        // Enrichment fields should have defaults
        assertFalse(tx.isCepFlagged());
        assertFalse(tx.isRuleFlagged());
        assertEquals(0.0, tx.getMlScore(), 0.001);
        assertFalse(tx.isMlAvailable());
        assertTrue(tx.getCepAlerts().isEmpty());
        assertTrue(tx.getRuleAlerts().isEmpty());
        assertNull(tx.getAggFeatures());
    }
}
