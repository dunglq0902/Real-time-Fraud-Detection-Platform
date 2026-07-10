package com.frauddetection.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Transaction POJO — core data model flowing through the Flink pipeline.
 *
 * <p>Fields are populated in stages (matching pipeline order):
 * <ol>
 *   <li>Kafka Source: transactionId, accountId, cardId, timestamp, amount, eventType, etc.</li>
 *   <li>Rules Engine: ruleAlerts, ruleFlagged</li>
 *   <li>CEP Engine + CepAlertMerger: cepAlerts, cepFlagged</li>
 *   <li>Feature Aggregator: aggFeatures</li>
 *   <li>ML Scorer: mlScore, mlAvailable</li>
 * </ol>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction implements Serializable {

    private static final long serialVersionUID = 1L;

    // ── From Kafka Source ──────────────────────────────────
    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("account_id")
    private String accountId;

    @JsonProperty("card_id")
    private String cardId;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("produced_at")
    private long producedAt;

    @JsonProperty("amount")
    private double amount;

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("channel")
    private String channel;

    @JsonProperty("location")
    private String location; // JSON string: {"latitude": ..., "longitude": ...}

    @JsonProperty("status")
    private String status;

    @JsonProperty("merchant_id")
    private String merchantId;

    @JsonProperty("is_fraud")
    private int isFraud;

    // ── Parsed location fields ────────────────────────────
    private double latitude;
    private double longitude;

    // ── Enrichment fields (populated during pipeline) ─────
    private List<FraudAlert> cepAlerts = new ArrayList<>();
    private boolean cepFlagged = false;
    private boolean lateAlert = false;

    private List<RuleViolation> ruleAlerts = new ArrayList<>();
    private List<FraudAlert> ruleAlertObjects = new ArrayList<>();
    private boolean ruleFlagged = false;

    private AggregateFeatures aggFeatures;

    private double mlScore = 0.0;
    private boolean mlAvailable = false;

    // ── Constructors ──────────────────────────────────────
    public Transaction() {}

    /**
     * Copy constructor — creates a deep-enough copy of another Transaction.
     * Primitive fields are copied by value; list fields are defensively copied
     * to prevent shared-state mutation in Flink operators.
     *
     * @param other the transaction to copy
     */
    public Transaction(Transaction other) {
        this.transactionId = other.transactionId;
        this.accountId = other.accountId;
        this.cardId = other.cardId;
        this.timestamp = other.timestamp;
        this.producedAt = other.producedAt;
        this.amount = other.amount;
        this.eventType = other.eventType;
        this.channel = other.channel;
        this.location = other.location;
        this.status = other.status;
        this.merchantId = other.merchantId;
        this.isFraud = other.isFraud;
        this.latitude = other.latitude;
        this.longitude = other.longitude;
        this.cepAlerts = other.cepAlerts != null ? new ArrayList<>(other.cepAlerts) : new ArrayList<>();
        this.cepFlagged = other.cepFlagged;
        this.lateAlert = other.lateAlert;
        this.ruleAlerts = other.ruleAlerts != null ? new ArrayList<>(other.ruleAlerts) : new ArrayList<>();
        this.ruleAlertObjects = other.ruleAlertObjects != null ? new ArrayList<>(other.ruleAlertObjects) : new ArrayList<>();
        this.ruleFlagged = other.ruleFlagged;
        this.aggFeatures = other.aggFeatures;
        this.mlScore = other.mlScore;
        this.mlAvailable = other.mlAvailable;
    }

    // ── Location Parsing ──────────────────────────────────
    /**
     * Parse the JSON location string and populate latitude/longitude fields.
     * Called after deserialization.
     *
     * <p><b>Performance:</b> Uses manual string parsing instead of ObjectMapper
     * to avoid creating a heavy JSON parser object per transaction (~1000x/s).
     * Location format is always: {"latitude": X, "longitude": Y}
     */
    public void parseLocation() {
        if (location != null && !location.isEmpty()) {
            try {
                // Manual parsing for simple JSON: {"latitude": 10.123, "longitude": 106.456}
                this.latitude = extractJsonDouble(location, "latitude");
                this.longitude = extractJsonDouble(location, "longitude");
            } catch (Exception e) {
                this.latitude = 0.0;
                this.longitude = 0.0;
            }
        }
    }

    /**
     * Fast extraction of a double value from a simple JSON string.
     * Avoids ObjectMapper overhead for the common {key: value} pattern.
     */
    private static double extractJsonDouble(String json, String key) {
        int keyIdx = json.indexOf("\"" + key + "\"");
        if (keyIdx < 0) return 0.0;
        int colonIdx = json.indexOf(':', keyIdx + key.length() + 2);
        if (colonIdx < 0) return 0.0;
        int start = colonIdx + 1;
        // Skip whitespace
        while (start < json.length() && json.charAt(start) == ' ') start++;
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end))
                || json.charAt(end) == '.' || json.charAt(end) == '-')) {
            end++;
        }
        if (end > start) {
            return Double.parseDouble(json.substring(start, end));
        }
        return 0.0;
    }

    // ── Getters & Setters ─────────────────────────────────
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }

    public String getCardId() { return cardId; }
    public void setCardId(String cardId) { this.cardId = cardId; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public long getProducedAt() { return producedAt; }
    public void setProducedAt(long producedAt) { this.producedAt = producedAt; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }

    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }

    public int getIsFraud() { return isFraud; }
    public void setIsFraud(int isFraud) { this.isFraud = isFraud; }

    public double getLatitude() { return latitude; }
    public void setLatitude(double latitude) { this.latitude = latitude; }

    public double getLongitude() { return longitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }

    public List<FraudAlert> getCepAlerts() { return cepAlerts; }
    public void setCepAlerts(List<FraudAlert> cepAlerts) { this.cepAlerts = cepAlerts; }

    public boolean isCepFlagged() { return cepFlagged; }
    public void setCepFlagged(boolean cepFlagged) { this.cepFlagged = cepFlagged; }

    public boolean isLateAlert() { return lateAlert; }
    public void setLateAlert(boolean lateAlert) { this.lateAlert = lateAlert; }

    public List<RuleViolation> getRuleAlerts() { return ruleAlerts; }
    public void setRuleAlerts(List<RuleViolation> ruleAlerts) { this.ruleAlerts = ruleAlerts; }

    public List<FraudAlert> getRuleAlertObjects() { return ruleAlertObjects; }
    public void setRuleAlertObjects(List<FraudAlert> ruleAlertObjects) { this.ruleAlertObjects = ruleAlertObjects; }

    public boolean isRuleFlagged() { return ruleFlagged; }
    public void setRuleFlagged(boolean ruleFlagged) { this.ruleFlagged = ruleFlagged; }

    public AggregateFeatures getAggFeatures() { return aggFeatures; }
    public void setAggFeatures(AggregateFeatures aggFeatures) { this.aggFeatures = aggFeatures; }

    public double getMlScore() { return mlScore; }
    public void setMlScore(double mlScore) { this.mlScore = mlScore; }

    public boolean isMlAvailable() { return mlAvailable; }
    public void setMlAvailable(boolean mlAvailable) { this.mlAvailable = mlAvailable; }

    // ── Short-circuit Helper ─────────────────────────────
    /**
     * Check if this transaction has already been flagged as CRITICAL by
     * the Rules Engine or CEP Engine. Used to short-circuit downstream
     * processing (FeatureAggregator, OnnxModelScorer) — no need to waste
     * CPU on ML inference for transactions that are already confirmed fraud.
     *
     * @return true if any CRITICAL rule violation or CRITICAL CEP alert exists
     */
    public boolean hasCriticalAlert() {
        // Check Rules Engine — CRITICAL severity
        if (ruleFlagged && ruleAlerts != null) {
            for (RuleViolation rv : ruleAlerts) {
                if ("CRITICAL".equals(rv.getSeverity())) {
                    return true;
                }
            }
        }
        // Check CEP Engine — CRITICAL severity
        if (cepFlagged && cepAlerts != null) {
            for (FraudAlert fa : cepAlerts) {
                if ("CRITICAL".equals(fa.getSeverity())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "Transaction{" +
               "transactionId='" + transactionId + '\'' +
               ", accountId='" + accountId + '\'' +
               ", amount=" + amount +
               ", eventType='" + eventType + '\'' +
               ", timestamp=" + timestamp +
               '}';
    }
}
