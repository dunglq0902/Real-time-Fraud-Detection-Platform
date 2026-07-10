package com.frauddetection.model;

import java.io.Serializable;

/**
 * DecisionResult — final output record for ClickHouse and Kafka.
 * Maps 1:1 to the fraud_detection.transactions table schema.
 */
public class DecisionResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private String transactionId;
    private String accountId;
    private String cardId;
    private long eventTime; // original transaction timestamp (ms)
    private double amount;
    private String eventType;
    private String channel;
    private double latitude;
    private double longitude;
    private String merchantId;
    private String status; // COMPLETED
    private double mlScore;
    private String ruleTriggered; // comma-separated rule IDs
    private int isFraud; // detected fraud flag: 0 or 1
    private int groundTruthIsFraud; // simulator label: 0 or 1
    private String decision; // APPROVE, ALERT, BLOCK
    private String decisionSource; // NONE, RULES_ENGINE, CEP_ENGINE, ML_SCORING
    private double combinedScore;
    private long producedAt; // Kafka produce timestamp (ms)
    private long decidedAt; // Decision timestamp (ms)

    public DecisionResult() {
    }

    // ── Getters & Setters ─────────────────────────────────
    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getCardId() {
        return cardId;
    }

    public void setCardId(String cardId) {
        this.cardId = cardId;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public double getMlScore() {
        return mlScore;
    }

    public void setMlScore(double mlScore) {
        this.mlScore = mlScore;
    }

    public String getRuleTriggered() {
        return ruleTriggered;
    }

    public void setRuleTriggered(String ruleTriggered) {
        this.ruleTriggered = ruleTriggered;
    }

    public int getIsFraud() {
        return isFraud;
    }

    public void setIsFraud(int isFraud) {
        this.isFraud = isFraud;
    }

    public int getGroundTruthIsFraud() {
        return groundTruthIsFraud;
    }

    public void setGroundTruthIsFraud(int groundTruthIsFraud) {
        this.groundTruthIsFraud = groundTruthIsFraud;
    }

    public String getDecision() {
        return decision;
    }

    public void setDecision(String decision) {
        this.decision = decision;
    }

    public String getDecisionSource() {
        return decisionSource;
    }

    public void setDecisionSource(String decisionSource) {
        this.decisionSource = decisionSource;
    }

    public double getCombinedScore() {
        return combinedScore;
    }

    public void setCombinedScore(double combinedScore) {
        this.combinedScore = combinedScore;
    }

    public long getProducedAt() {
        return producedAt;
    }

    public void setProducedAt(long producedAt) {
        this.producedAt = producedAt;
    }

    public long getDecidedAt() {
        return decidedAt;
    }

    public void setDecidedAt(long decidedAt) {
        this.decidedAt = decidedAt;
    }

    @Override
    public String toString() {
        return "DecisionResult{" +
                "transactionId='" + transactionId + '\'' +
                ", decision='" + decision + '\'' +
                ", decisionSource='" + decisionSource + '\'' +
                ", combinedScore=" + combinedScore +
                '}';
    }
}
