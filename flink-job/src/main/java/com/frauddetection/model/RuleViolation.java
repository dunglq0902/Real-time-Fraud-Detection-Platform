package com.frauddetection.model;

import java.io.Serializable;

/**
 * RuleViolation — result of evaluating a FraudRule against a Transaction.
 * Embedded in Transaction.ruleAlerts for the Decision Engine.
 */
public class RuleViolation implements Serializable {

    private static final long serialVersionUID = 1L;

    private String ruleId;
    private String ruleName;
    private String severity;
    private double score;
    private String details;

    public RuleViolation() {
    }

    public RuleViolation(String ruleId, String ruleName, String severity,
            double score, String details) {
        this.ruleId = ruleId;
        this.ruleName = ruleName;
        this.severity = severity;
        this.score = score;
        this.details = details;
    }

    // ── Getters & Setters ─────────────────────────────────
    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    @Override
    public String toString() {
        return "RuleViolation{" +
                "ruleId='" + ruleId + '\'' +
                ", severity='" + severity + '\'' +
                ", score=" + score +
                '}';
    }
}
