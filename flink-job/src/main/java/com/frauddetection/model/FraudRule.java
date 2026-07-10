package com.frauddetection.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * FraudRule — dynamic rule received from Kafka rules topic.
 *
 * <p>
 * Rules are broadcast to all Flink task managers via Broadcast State.
 * Each rule defines a condition to evaluate against transaction fields.
 *
 * <p>
 * JSON example from simulator:
 * 
 * <pre>
 * {
 *   "rule_id": "RULE_001",
 *   "rule_name": "High Amount Threshold",
 *   "field": "amount",
 *   "operator": "GREATER_THAN",
 *   "threshold": 10000.0,
 *   "severity": "HIGH",
 *   "version": 1,
 *   "active": true,
 *   "time_window": {"start_hour": 0, "end_hour": 5}
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FraudRule implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("rule_id")
    private String ruleId;

    @JsonProperty("rule_name")
    private String ruleName;

    @JsonProperty("field")
    private String field;

    @JsonProperty("operator")
    private String operator; // GREATER_THAN, LESS_THAN, EQUALS, IN, NOT_IN

    @JsonProperty("threshold")
    private Object threshold; // Can be Number, String, or List<String>

    @JsonProperty("severity")
    private String severity; // CRITICAL, HIGH, MEDIUM, LOW

    @JsonProperty("version")
    private int version;

    @JsonProperty("active")
    private boolean active = true;

    @JsonProperty("time_window")
    private Map<String, Integer> timeWindow; // {"start_hour": 0, "end_hour": 5}

    public FraudRule() {
    }

    // ── Threshold helpers ─────────────────────────────────

    /**
     * Get threshold as a double (for numeric comparisons).
     */
    public double getThresholdAsDouble() {
        if (threshold instanceof Number) {
            return ((Number) threshold).doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(threshold));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    /**
     * Get threshold as a list of strings (for IN/NOT_IN operators).
     */
    @SuppressWarnings("unchecked")
    public List<String> getThresholdAsList() {
        if (threshold instanceof List) {
            return (List<String>) threshold;
        }
        return List.of(String.valueOf(threshold));
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

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public Object getThreshold() {
        return threshold;
    }

    public void setThreshold(Object threshold) {
        this.threshold = threshold;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public Map<String, Integer> getTimeWindow() {
        return timeWindow;
    }

    public void setTimeWindow(Map<String, Integer> timeWindow) {
        this.timeWindow = timeWindow;
    }

    @Override
    public String toString() {
        return "FraudRule{" +
                "ruleId='" + ruleId + '\'' +
                ", ruleName='" + ruleName + '\'' +
                ", field='" + field + '\'' +
                ", operator='" + operator + '\'' +
                ", threshold=" + threshold +
                ", severity='" + severity + '\'' +
                ", version=" + version +
                ", active=" + active +
                '}';
    }
}
