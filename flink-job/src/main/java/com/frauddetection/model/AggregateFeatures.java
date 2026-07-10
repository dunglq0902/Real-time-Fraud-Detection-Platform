package com.frauddetection.model;

import java.io.Serializable;

/**
 * AggregateFeatures — computed per-account features for ML scoring.
 * Calculated by FeatureAggregator using Flink keyed state.
 */
public class AggregateFeatures implements Serializable {

    private static final long serialVersionUID = 1L;

    private int txCount1h; // Number of transactions in last 1 hour
    private int txCount24h; // Number of transactions in last 24 hours
    private double avgAmount24h; // Average amount in last 24 hours
    private double amountDeviation; // Deviation of current amount from 24h average
    private long timeSinceLastMs; // Milliseconds since last transaction
    private double distanceKm; // Distance in km from last transaction

    public AggregateFeatures() {
    }

    public AggregateFeatures(int txCount1h, int txCount24h, double avgAmount24h,
            double amountDeviation, long timeSinceLastMs) {
        this.txCount1h = txCount1h;
        this.txCount24h = txCount24h;
        this.avgAmount24h = avgAmount24h;
        this.amountDeviation = amountDeviation;
        this.timeSinceLastMs = timeSinceLastMs;
        this.distanceKm = 0.0;
    }

    public AggregateFeatures(int txCount1h, int txCount24h, double avgAmount24h,
            double amountDeviation, long timeSinceLastMs, double distanceKm) {
        this.txCount1h = txCount1h;
        this.txCount24h = txCount24h;
        this.avgAmount24h = avgAmount24h;
        this.amountDeviation = amountDeviation;
        this.timeSinceLastMs = timeSinceLastMs;
        this.distanceKm = distanceKm;
    }

    // ── Getters & Setters ─────────────────────────────────
    public int getTxCount1h() {
        return txCount1h;
    }

    public void setTxCount1h(int txCount1h) {
        this.txCount1h = txCount1h;
    }

    public int getTxCount24h() {
        return txCount24h;
    }

    public void setTxCount24h(int txCount24h) {
        this.txCount24h = txCount24h;
    }

    public double getAvgAmount24h() {
        return avgAmount24h;
    }

    public void setAvgAmount24h(double avgAmount24h) {
        this.avgAmount24h = avgAmount24h;
    }

    public double getAmountDeviation() {
        return amountDeviation;
    }

    public void setAmountDeviation(double amountDeviation) {
        this.amountDeviation = amountDeviation;
    }

    public long getTimeSinceLastMs() {
        return timeSinceLastMs;
    }

    public void setTimeSinceLastMs(long timeSinceLastMs) {
        this.timeSinceLastMs = timeSinceLastMs;
    }

    public double getDistanceKm() {
        return distanceKm;
    }

    public void setDistanceKm(double distanceKm) {
        this.distanceKm = distanceKm;
    }

    @Override
    public String toString() {
        return "AggregateFeatures{" +
                "txCount1h=" + txCount1h +
                ", txCount24h=" + txCount24h +
                ", avgAmount24h=" + avgAmount24h +
                ", amountDeviation=" + amountDeviation +
                ", timeSinceLastMs=" + timeSinceLastMs +
                ", distanceKm=" + distanceKm +
                '}';
    }
}
