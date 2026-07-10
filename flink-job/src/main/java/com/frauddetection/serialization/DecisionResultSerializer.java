package com.frauddetection.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.model.DecisionResult;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka SerializationSchema for DecisionResult.
 * Serializes to JSON bytes. Currently unused — the pipeline uses
 * {@link FraudAlertSerializer} for the Kafka fraud-alerts topic
 * and custom HTTP JSON for ClickHouse sinks.
 */
public class DecisionResultSerializer implements SerializationSchema<DecisionResult> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DecisionResultSerializer.class);

    private transient ObjectMapper objectMapper;

    @Override
    public byte[] serialize(DecisionResult element) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to serialize DecisionResult: {}", e.getMessage());
            return new byte[0];
        }
    }
}
