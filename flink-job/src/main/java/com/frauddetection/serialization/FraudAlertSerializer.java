package com.frauddetection.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.model.FraudAlert;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka SerializationSchema for FraudAlert.
 * Serializes to JSON bytes for the fraud-alerts Kafka topic.
 */
public class FraudAlertSerializer implements SerializationSchema<FraudAlert> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FraudAlertSerializer.class);

    private transient ObjectMapper objectMapper;

    @Override
    public byte[] serialize(FraudAlert element) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to serialize FraudAlert: {}", e.getMessage());
            return new byte[0];
        }
    }
}
