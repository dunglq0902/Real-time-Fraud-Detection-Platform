package com.frauddetection.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Kafka DeserializationSchema for FraudRule events from the rules topic.
 * Returns raw JSON string (not POJO) because RulesEngine stores raw JSON in BroadcastState.
 */
public class FraudRuleDeserializer implements DeserializationSchema<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String deserialize(byte[] message) throws IOException {
        if (message == null) return null;
        return new String(message, java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
