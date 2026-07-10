package com.frauddetection.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.model.Transaction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Kafka DeserializationSchema for Transaction events.
 * Parses JSON bytes → Transaction POJO using Jackson.
 * Also parses the nested location JSON string to populate latitude/longitude.
 */
public class TransactionDeserializer implements DeserializationSchema<Transaction> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TransactionDeserializer.class);

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Transaction deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        try {
            Transaction tx = objectMapper.readValue(message, Transaction.class);
            // Parse the nested location JSON string → latitude/longitude
            tx.parseLocation();
            return tx;
        } catch (Exception e) {
            LOG.warn("Failed to deserialize transaction: {}", new String(message), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Transaction nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}
