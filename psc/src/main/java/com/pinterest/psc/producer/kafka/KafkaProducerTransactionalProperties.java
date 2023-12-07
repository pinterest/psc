package com.pinterest.psc.producer.kafka;

import com.pinterest.psc.producer.PscProducerTransactionalProperties;

/**
 * Moved to PscProducerTransactionalProperties
 */
@Deprecated
public class KafkaProducerTransactionalProperties extends PscProducerTransactionalProperties {

    public KafkaProducerTransactionalProperties(long producerId, short epoch) {
        super(producerId, epoch);
    }

    public long getProducerId() {
        return producerId;
    }

    public short getEpoch() {
        return epoch;
    }

    @Override
    public String toString() {
        return "KafkaProducerTransactionalProperties {" +
                "producerId=" + producerId +
                ", epoch=" + epoch +
                '}';
    }
}
