package com.pinterest.psc.producer;

public class PscProducerTransactionalProperties {

    protected long producerId;
    protected short epoch;

    public PscProducerTransactionalProperties(long producerId, short epoch) {
        this.producerId = producerId;
        this.epoch = epoch;
    }

    public long getProducerId() {
        return producerId;
    }

    public short getEpoch() {
        return epoch;
    }
}
