package com.pinterest.psc.producer.transaction;

import com.pinterest.psc.producer.PscProducerTransactionalProperties;

import java.util.concurrent.Future;

public interface TransactionManagerOperator {

    short getEpoch(Object transactionManager);

    String getTransactionId(Object transactionManager);

    long getProducerId(Object transactionManager);

    void setEpoch(Object transactionManager, short epoch);

    void setTransactionId(Object transactionManager, String transactionId);

    void setProducerId(Object transactionManager, long producerId);

    Future<Boolean> enqueueInFlightTransactions(Object transactionManager);

    void resumeTransaction(Object transactionManager, PscProducerTransactionalProperties transactionalProperties);
}
