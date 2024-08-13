package com.pinterest.psc.producer.transaction;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.producer.PscProducerTransactionalProperties;
import org.apache.kafka.clients.producer.internals.TransactionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class TransactionManagerUtils {

    private static final Map<String, TransactionManagerOperator> TXN_MANAGER_CLASSNAME_TO_OPERATOR = new HashMap<>();

    private static TransactionManagerOperator getOrCreateTransactionManagerOperator(Object transactionManager) {
        return TXN_MANAGER_CLASSNAME_TO_OPERATOR.computeIfAbsent(transactionManager.getClass().getName(), className -> {
            if (className.equals(TransactionManager.class.getName())) {
                return new KafkaTransactionManagerOperator();
            }
            throw new IllegalArgumentException("Unsupported transaction manager class: " + className);
        });
    }

    public static short getEpoch(Object transactionManager) {
        return getOrCreateTransactionManagerOperator(transactionManager).getEpoch(transactionManager);
    }

    public static long getProducerId(Object transactionManager) {
        return getOrCreateTransactionManagerOperator(transactionManager).getProducerId(transactionManager);
    }

    public static String getTransactionId(Object transactionManager) {
        return getOrCreateTransactionManagerOperator(transactionManager).getTransactionId(transactionManager);
    }

    public static void setEpoch(Object transactionManager, short epoch) {
        getOrCreateTransactionManagerOperator(transactionManager).setEpoch(transactionManager, epoch);
    }

    public static void setProducerId(Object transactionManager, long producerId) {
        getOrCreateTransactionManagerOperator(transactionManager).setProducerId(transactionManager, producerId);
    }

    public static void setTransactionId(Object transactionManager, String transactionalId) {
        getOrCreateTransactionManagerOperator(transactionManager).setTransactionId(transactionManager, transactionalId);
    }

    public static Future<Boolean> enqueueInFlightTransactions(Object transactionManager) {
        return getOrCreateTransactionManagerOperator(transactionManager).enqueueInFlightTransactions(transactionManager);
    }

    public static void resumeTransaction(Object transactionManager, PscProducerTransactionalProperties transactionalProperties) {
        getOrCreateTransactionManagerOperator(transactionManager).resumeTransaction(transactionManager, transactionalProperties);
    }


}
