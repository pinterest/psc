package com.pinterest.psc.producer.transaction;

import com.pinterest.psc.producer.PscProducerTransactionalProperties;
import com.pinterest.psc.producer.transaction.kafka.KafkaTransactionManagerOperator;
import org.apache.kafka.clients.producer.internals.TransactionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * This class is used to abstract the differences between different transaction managers and unify the operations on them.
 * The operations supported here generally require direct field access or reflections on the transaction manager object,
 * which is necessary for certain operations like setting the producer ID, epoch, or transaction ID.
 *
 * For regular transaction operations like beginTransaction, commitTransaction, and abortTransaction, they are not included here
 * because they should be directly accessible via public APIs of the backend producer implementation.
 *
 * Each backend PubSub implementation should provide a {@link TransactionManagerOperator} implementation to support the operations,
 * and register it in the TXN_MANAGER_CLASSNAME_TO_OPERATOR map.
 */
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

    /**
     * Get the epoch of the transaction manager.
     *
     * @param transactionManager the transaction manager object
     * @return the epoch of the transaction manager
     */
    public static short getEpoch(Object transactionManager) {
        return getOrCreateTransactionManagerOperator(transactionManager).getEpoch(transactionManager);
    }

    /**
     * Get the producer ID of the transaction manager.
     *
     * @param transactionManager the transaction manager object
     * @return the producer ID of the transaction manager
     */
    public static long getProducerId(Object transactionManager) {
        return getOrCreateTransactionManagerOperator(transactionManager).getProducerId(transactionManager);
    }

    /**
     * Get the transaction ID of the transaction manager.
     *
     * @param transactionManager the transaction manager object
     * @return the transaction ID of the transaction manager
     */
    public static String getTransactionId(Object transactionManager) {
        return getOrCreateTransactionManagerOperator(transactionManager).getTransactionId(transactionManager);
    }

    /**
     * Set the epoch of the transaction manager.
     *
     * @param transactionManager the transaction manager object
     * @param epoch the epoch to set
     */
    public static void setEpoch(Object transactionManager, short epoch) {
        getOrCreateTransactionManagerOperator(transactionManager).setEpoch(transactionManager, epoch);
    }

    /**
     * Set the producer ID of the transaction manager.
     *
     * @param transactionManager the transaction manager object
     * @param producerId the producer ID to set
     */
    public static void setProducerId(Object transactionManager, long producerId) {
        getOrCreateTransactionManagerOperator(transactionManager).setProducerId(transactionManager, producerId);
    }

    /**
     * Set the transaction ID of the transaction manager.
     *
     * @param transactionManager the transaction manager object
     * @param transactionalId the transaction ID to set
     */
    public static void setTransactionId(Object transactionManager, String transactionalId) {
        getOrCreateTransactionManagerOperator(transactionManager).setTransactionId(transactionManager, transactionalId);
    }

    /**
     * Enqueue in-flight transactions in the transaction manager.
     *
     * @param transactionManager the transaction manager object
     * @return a future that completes when the in-flight transactions are enqueued
     */
    public static Future<Boolean> enqueueInFlightTransactions(Object transactionManager) {
        return getOrCreateTransactionManagerOperator(transactionManager).enqueueInFlightTransactions(transactionManager);
    }

    /**
     * Resume the transaction in the transaction manager with the given transactional properties. Typically,
     * this is used to resume a transaction with the same transaction ID and producer ID after a previous transaction
     * has been aborted or failed.
     *
     * @param transactionManager the transaction manager object
     * @param transactionalProperties the transactional properties
     */
    public static void resumeTransaction(Object transactionManager, PscProducerTransactionalProperties transactionalProperties) {
        getOrCreateTransactionManagerOperator(transactionManager).resumeTransaction(transactionManager, transactionalProperties);
    }
}
