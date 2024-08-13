package com.pinterest.psc.producer.transaction;

import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.producer.PscProducerTransactionalProperties;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaTransactionManagerOperator implements TransactionManagerOperator {

    private static final String KAFKA_TXN_MANAGER_PRODUCER_ID_AND_EPOCH_FIELD_NAME = "producerIdAndEpoch";
    private static final String TRANSACTION_MANAGER_STATE_ENUM =
            "org.apache.kafka.clients.producer.internals.TransactionManager$State";

    private ProducerIdAndEpoch getProducerIdAndEpoch(Object transactionManager) {
        ProducerIdAndEpoch producerIdAndEpoch = (ProducerIdAndEpoch) PscCommon.getField(transactionManager, KAFKA_TXN_MANAGER_PRODUCER_ID_AND_EPOCH_FIELD_NAME);
        if (producerIdAndEpoch == null) {
            throw new IllegalStateException("ProducerIdAndEpoch is null");
        }
        return producerIdAndEpoch;
    }

    @Override
    public short getEpoch(Object transactionManager) {
        ProducerIdAndEpoch producerIdAndEpoch = getProducerIdAndEpoch(transactionManager);
        return (short) PscCommon.getField(producerIdAndEpoch, "epoch");
    }

    @Override
    public String getTransactionId(Object transactionManager) {
        return (String) PscCommon.getField(transactionManager, "transactionalId");
    }

    @Override
    public long getProducerId(Object transactionManager) {
        ProducerIdAndEpoch producerIdAndEpoch = getProducerIdAndEpoch(transactionManager);
        return (long) PscCommon.getField(producerIdAndEpoch, "producerId");
    }

    @Override
    public void setEpoch(Object transactionManager, short epoch) {
        ProducerIdAndEpoch producerIdAndEpoch = getProducerIdAndEpoch(transactionManager);
        PscCommon.setField(producerIdAndEpoch, "epoch", epoch);
    }

    @Override
    public void setTransactionId(Object transactionManager, String transactionId) {
        PscCommon.setField(transactionManager, "transactionalId", transactionId);
        PscCommon.setField(
                transactionManager,
                "currentState",
                getTransactionManagerState("UNINITIALIZED"));
    }

    @Override
    public void setProducerId(Object transactionManager, long producerId) {
        ProducerIdAndEpoch producerIdAndEpoch = getProducerIdAndEpoch(transactionManager);
        PscCommon.setField(producerIdAndEpoch, "producerId", producerId);
    }

    @Override
    public Future<Boolean> enqueueInFlightTransactions(Object transactionManager) {
        TransactionalRequestResult result = enqueueNewPartitions(transactionManager);
        return new Future<Boolean>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return result.isCompleted();
            }

            @Override
            public Boolean get() {
                result.await();
                return result.isSuccessful();
            }

            @Override
            public Boolean get(long timeout, TimeUnit unit) {
                result.await(timeout, unit);
                return result.isSuccessful();
            }
        };
    }

    private Enum<?> getTransactionManagerState(String enumName) {
        try {
            Class<Enum> cl = (Class<Enum>) Class.forName(TRANSACTION_MANAGER_STATE_ENUM);
            return Enum.valueOf(cl, enumName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    /**
     * Enqueues new transactions at the transaction manager and returns a {@link
     * TransactionalRequestResult} that allows waiting on them.
     *
     * <p>If there are no new transactions we return a {@link TransactionalRequestResult} that is
     * already done.
     */
    private TransactionalRequestResult enqueueNewPartitions(Object transactionManager) {
        Object newPartitionsInTransaction =
                PscCommon.getField(transactionManager, "newPartitionsInTransaction");
        Object newPartitionsInTransactionIsEmpty =
                PscCommon.invoke(newPartitionsInTransaction, "isEmpty");
        TransactionalRequestResult result;
        if (newPartitionsInTransactionIsEmpty instanceof Boolean
                && !((Boolean) newPartitionsInTransactionIsEmpty)) {
            Object txnRequestHandler =
                    PscCommon.invoke(transactionManager, "addPartitionsToTransactionHandler");
            PscCommon.invoke(
                    transactionManager,
                    "enqueueRequest",
                    new Class[] {txnRequestHandler.getClass().getSuperclass()},
                    new Object[] {txnRequestHandler});
            result =
                    (TransactionalRequestResult)
                            PscCommon.getField(
                                    txnRequestHandler,
                                    txnRequestHandler.getClass().getSuperclass(),
                                    "result");
        } else {
            // we don't have an operation but this operation string is also used in
            // addPartitionsToTransactionHandler.
            result = new TransactionalRequestResult("AddPartitionsToTxn");
            result.done();
        }
        return result;
    }

    @Override
    public void resumeTransaction(Object transactionManager, PscProducerTransactionalProperties transactionalProperties) {
        Object topicPartitionBookkeeper =
                PscCommon.getField(transactionManager, "topicPartitionBookkeeper");

        transitionTransactionManagerStateTo(transactionManager, "INITIALIZING");
        PscCommon.invoke(topicPartitionBookkeeper, "reset");

        PscCommon.setField(
                transactionManager,
                KAFKA_TXN_MANAGER_PRODUCER_ID_AND_EPOCH_FIELD_NAME,
                createProducerIdAndEpoch(transactionalProperties.getProducerId(), transactionalProperties.getEpoch()));

        transitionTransactionManagerStateTo(transactionManager, "READY");

        transitionTransactionManagerStateTo(transactionManager, "IN_TRANSACTION");
        PscCommon.setField(transactionManager, "transactionStarted", true);
    }

    private ProducerIdAndEpoch createProducerIdAndEpoch(long producerId, short epoch) {
        try {
            Field field =
                    TransactionManager.class.getDeclaredField(KAFKA_TXN_MANAGER_PRODUCER_ID_AND_EPOCH_FIELD_NAME);
            Class<?> clazz = field.getType();
            Constructor<?> constructor = clazz.getDeclaredConstructor(Long.TYPE, Short.TYPE);
            constructor.setAccessible(true);
            return (ProducerIdAndEpoch) constructor.newInstance(producerId, epoch);
        } catch (InvocationTargetException
                 | InstantiationException
                 | IllegalAccessException
                 | NoSuchFieldException
                 | NoSuchMethodException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    private void transitionTransactionManagerStateTo(
            Object transactionManager, String state) {
        PscCommon.invoke(transactionManager, "transitionTo", getTransactionManagerState(state));
    }
}
