/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.connector.psc.sink;

import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.producer.PscProducer;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link PscProducer} that exposes private fields to allow resume producing from a given state.
 * Note that transactional operations are only supported by PSC producers in the backend.
 * This class is implemented assuming only one backendProducer is active. If there are multiple backendProducers,
 * the operations in this class will fail.
 */
class FlinkPscInternalProducer<K, V> extends PscProducer<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkPscInternalProducer.class);
    private static final String TRANSACTION_STATE_ENUM =
            "com.pinterest.psc.producer.PscProducer$TransactionalState";
    private static final String KAFKA_TXN_MANAGER_PRODUCER_ID_AND_EPOCH_FIELD_NAME = "producerIdAndEpoch";
    @Nullable private String transactionalId;
    private volatile boolean inTransaction;
    private volatile boolean closed;

    public FlinkPscInternalProducer(Properties properties, @Nullable String transactionalId) throws ConfigurationException, ProducerException {
        super(PscConfigurationUtils.propertiesToPscConfiguration(withTransactionalId(properties, transactionalId)));
        this.transactionalId = transactionalId;
    }

    private static Properties withTransactionalId(
            Properties properties, @Nullable String transactionalId) {
        if (transactionalId == null) {
            return properties;
        }
        Properties props = new Properties();
        props.putAll(properties);
        props.setProperty(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID, transactionalId);
        return props;
    }

    @Override
    public void flush() throws ProducerException {
        super.flush();
        if (inTransaction) {
            flushNewPartitions();
        }
    }

    @Override
    public void beginTransaction() throws ProducerException {
        super.beginTransaction();
        inTransaction = true;
    }

    @Override
    public void abortTransaction() throws ProducerException {
        LOG.debug("abortTransaction {}", transactionalId);
        checkState(inTransaction, "Transaction was not started");
        inTransaction = false;
        super.abortTransaction();
    }

    @Override
    public void commitTransaction() throws ProducerException {
        LOG.debug("commitTransaction {}", transactionalId);
        checkState(inTransaction, "Transaction was not started");
        inTransaction = false;
        super.commitTransaction();
    }

    public boolean isInTransaction() {
        return inTransaction;
    }

    @Override
    public void close() {
        closed = true;
        if (inTransaction) {
            // This is state is most likely reached in case of a failure.
            // If this producer is still in transaction, it should be committing.
            // However, at this point, we cannot decide that and we shouldn't prolong cancellation.
            // So hard kill this producer with all resources.
            try {
                super.close(Duration.ZERO);
            } catch (ProducerException e) {
                throw new RuntimeException("Failed to close PscProducer", e);
            }
        } else {
            // If this is outside of a transaction, we should be able to cleanly shutdown.
            try {
                super.close(Duration.ofHours(1));
            } catch (ProducerException e) {
                throw new RuntimeException("Failed to close PscProducer", e);
            }
        }
    }

    @Override
    public void close(Duration timeout) throws ProducerException {
        closed = true;
        super.close(timeout);
    }

    public boolean isClosed() {
        return closed;
    }

    @Nullable
    public String getTransactionalId() {
        return transactionalId;
    }

    private static Object getProducerIdAndEpoch(Object transactionManager) {
        return getField(transactionManager, KAFKA_TXN_MANAGER_PRODUCER_ID_AND_EPOCH_FIELD_NAME);
    }

    private static void verifyTransactionManagerCompatibility(Object transactionManager) throws ProducerException {
        if (!(transactionManager instanceof TransactionManager)) {
            // only Kafka transaction managers are supported at the moment
            throw new ProducerException("Unsupported transaction manager: " + transactionManager);
        }
    }

    public short getEpoch() throws ProducerException {
        Object transactionManager = getTransactionManager();
        Object producerIdAndEpoch = getProducerIdAndEpoch(transactionManager);
        return (short) getField(producerIdAndEpoch, "epoch");
    }

    public long getProducerId() throws ProducerException {
        Object transactionManager = getTransactionManager();
        Object producerIdAndEpoch = getProducerIdAndEpoch(transactionManager);
        return (long) getField(producerIdAndEpoch, "producerId");
    }

    public void initTransactionId(String transactionalId) throws ProducerException {
        if (!transactionalId.equals(this.transactionalId)) {
            setTransactionId(transactionalId);
            initTransactions();
        }
    }

    public void setTransactionId(String transactionalId) throws ProducerException {
        if (!transactionalId.equals(this.transactionalId)) {
            checkState(
                    !inTransaction,
                    String.format("Another transaction %s is still open.", transactionalId));
            LOG.debug("Change transaction id from {} to {}", this.transactionalId, transactionalId);
            Object transactionManager = getTransactionManager();
            synchronized (transactionManager) {
                setField(transactionManager, "transactionalId", transactionalId);
                setField(
                        transactionManager,
                        "currentState",
                        getTransactionManagerState("UNINITIALIZED"));
                this.transactionalId = transactionalId;
            }
        }
    }

    /**
     * Besides committing {@link PscProducer#commitTransaction}
     * is also adding new partitions to the transaction. flushNewPartitions method is moving this
     * logic to pre-commit/flush, to make resumeTransaction simpler. Otherwise resumeTransaction
     * would require to restore state of the not yet added/"in-flight" partitions.
     */
    private void flushNewPartitions() throws ProducerException {
        LOG.info("Flushing new partitions");
        TransactionalRequestResult result = enqueueNewPartitions();
        super.wakeup();
        result.await();
    }

    /**
     * Enqueues new transactions at the transaction manager and returns a {@link
     * TransactionalRequestResult} that allows waiting on them.
     *
     * <p>If there are no new transactions we return a {@link TransactionalRequestResult} that is
     * already done.
     */
    private TransactionalRequestResult enqueueNewPartitions() throws ProducerException {
        Object transactionManager = getTransactionManager();
        synchronized (transactionManager) {
            Object newPartitionsInTransaction =
                    getField(transactionManager, "newPartitionsInTransaction");
            Object newPartitionsInTransactionIsEmpty =
                    invoke(newPartitionsInTransaction, "isEmpty");
            TransactionalRequestResult result;
            if (newPartitionsInTransactionIsEmpty instanceof Boolean
                    && !((Boolean) newPartitionsInTransactionIsEmpty)) {
                Object txnRequestHandler =
                        invoke(transactionManager, "addPartitionsToTransactionHandler");
                invoke(
                        transactionManager,
                        "enqueueRequest",
                        new Class[] {txnRequestHandler.getClass().getSuperclass()},
                        new Object[] {txnRequestHandler});
                result =
                        (TransactionalRequestResult)
                                getField(
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
    }

    private static Object invoke(Object object, String methodName, Object... args) {
        Class<?>[] argTypes = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argTypes[i] = args[i].getClass();
        }
        return invoke(object, methodName, argTypes, args);
    }

    private static Object invoke(
            Object object, String methodName, Class<?>[] argTypes, Object[] args) {
        try {
            Method method = object.getClass().getDeclaredMethod(methodName, argTypes);
            method.setAccessible(true);
            return method.invoke(object, args);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible PscProducer version", e);
        }
    }

    /**
     * Gets and returns the field {@code fieldName} from the given Object {@code object} using
     * reflection.
     */
    private static Object getField(Object object, String fieldName) {
        return getField(object, object.getClass(), fieldName);
    }

    /**
     * Gets and returns the field {@code fieldName} from the given Object {@code object} using
     * reflection.
     */
    private static Object getField(Object object, Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible PscProducer version", e);
        }
    }

    /**
     * Instead of obtaining producerId and epoch from the transaction coordinator, re-use previously
     * obtained ones, so that we can resume transaction after a restart. Implementation of this
     * method is based on {@link PscProducer#initTransactions()}.
     */
    public void resumeTransaction(long producerId, short epoch) throws ProducerException {
        checkState(!inTransaction, "Already in transaction %s", transactionalId);
        checkState(
                producerId >= 0 && epoch >= 0,
                "Incorrect values for producerId %s and epoch %s",
                producerId,
                epoch);
        LOG.info(
                "Attempting to resume transaction {} with producerId {} and epoch {}",
                transactionalId,
                producerId,
                epoch);

        Object transactionManager = getTransactionManager();
        synchronized (transactionManager) {
            Object topicPartitionBookkeeper =
                    getField(transactionManager, "topicPartitionBookkeeper");

            transitionTransactionManagerStateTo(transactionManager, "INITIALIZING");
            invoke(topicPartitionBookkeeper, "reset");

            setField(
                    transactionManager,
                    KAFKA_TXN_MANAGER_PRODUCER_ID_AND_EPOCH_FIELD_NAME,
                    createProducerIdAndEpoch(producerId, epoch));

            transitionTransactionManagerStateTo(transactionManager, "READY");

            transitionTransactionManagerStateTo(transactionManager, "IN_TRANSACTION");
            setField(transactionManager, "transactionStarted", true);
            this.inTransaction = true;
        }
    }

    private static Object createProducerIdAndEpoch(long producerId, short epoch) {
        try {
            Field field =
                    TransactionManager.class.getDeclaredField(KAFKA_TXN_MANAGER_PRODUCER_ID_AND_EPOCH_FIELD_NAME);
            Class<?> clazz = field.getType();
            Constructor<?> constructor = clazz.getDeclaredConstructor(Long.TYPE, Short.TYPE);
            constructor.setAccessible(true);
            return constructor.newInstance(producerId, epoch);
        } catch (InvocationTargetException
                | InstantiationException
                | IllegalAccessException
                | NoSuchFieldException
                | NoSuchMethodException e) {
            throw new RuntimeException("Incompatible PscProducer version", e);
        }
    }

    /**
     * Sets the field {@code fieldName} on the given Object {@code object} to {@code value} using
     * reflection.
     */
    private static void setField(Object object, String fieldName, Object value) {
        setField(object, object.getClass(), fieldName, value);
    }

    private static void setField(Object object, Class<?> clazz, String fieldName, Object value) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible PscProducer version", e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Enum<?> getTransactionManagerState(String enumName) {
        try {
            Class<Enum> cl = (Class<Enum>) Class.forName(TRANSACTION_STATE_ENUM);
            return Enum.valueOf(cl, enumName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Incompatible PscProducer version", e);
        }
    }

    private Object getTransactionManager() throws ProducerException {
        Set<Object> txnManagers = super.getTransactionManagers();
        if (txnManagers.size() != 1) {
            // This should never happen unless the same PscProducer spun up multiple BackendProducers
            throw new ProducerException("Expected exactly one transaction manager, but found " + txnManagers.size());
        }
        Object txnManager = txnManagers.iterator().next();
        verifyTransactionManagerCompatibility(txnManager);
        return txnManager;
    }

    private static void transitionTransactionManagerStateTo(
            Object transactionManager, String state) {
        invoke(transactionManager, "transitionTo", getTransactionManagerState(state));
    }

    @Override
    public String toString() {
        return "FlinkPscInternalProducer{"
                + "transactionalId='"
                + transactionalId
                + "', inTransaction="
                + inTransaction
                + ", closed="
                + closed
                + '}';
    }
}
