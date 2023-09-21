/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.streaming.connectors.psc.internals;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import com.pinterest.psc.producer.Callback;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.producer.PscProducerTransactionalProperties;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Internal flink PSC producer.
 */
@PublicEvolving
public class FlinkPscInternalProducer<K, V> extends PscProducer<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkPscInternalProducer.class);
    private volatile boolean closed;

    @Nullable
    protected final String transactionalId;

    private static PscConfiguration toConfiguration(Properties properties) {
        PscConfiguration pscConfiguration = new PscConfiguration();
        properties.forEach((key, value) -> pscConfiguration.setProperty(key.toString(), value));
        return pscConfiguration;
    }

    public FlinkPscInternalProducer(Properties pscProducerConfiguration) throws ProducerException, ConfigurationException {
        super(toConfiguration(pscProducerConfiguration));
        transactionalId = pscProducerConfiguration.getProperty(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID);
        closed = false;
    }

    // -------------------------------- Simple proxy method calls --------------------------------

    @Override
    public void beginTransaction() throws ProducerException {
        ensureNotClosed();
        super.beginTransaction();
    }

    @Override
    public void commitTransaction() throws ProducerException {
        ensureNotClosed();
        super.commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerException {
        ensureNotClosed();
        super.abortTransaction();
    }

    @Override
    public PscProducerTransactionalProperties initTransactions(String topicUri) throws ProducerException, ConfigurationException {
        ensureNotClosed();
        return super.initTransactions(topicUri);
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicUriPartition, MessageId> offsets, String consumerGroupId) throws ProducerException, ConfigurationException {
        ensureNotClosed();
        super.sendOffsetsToTransaction(offsets, consumerGroupId);
    }

    @Override
    public Future<MessageId> send(PscProducerMessage<K, V> record) throws ProducerException, ConfigurationException {
        return super.send(record);
    }

    @Override
    public Future<MessageId> send(PscProducerMessage<K, V> record, Callback callback) throws ProducerException, ConfigurationException {
        return super.send(record, callback);
    }

    @Override
    public Set<TopicUriPartition> getPartitions(String topicUri) throws ProducerException, ConfigurationException {
        ensureNotClosed();
        return super.getPartitions(topicUri);
    }

    @Override
    public Map<MetricName, Metric> metrics() throws ClientException {
        return super.metrics();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Close without timeout is now allowed because it can leave lingering Kafka threads.");
    }

    @Override
    public void close(Duration duration) throws ProducerException {
        super.close(duration);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Closed internal PscProducer {}. Stacktrace: {}",
                    System.identityHashCode(this),
                    Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
        }
        closed = true;
    }

    // -------------------------------- New methods or methods with changed behaviour --------------------------------

    @Override
    public void flush() throws ProducerException {
        super.flush();
        if (transactionalId != null && super.isInTransaction()) {
            ensureNotClosed();
            flushNewPartitions();
        }
    }

	public void resumeTransaction(PscProducerTransactionalProperties pscProducerTransactionalProperties, Set<String> topicUris) throws ProducerException {
        ensureNotClosed();
        super.resumeTransaction(pscProducerTransactionalProperties, topicUris);
	}

	public String getTransactionalId() {
        return transactionalId;
    }

    public long getProducerId(PscProducerMessage pscProducerMessage) throws ProducerException {
        Object transactionManager = super.getTransactionManager(pscProducerMessage);
        Object producerIdAndEpoch = getField(transactionManager, "producerIdAndEpoch");
        return (long) getField(producerIdAndEpoch, "producerId");
    }

    public short getEpoch(PscProducerMessage pscProducerMessage) throws ProducerException {
        Object transactionManager = super.getTransactionManager(pscProducerMessage);
        Object producerIdAndEpoch = getField(transactionManager, "producerIdAndEpoch");
        return (short) getField(producerIdAndEpoch, "epoch");
    }

    @VisibleForTesting
    public Set<Integer> getTransactionCoordinatorIds() throws ProducerException {
        Set<Integer> coordinatorIds = new HashSet<>();
        super.getTransactionManagers().forEach(transactionManager ->
                coordinatorIds.add(
                        ((Node) invoke(transactionManager, "coordinator", FindCoordinatorRequest.CoordinatorType.TRANSACTION)).id()
                )
        );
        return coordinatorIds;
    }

    private void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException(String.format("The producer %s has already been closed", System.identityHashCode(this)));
        }
    }

    /**
     * Besides committing {@link PscProducer#commitTransaction} is also adding new
     * partitions to the transaction. flushNewPartitions method is moving this logic to pre-commit/flush, to make
     * resumeTransaction simpler. Otherwise resumeTransaction would require to restore state of the not yet added/"in-flight"
     * partitions.
     */
    private void flushNewPartitions() throws ProducerException {
        LOG.info("Flushing new partitions");
        Set<TransactionalRequestResult> results = enqueueNewPartitions();
        super.wakeup();
        results.forEach(TransactionalRequestResult::await);
    }

    /**
     * Enqueues new transactions at the transaction manager and returns a {@link
     * TransactionalRequestResult} that allows waiting on them.
     *
     * <p>If there are no new transactions we return a {@link TransactionalRequestResult} that is
     * already done.
     */
    private Set<TransactionalRequestResult> enqueueNewPartitions() throws ProducerException {
        Set<TransactionalRequestResult> transactionalRequestResults = new HashSet<>();
        Set<Object> transactionManagers = super.getTransactionManagers();
        for (Object transactionManager : transactionManagers) {
            synchronized (transactionManager) {
                Object newPartitionsInTransaction = getField(transactionManager, "newPartitionsInTransaction");
                Object newPartitionsInTransactionIsEmpty = invoke(newPartitionsInTransaction, "isEmpty");
                TransactionalRequestResult result;
                if (newPartitionsInTransactionIsEmpty instanceof Boolean && !((Boolean) newPartitionsInTransactionIsEmpty)) {
                    Object txnRequestHandler = invoke(transactionManager, "addPartitionsToTransactionHandler");
                    invoke(transactionManager, "enqueueRequest", new Class[]{txnRequestHandler.getClass().getSuperclass()}, new Object[]{txnRequestHandler});
                    result = (TransactionalRequestResult) getField(txnRequestHandler, txnRequestHandler.getClass().getSuperclass(), "result");
                } else {
                    // we don't have an operation but this operation string is also used in
                    // addPartitionsToTransactionHandler.
                    result = new TransactionalRequestResult("AddPartitionsToTxn");
                    result.done();
                }
                transactionalRequestResults.add(result);
            }
        }
        return transactionalRequestResults;
    }

    protected static Enum<?> getEnum(String enumFullName) {
        String[] x = enumFullName.split("\\.(?=[^\\.]+$)");
        if (x.length == 2) {
            String enumClassName = x[0];
            String enumName = x[1];
            try {
                Class<Enum> cl = (Class<Enum>) Class.forName(enumClassName);
                return Enum.valueOf(cl, enumName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Incompatible KafkaProducer version", e);
            }
        }
        return null;
    }

    protected static Object invoke(Object object, String methodName, Object... args) {
        Class<?>[] argTypes = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argTypes[i] = args[i].getClass();
        }
        return invoke(object, methodName, argTypes, args);
    }

    private static Object invoke(Object object, String methodName, Class<?>[] argTypes, Object[] args) {
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
    protected static Object getField(Object object, String fieldName) {
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
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    /**
     * Sets the field {@code fieldName} on the given Object {@code object} to {@code value} using
     * reflection.
     */
    protected static void setField(Object object, String fieldName, Object value) {
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

}
