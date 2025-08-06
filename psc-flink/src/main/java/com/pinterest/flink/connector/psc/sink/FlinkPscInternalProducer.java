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

import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.producer.Callback;
import com.pinterest.psc.producer.PscBackendProducer;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.producer.PscProducerTransactionalProperties;
import com.pinterest.psc.producer.transaction.TransactionManagerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link PscProducer} that exposes private fields to allow resume producing from a given state.
 * Note that transactional operations are only supported by PSC producers in the backend.
 * This class is implemented assuming only one backendProducer is active. If there are multiple backendProducers,
 * the operations in this class will fail.
 */
class FlinkPscInternalProducer<K, V> extends PscProducer<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkPscInternalProducer.class);
    @Nullable private String transactionalId;
    private volatile boolean hasRecordsInTransaction;
    private volatile boolean inTransaction;
    private volatile boolean closed;
    private TopicUri clusterUri = null;

    public FlinkPscInternalProducer(Properties properties, @Nullable String transactionalId) throws ConfigurationException, ProducerException, TopicUriSyntaxException {
        super(PscConfigurationUtils.propertiesToPscConfiguration(withTransactionalId(properties, transactionalId)));
        if (transactionalId != null) {
            // Producer is transactional, so the backend producer should be immediately initialized given the ClusterUri in the properties
            this.clusterUri = PscFlinkConfiguration.validateAndGetBaseClusterUri(properties);
            getBackendProducerForTopicUri(validateTopicUri(clusterUri.getTopicUriAsString()));
        }
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
    public Future<MessageId> send(PscProducerMessage<K, V> message, Callback callback) throws ConfigurationException, ProducerException {
        if (inTransaction) {
            hasRecordsInTransaction = true;
        }
        return super.send(message, callback);
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
        hasRecordsInTransaction = false;
        super.abortTransaction();
    }

    @Override
    public void commitTransaction() throws ProducerException {
        LOG.debug("commitTransaction {}", transactionalId);
        checkState(inTransaction, "Transaction was not started");
        inTransaction = false;
        hasRecordsInTransaction = false;
        super.commitTransaction();
    }

    public boolean isInTransaction() {
        return inTransaction;
    }

    public boolean hasRecordsInTransaction() {
        return hasRecordsInTransaction;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        if (inTransaction) {
            // This is state is most likely reached in case of a failure.
            // If this producer is still in transaction, it should be committing.
            // However, at this point, we cannot decide that and we shouldn't prolong cancellation.
            // So hard kill this producer with all resources.
            super.close(Duration.ZERO);
        } else {
            // If this is outside of a transaction, we should be able to cleanly shutdown.
            super.close(Duration.ofHours(1));
        }
    }

    @Override
    public void close(Duration timeout) throws IOException {
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

    public short getEpoch() throws ProducerException {
        return TransactionManagerUtils.getEpoch(getTransactionManager());
    }

    public long getProducerId() throws ProducerException {
        return TransactionManagerUtils.getProducerId(getTransactionManager());
    }

    public void initTransactionId(String transactionalId) throws ProducerException {
        if (!transactionalId.equals(this.transactionalId)) {
            setTransactionId(transactionalId);
            initTransactions();
        }
    }

    /**
     * Initializes the transactional producer in the backend. This is a custom implementation of initTransactions specifically
     * for this class which does not require passing in a topicUri to the method. Instead, creating an instance
     * of this class with non-null transactional ID will already initialize the backend producer, which allows
     * this method to be called without any arguments since there is only one backend producer.
     * @throws ProducerException
     */
    public void initTransactions() throws ProducerException {
        ensureOpen();
        if (getBackendProducers().size() != 1)
            throw new IllegalStateException("initTransactions() can only be called when there is exactly one backend producer" +
                    " already created.");
        PscBackendProducer<K, V> backendProducer = getBackendProducers().iterator().next();
        initTransactions(backendProducer, false);
    }

    public void setTransactionId(String transactionalId) throws ProducerException {
        if (!transactionalId.equals(this.transactionalId)) {
            checkState(
                    !inTransaction,
                    String.format("Another transaction %s is still open.", transactionalId));
            LOG.debug("Change transaction id from {} to {}", this.transactionalId, transactionalId);
            Object transactionManager = getTransactionManager();
            synchronized (transactionManager) {
                TransactionManagerUtils.setTransactionId(transactionManager, transactionalId);
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
        Future<Boolean> future = TransactionManagerUtils.enqueueInFlightTransactions(getTransactionManager());
        super.wakeup();
        try {
            boolean isSuccessful = future.get();
            if (!isSuccessful) {
                throw new ProducerException("Flushing in-flight transactions failed");
            }
        } catch (Exception e) {
            throw new ProducerException("Failed to flush new partitions", e);
        }
    }

    /**
     * Instead of obtaining producerId and epoch from the transaction coordinator, re-use previously
     * obtained ones, so that we can resume transaction after a restart.
     */
    public void resumeTransaction(long producerId, short epoch) throws ProducerException {
        ensureOnlyOneBackendProducer();
        checkState(!inTransaction, "Already in transaction %s", transactionalId);
        checkState(clusterUri != null, "ClusterUri is not set");
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

        PscProducerTransactionalProperties pscProducerTransactionalProperties = new PscProducerTransactionalProperties(producerId, epoch);
        super.resumeTransaction(pscProducerTransactionalProperties, Collections.singleton(clusterUri.getTopicUriAsString()));
        this.inTransaction = true;
        this.hasRecordsInTransaction = true;
    }

    private void ensureOnlyOneBackendProducer() {
        if (getBackendProducers().size() != 1) {
            throw new IllegalStateException("This operation is only supported when there is exactly one backend producer");
        }
    }

    private Object getTransactionManager() throws ProducerException {
        return super.getExactlyOneTransactionManager();
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
