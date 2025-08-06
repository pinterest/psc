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
import com.pinterest.psc.producer.transaction.TransactionManagerUtils;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

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

    /*
    @Override
    public void sendOffsetsToTransaction(Map<TopicUriPartition, MessageId> offsets, String consumerGroupId) throws ProducerException, ConfigurationException {
        ensureNotClosed();
        super.sendOffsetsToTransaction(offsets, consumerGroupId);
    }
    */

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
    public void close(Duration duration) throws IOException {
        super.close(duration);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Closed internal PscProducer {}. Stacktrace: {}",
                    System.identityHashCode(this),
                    Arrays.stream(Thread.currentThread().getStackTrace())
                            .map(StackTraceElement::toString)
                            .collect(Collectors.joining("\n")));        }
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
        return TransactionManagerUtils.getProducerId(transactionManager);
    }

    public short getEpoch(PscProducerMessage pscProducerMessage) throws ProducerException {
        Object transactionManager = super.getTransactionManager(pscProducerMessage);
        return TransactionManagerUtils.getEpoch(transactionManager);
    }

    @VisibleForTesting
    public Set<Integer> getTransactionCoordinatorIds() throws ProducerException {
        Set<Integer> coordinatorIds = new HashSet<>();
        super.getTransactionManagers().forEach(transactionManager ->
                coordinatorIds.add(
                        TransactionManagerUtils.getTransactionCoordinatorId(transactionManager)
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
        Set<Future<Boolean>> results = enqueueNewPartitions();
        super.wakeup();
        results.forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
                throw new RuntimeException("Error while flushing new partitions", e);
            }
        });
    }

    /**
     * Enqueues new transactions at the transaction manager and returns a {@link
     * TransactionalRequestResult} that allows waiting on them.
     *
     * <p>If there are no new transactions we return a {@link TransactionalRequestResult} that is
     * already done.
     */
    private Set<Future<Boolean>> enqueueNewPartitions() throws ProducerException {
        Set<Future<Boolean>> transactionalRequestResults = new HashSet<>();
        Set<Object> transactionManagers = super.getTransactionManagers();
        for (Object transactionManager : transactionManagers) {
            synchronized (transactionManager) {
                Future<Boolean> transactionalRequestResultFuture =
                        TransactionManagerUtils.enqueueInFlightTransactions(transactionManager);
                transactionalRequestResults.add(transactionalRequestResultFuture);
            }
        }
        return transactionalRequestResults;
    }

}
