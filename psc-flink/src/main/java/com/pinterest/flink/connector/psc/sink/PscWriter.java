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

import com.pinterest.flink.connector.psc.MetricUtil;
import com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscMetricMutableWrapper;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import com.pinterest.psc.producer.Callback;
import com.pinterest.psc.producer.PscProducerMessage;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava30.com.google.common.io.Closer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is responsible to write records in a PSC topicUri and to handle the different delivery
 * {@link DeliveryGuarantee}s.
 *
 * @param <IN> The type of the input elements.
 */
class PscWriter<IN>
        implements StatefulSink.StatefulSinkWriter<IN, PscWriterState>,
                TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, PscCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(PscWriter.class);
    private static final String PSC_PRODUCER_METRIC_NAME = "PscProducer";
    private static final long METRIC_UPDATE_INTERVAL_MILLIS = 500;

    private static final String KEY_DISABLE_METRICS = "flink.disable-metrics";
    private static final String KEY_REGISTER_METRICS = "register.producer.metrics";
    private static final String PSC_PRODUCER_METRICS = "producer-metrics";

    private final DeliveryGuarantee deliveryGuarantee;
    private final Properties pscProducerConfig;
    private final String transactionalIdPrefix;
    private final PscRecordSerializationSchema<IN> recordSerializer;
    private final Callback deliveryCallback;
    private final PscRecordSerializationSchema.PscSinkContext pscSinkContext;

    private final Map<String, PscMetricMutableWrapper> previouslyCreatedMetrics = new HashMap<>();
    private final SinkWriterMetricGroup metricGroup;
    private final boolean disabledMetrics;
    private final Counter numRecordsOutCounter;
    private final Counter numBytesOutCounter;
    private final Counter numRecordsOutErrorsCounter;
    private final ProcessingTimeService timeService;

    // Number of outgoing bytes at the latest metric sync
    private long latestOutgoingByteTotal;
    private Metric byteOutMetric;
    private FlinkPscInternalProducer<byte[], byte[]> currentProducer;
    private final PscWriterState pscWriterState;
    // producer pool only used for exactly once
    private final Deque<FlinkPscInternalProducer<byte[], byte[]>> producerPool =
            new ArrayDeque<>();
    private final Closer closer = Closer.create();
    private long lastCheckpointId;

    private boolean closed = false;
    private long lastSync = System.currentTimeMillis();

    /**
     * Constructor creating a PSC writer.
     *
     * <p>It will throw a {@link RuntimeException} if {@link
     * PscRecordSerializationSchema#open(SerializationSchema.InitializationContext,
     * PscRecordSerializationSchema.PscSinkContext)} fails.
     *
     * @param deliveryGuarantee the Sink's delivery guarantee
     * @param pscProducerConfig the properties to configure the {@link FlinkPscInternalProducer}
     * @param transactionalIdPrefix used to create the transactionalIds
     * @param sinkInitContext context to provide information about the runtime environment
     * @param recordSerializer serialize to transform the incoming records to {@link PscProducerMessage}
     * @param schemaContext context used to initialize the {@link PscRecordSerializationSchema}
     * @param recoveredStates state from an previous execution which was covered
     */
    PscWriter(
            DeliveryGuarantee deliveryGuarantee,
            Properties pscProducerConfig,
            String transactionalIdPrefix,
            Sink.InitContext sinkInitContext,
            PscRecordSerializationSchema<IN> recordSerializer,
            SerializationSchema.InitializationContext schemaContext,
            Collection<PscWriterState> recoveredStates) throws ConfigurationException, ClientException {
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        this.pscProducerConfig = checkNotNull(pscProducerConfig, "pscProducerConfig");
        this.transactionalIdPrefix = checkNotNull(transactionalIdPrefix, "transactionalIdPrefix");
        this.recordSerializer = checkNotNull(recordSerializer, "recordSerializer");
        this.deliveryCallback =
                new WriterCallback(
                        sinkInitContext.getMailboxExecutor(),
                        sinkInitContext.<MessageId>metadataConsumer().orElse(null));
        this.disabledMetrics =
                pscProducerConfig.containsKey(KEY_DISABLE_METRICS)
                                && Boolean.parseBoolean(
                                        pscProducerConfig.get(KEY_DISABLE_METRICS).toString())
                        || pscProducerConfig.containsKey(KEY_REGISTER_METRICS)
                                && !Boolean.parseBoolean(
                                        pscProducerConfig.get(KEY_REGISTER_METRICS).toString());
        checkNotNull(sinkInitContext, "sinkInitContext");
        this.timeService = sinkInitContext.getProcessingTimeService();
        this.metricGroup = sinkInitContext.metricGroup();
        this.numBytesOutCounter = metricGroup.getIOMetricGroup().getNumBytesOutCounter();
        this.numRecordsOutCounter = metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
        this.numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        this.pscSinkContext =
                new DefaultPscSinkContext(
                        sinkInitContext.getSubtaskId(),
                        sinkInitContext.getNumberOfParallelSubtasks(),
                        PscConfigurationUtils.propertiesToPscConfiguration(pscProducerConfig));
        try {
            recordSerializer.open(schemaContext, pscSinkContext);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Cannot initialize schema.", e);
        }

        this.pscWriterState = new PscWriterState(transactionalIdPrefix);
        this.lastCheckpointId =
                sinkInitContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            abortLingeringTransactions(
                    checkNotNull(recoveredStates, "recoveredStates"), lastCheckpointId + 1);
            this.currentProducer = getTransactionalProducer(lastCheckpointId + 1);
            this.currentProducer.beginTransaction();
        } else if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE
                || deliveryGuarantee == DeliveryGuarantee.NONE) {
            this.currentProducer = new FlinkPscInternalProducer<>(this.pscProducerConfig, null);
            closer.register(this.currentProducer);
            initPscMetrics(this.currentProducer);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported PSC writer semantic " + this.deliveryGuarantee);
        }

        initFlinkMetrics();
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        final PscProducerMessage<byte[], byte[]> record =
                recordSerializer.serialize(element, pscSinkContext, context.timestamp());
        try {
            currentProducer.send(record, deliveryCallback);
        } catch (ProducerException | ConfigurationException e) {
            throw new RuntimeException(e);
        }
        numRecordsOutCounter.inc();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (deliveryGuarantee != DeliveryGuarantee.NONE || endOfInput) {
            LOG.debug("final flush={}", endOfInput);
            try {
                currentProducer.flush();
            } catch (ProducerException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Collection<PscCommittable> prepareCommit() {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            final List<PscCommittable> committables;
            try {
                committables = Collections.singletonList(
                        PscCommittable.of(currentProducer, producerPool::add));
            } catch (ProducerException e) {
                throw new RuntimeException(e);
            }
            LOG.debug("Committing {} committables.", committables);
            return committables;
        }
        return Collections.emptyList();
    }

    @Override
    public List<PscWriterState> snapshotState(long checkpointId) throws IOException {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            try {
                currentProducer = getTransactionalProducer(checkpointId + 1);
                currentProducer.beginTransaction();
            } catch (ProducerException | ConfigurationException e) {
                throw new RuntimeException(e);
            }
        }
        return ImmutableList.of(pscWriterState);
    }

    @Override
    public void close() throws Exception {
        closed = true;
        LOG.debug("Closing writer with {}", currentProducer);
        closeAll(
                this::abortCurrentProducer,
                closer,
                producerPool::clear,
                () -> {
                    checkState(currentProducer.isClosed());
                    currentProducer = null;
                });
    }

    private void abortCurrentProducer() {
        if (currentProducer.isInTransaction()) {
            try {
                currentProducer.abortTransaction();
            } catch (ProducerException e) {
                if (e.getCause() instanceof ProducerFencedException)
                    LOG.debug("Producer {} fenced while aborting", currentProducer.getTransactionalId());
            }
        }
    }

    @VisibleForTesting
    Deque<FlinkPscInternalProducer<byte[], byte[]>> getProducerPool() {
        return producerPool;
    }

    @VisibleForTesting
    FlinkPscInternalProducer<byte[], byte[]> getCurrentProducer() {
        return currentProducer;
    }

    void abortLingeringTransactions(
            Collection<PscWriterState> recoveredStates, long startCheckpointId) {
        List<String> prefixesToAbort = Lists.newArrayList(transactionalIdPrefix);

        final Optional<PscWriterState> lastStateOpt = recoveredStates.stream().findFirst();
        if (lastStateOpt.isPresent()) {
            PscWriterState lastState = lastStateOpt.get();
            if (!lastState.getTransactionalIdPrefix().equals(transactionalIdPrefix)) {
                prefixesToAbort.add(lastState.getTransactionalIdPrefix());
                LOG.warn(
                        "Transactional id prefix from previous execution {} has changed to {}.",
                        lastState.getTransactionalIdPrefix(),
                        transactionalIdPrefix);
            }
        }

        try (TransactionAborter transactionAborter =
                new TransactionAborter(
                        pscSinkContext.getParallelInstanceId(),
                        pscSinkContext.getNumberOfParallelInstances(),
                        this::getOrCreateTransactionalProducer,
                        producerPool::add)) {
            transactionAborter.abortLingeringTransactions(prefixesToAbort, startCheckpointId);
        } catch (ProducerException e) {
            throw new RuntimeException("Failed to abort lingering transactions", e);
        }
    }

    /**
     * For each checkpoint we create new {@link FlinkPscInternalProducer} so that new transactions
     * will not clash with transactions created during previous checkpoints ({@code
     * producer.initTransactions()} assures that we obtain new producerId and epoch counters).
     *
     * <p>Ensures that all transaction ids in between lastCheckpointId and checkpointId are
     * initialized.
     */
    private FlinkPscInternalProducer<byte[], byte[]> getTransactionalProducer(long checkpointId) throws ConfigurationException, ProducerException {
        checkState(
                checkpointId > lastCheckpointId,
                "Expected %s > %s",
                checkpointId,
                lastCheckpointId);
        FlinkPscInternalProducer<byte[], byte[]> producer = null;
        // in case checkpoints have been aborted, Flink would create non-consecutive transaction ids
        // this loop ensures that all gaps are filled with initialized (empty) transactions
        for (long id = lastCheckpointId + 1; id <= checkpointId; id++) {
            String transactionalId =
                    TransactionalIdFactory.buildTransactionalId(
                            transactionalIdPrefix, pscSinkContext.getParallelInstanceId(), id);
            producer = getOrCreateTransactionalProducer(transactionalId);
        }
        this.lastCheckpointId = checkpointId;
        assert producer != null;
        LOG.info("Created new transactional producer {}", producer.getTransactionalId());
        return producer;
    }

    private FlinkPscInternalProducer<byte[], byte[]> getOrCreateTransactionalProducer(
            String transactionalId) {
        FlinkPscInternalProducer<byte[], byte[]> producer = producerPool.poll();
        try {
            if (producer == null) {
                producer = new FlinkPscInternalProducer<>(pscProducerConfig, transactionalId);
                closer.register(producer);
                producer.initTransactions();
                initPscMetrics(producer);
            } else {
                producer.initTransactionId(transactionalId);
            }
        } catch (ConfigurationException | ClientException e) {
            throw new RuntimeException(e);
        }
        return producer;
    }

    private void initFlinkMetrics() {
        metricGroup.setCurrentSendTimeGauge(this::computeSendTime);
        registerMetricSync();
    }

    private void initPscMetrics(FlinkPscInternalProducer<byte[], byte[]> producer) throws ClientException {
        byteOutMetric =
                MetricUtil.getPscMetric(
                        producer.metrics(), PSC_PRODUCER_METRICS, "outgoing-byte-total");
        if (disabledMetrics) {
            return;
        }
        final MetricGroup pscMetricGroup = metricGroup.addGroup(PSC_PRODUCER_METRIC_NAME);
        producer.metrics().entrySet().forEach(initMetric(pscMetricGroup));
    }

    private Consumer<Map.Entry<MetricName, ? extends Metric>> initMetric(
            MetricGroup pscMetricGroup) {
        return (entry) -> {
            final String name = entry.getKey().name();
            final Metric metric = entry.getValue();
            if (previouslyCreatedMetrics.containsKey(name)) {
                final PscMetricMutableWrapper wrapper = previouslyCreatedMetrics.get(name);
                wrapper.setPscMetric(metric);
            } else {
                final PscMetricMutableWrapper wrapper = new PscMetricMutableWrapper(metric);
                previouslyCreatedMetrics.put(name, wrapper);
                pscMetricGroup.gauge(name, wrapper);
            }
        };
    }

    private long computeSendTime() {
        FlinkPscInternalProducer<byte[], byte[]> producer = this.currentProducer;
        if (producer == null) {
            return -1L;
        }
        try {
            final Metric sendTime =
                    MetricUtil.getPscMetric(
                            producer.metrics(), PSC_PRODUCER_METRICS, "request-latency-avg");
            final Metric queueTime =
                    MetricUtil.getPscMetric(
                            producer.metrics(), PSC_PRODUCER_METRICS, "record-queue-time-avg");
            return ((Number) sendTime.metricValue()).longValue()
                    + ((Number) queueTime.metricValue()).longValue();
        } catch (ClientException e) {
            LOG.warn("Failed to compute send time", e);
            return -1L;
        }
    }

    private void registerMetricSync() {
        timeService.registerTimer(
                lastSync + METRIC_UPDATE_INTERVAL_MILLIS,
                (time) -> {
                    if (closed) {
                        return;
                    }
                    long outgoingBytesUntilNow = ((Number) byteOutMetric.metricValue()).longValue();
                    long outgoingBytesSinceLastUpdate =
                            outgoingBytesUntilNow - latestOutgoingByteTotal;
                    numBytesOutCounter.inc(outgoingBytesSinceLastUpdate);
                    latestOutgoingByteTotal = outgoingBytesUntilNow;
                    lastSync = time;
                    registerMetricSync();
                });
    }

    private class WriterCallback implements Callback {
        private final MailboxExecutor mailboxExecutor;
        @Nullable private final Consumer<MessageId> metadataConsumer;

        public WriterCallback(
                MailboxExecutor mailboxExecutor,
                @Nullable Consumer<MessageId> metadataConsumer) {
            this.mailboxExecutor = mailboxExecutor;
            this.metadataConsumer = metadataConsumer;
        }

        @Override
        public void onCompletion(MessageId metadata, Exception exception) {
            if (exception != null) {
                FlinkPscInternalProducer<byte[], byte[]> producer =
                        PscWriter.this.currentProducer;
                mailboxExecutor.execute(
                        () -> {
                            numRecordsOutErrorsCounter.inc();
                            throwException(metadata, exception, producer);
                        },
                        "Failed to send data with PSC");
            }

            if (metadataConsumer != null) {
                metadataConsumer.accept(metadata);
            }
        }

        private void throwException(
                MessageId metadata,
                Exception exception,
                FlinkPscInternalProducer<byte[], byte[]> producer) {
            String message =
                    String.format("Failed to send data with PSC %s with %s ", metadata, producer);
            if (exception.getCause() instanceof UnknownProducerIdException) {
                message += PscCommitter.UNKNOWN_PRODUCER_ID_ERROR_MESSAGE;
            }
            throw new FlinkRuntimeException(message, exception);
        }
    }
}
