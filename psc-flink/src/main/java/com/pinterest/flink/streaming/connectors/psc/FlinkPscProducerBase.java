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

package com.pinterest.flink.streaming.connectors.psc;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import com.pinterest.psc.producer.Callback;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.ByteArraySerializer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import com.pinterest.flink.streaming.connectors.psc.internals.KeyedSerializationSchemaWrapper;
import com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscMetricWrapper;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import com.pinterest.flink.streaming.util.serialization.psc.KeyedSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.flink.util.PropertiesUtil;
import org.apache.flink.util.SerializableObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * Flink Sink to produce data into a backend pubsub topic.
 *
 * <p>Please note that this producer provides at-least-once reliability guarantees when
 * checkpoints are enabled and setFlushOnCheckpoint(true) is set.
 * Otherwise, the producer doesn't provide any reliability guarantees.
 *
 * @param <IN> Type of the messages to write into Kafka.
 */
@Internal
public abstract class FlinkPscProducerBase<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkPscProducerBase.class);

    private static final long serialVersionUID = 1L;

    /**
     * Configuration key for disabling the metrics reporting.
     */
    public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

    /**
     * User defined configuration for the Producer.
     */
    protected final Properties producerConfig;

    /**
     * The name of the default topic this producer is writing data to.
     */
    protected final String defaultTopicUri;

    /**
     * (Serializable) SerializationSchema for turning objects used with Flink into.
     * byte[] for Kafka.
     */
    protected final KeyedSerializationSchema<IN> schema;

    /**
     * User-provided partitioner for assigning an object to a Kafka partition for each topic.
     */
    protected final FlinkPscPartitioner<IN> flinkPscPartitioner;

    /**
     * Partitions of each topic.
     */
    protected final Map<String, int[]> topicUriPartitionsMap;

    /**
     * Flag indicating whether to accept failures (and log them), or to fail on failures.
     */
    protected boolean logFailuresOnly;

    /**
     * If true, the producer will wait until all outstanding records have been send to the broker.
     */
    protected boolean flushOnCheckpoint = true;

    // -------------------------------- Runtime fields ------------------------------------------

    /**
     * KafkaProducer instance.
     */
    protected transient PscProducer<byte[], byte[]> pscProducer;

    /**
     * The callback than handles error propagation or logging callbacks.
     */
    protected transient Callback callback;

    /**
     * Errors encountered in the async producer are stored here.
     */
    protected transient volatile Exception asyncException;

    /**
     * Lock for accessing the pending records.
     */
    protected final SerializableObject pendingRecordsLock = new SerializableObject();

    /**
     * Number of unacknowledged records.
     */
    protected long pendingRecords;

    private boolean hasInitializedMetrics = false;

    /**
     * The main constructor for creating a FlinkKafkaProducer.
     *
     * @param defaultTopicUri     The default topic to write data to
     * @param serializationSchema A serializable serialization schema for turning user objects into a PSC-consumable byte[] supporting key/value messages
     * @param producerConfig      Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
     * @param customPartitioner   A serializable partitioner for assigning messages to Kafka partitions. Passing null will use Kafka's partitioner.
     */
    public FlinkPscProducerBase(String defaultTopicUri, KeyedSerializationSchema<IN> serializationSchema, Properties producerConfig, FlinkPscPartitioner<IN> customPartitioner) {
        requireNonNull(defaultTopicUri, "TopicID not set");
        requireNonNull(serializationSchema, "serializationSchema not set");
        requireNonNull(producerConfig, "producerConfig not set");
        ClosureCleaner.clean(customPartitioner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        ClosureCleaner.ensureSerializable(serializationSchema);

        this.defaultTopicUri = defaultTopicUri;
        this.schema = serializationSchema;
        this.producerConfig = producerConfig;
        this.flinkPscPartitioner = customPartitioner;

        // set the producer configuration properties for PSC record key value serializers.
        if (!producerConfig.containsKey(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER)) {
            this.producerConfig.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, ByteArraySerializer.class.getName());
        } else {
            LOG.warn("Overwriting the '{}' is not recommended", PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER);
        }

        if (!producerConfig.containsKey(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER)) {
            this.producerConfig.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, ByteArraySerializer.class.getName());
        } else {
            LOG.warn("Overwriting the '{}' is not recommended", PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER);
        }

        this.topicUriPartitionsMap = new HashMap<>();
    }

    // ---------------------------------- Properties --------------------------

    /**
     * Defines whether the producer should fail on errors, or only log them.
     * If this is set to true, then exceptions will be only logged, if set to false,
     * exceptions will be eventually thrown and cause the streaming program to
     * fail (and enter recovery).
     *
     * @param logFailuresOnly The flag to indicate logging-only on exceptions.
     */
    public void setLogFailuresOnly(boolean logFailuresOnly) {
        this.logFailuresOnly = logFailuresOnly;
    }

    /**
     * If set to true, the Flink producer will wait for all outstanding messages in the Kafka buffers
     * to be acknowledged by the Kafka producer on a checkpoint.
     * This way, the producer can guarantee that messages in the Kafka buffers are part of the checkpoint.
     *
     * @param flush Flag indicating the flushing mode (true = flush on checkpoint)
     */
    public void setFlushOnCheckpoint(boolean flush) {
        this.flushOnCheckpoint = flush;
    }

    /**
     * Used for testing only.
     */
    @VisibleForTesting
    protected <K, V> PscProducer<K, V> getPscProducer(Properties properties) throws ProducerException, ConfigurationException {
        PscConfiguration pscConfiguration = new PscConfiguration();
        properties.forEach((key, value) -> pscConfiguration.setProperty(key.toString(), value));
        return new PscProducer<>(pscConfiguration);
    }

    // ----------------------------------- Utilities --------------------------

    /**
     * Initializes the connection to PSC.
     */
    @Override
    public void open(org.apache.flink.configuration.Configuration configuration) throws Exception {
        if (schema instanceof KeyedSerializationSchemaWrapper) {
            ((KeyedSerializationSchemaWrapper<IN>) schema).getSerializationSchema()
                .open(RuntimeContextInitializationContextAdapters.serializationAdapter(this.getRuntimeContext(), (metricGroup) -> {
                  return metricGroup.addGroup("user");
                }));
        }
        pscProducer = getPscProducer(this.producerConfig);

        RuntimeContext ctx = getRuntimeContext();

        if (null != flinkPscPartitioner) {
            flinkPscPartitioner.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
        }

        LOG.info("Starting FlinkKafkaProducer ({}/{}) to produce into default topic {}",
                ctx.getIndexOfThisSubtask() + 1, ctx.getNumberOfParallelSubtasks(), defaultTopicUri);

        if (!Boolean.parseBoolean(this.producerConfig.getProperty("flink.disable-metrics", "false"))) {
            Map<MetricName, ? extends Metric> metrics = this.pscProducer.metrics();
            if (metrics == null) {
                LOG.info("Producer implementation does not support metrics");
            } else {
                MetricGroup pscMetricGroup = this.getRuntimeContext().getMetricGroup().addGroup("PscProducer");
                for (Map.Entry<MetricName, ? extends Metric> metric: metrics.entrySet()) {
                    pscMetricGroup.gauge((metric.getKey()).name(), new PscMetricWrapper(metric.getValue()));
                }
            }
        }

        if (flushOnCheckpoint && !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
            LOG.warn("Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
            flushOnCheckpoint = false;
        }

        if (logFailuresOnly) {
            callback = new Callback() {
                @Override
                public void onCompletion(MessageId messageId, Exception exception) {
                    if (exception != null) {
                        LOG.error("Error while sending record using PSC producer: " + exception.getMessage(), exception);
                    }
                    acknowledgeMessage();
                }
            };
        } else {
            callback = new Callback() {
                @Override
                public void onCompletion(MessageId messageId, Exception exception) {
                    if (exception != null && asyncException == null) {
                        asyncException = exception;
                    }
                    acknowledgeMessage();
                }
            };
        }
    }

    private void initializeMetrics() throws ClientException {
        // register PSC metrics to Flink accumulators
        if (PropertiesUtil.getBoolean(producerConfig, KEY_DISABLE_METRICS, false)) {
            Map<MetricName, ? extends Metric> metrics = this.pscProducer.metrics();

            if (metrics == null) {
                // MapR's Kafka implementation returns null here.
                LOG.info("Producer implementation does not support metrics");
            } else {
                // PscProducer metric group may break metrics if migrating from Kafka (KafkaProducer metric group)
                final MetricGroup kafkaMetricGroup = getRuntimeContext().getMetricGroup().addGroup("PscProducer");
                for (Map.Entry<MetricName, ? extends Metric> metric : metrics.entrySet()) {
                    kafkaMetricGroup.gauge(metric.getKey().name(), new PscMetricWrapper(metric.getValue()));
                }
            }
        }
    }

    /**
     * Called when new data arrives to the sink, and forwards it to Kafka.
     *
     * @param next The incoming data
     */
    @Override
    public void invoke(IN next, Context context) throws Exception {
        // propagate asynchronous errors
        checkErroneous();

        byte[] serializedKey = schema.serializeKey(next);
        byte[] serializedValue = schema.serializeValue(next);
        String targetTopicUri = schema.getTargetTopic(next);
        if (targetTopicUri == null) {
            targetTopicUri = defaultTopicUri;
        }

        int[] partitions = this.topicUriPartitionsMap.get(targetTopicUri);
        if (null == partitions) {
            partitions = getPartitionsByTopic(targetTopicUri, pscProducer);
            this.topicUriPartitionsMap.put(targetTopicUri, partitions);
        }

        PscProducerMessage<byte[], byte[]> message;
        if (flinkPscPartitioner == null) {
            message = new PscProducerMessage<>(targetTopicUri, serializedKey, serializedValue);
        } else {
            message = new PscProducerMessage<>(
                    targetTopicUri,
                    flinkPscPartitioner.partition(next, serializedKey, serializedValue, targetTopicUri, partitions),
                    serializedKey,
                    serializedValue);
        }
        if (flushOnCheckpoint) {
            synchronized (pendingRecordsLock) {
                pendingRecords++;
            }
        }
        pscProducer.send(message, callback);
        if (!hasInitializedMetrics) {
            initializeMetrics();
            hasInitializedMetrics = true;
        }
    }

    @Override
    public void close() throws Exception {
        if (pscProducer != null) {
            pscProducer.close();
        }

        // make sure we propagate pending errors
        checkErroneous();
    }

    // ------------------- Logic for handling checkpoint flushing -------------------------- //

    private void acknowledgeMessage() {
        if (flushOnCheckpoint) {
            synchronized (pendingRecordsLock) {
                pendingRecords--;
                if (pendingRecords == 0) {
                    pendingRecordsLock.notifyAll();
                }
            }
        }
    }

    /**
     * Flush pending records.
     */
    protected abstract void flush();

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // nothing to do
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        // check for asynchronous errors and fail the checkpoint if necessary
        checkErroneous();

        if (flushOnCheckpoint) {
            // flushing is activated: We need to wait until pendingRecords is 0
            flush();
            synchronized (pendingRecordsLock) {
                if (pendingRecords != 0) {
                    throw new IllegalStateException("Pending record count must be zero at this point: " + pendingRecords);
                }

                // if the flushed requests has errors, we should propagate it also and fail the checkpoint
                checkErroneous();
            }
        }
    }

    // ----------------------------------- Utilities --------------------------

    protected void checkErroneous() throws Exception {
        Exception e = asyncException;
        if (e != null) {
            // prevent double throwing
            asyncException = null;
            throw new Exception("Failed to send data to Kafka: " + e.getMessage(), e);
        }
    }

    protected static int[] getPartitionsByTopic(String topicUri, PscProducer<byte[], byte[]> producer) throws ProducerException, ConfigurationException {
        // the fetched list is immutable, so we're creating a mutable copy in order to sort it
        List<TopicUriPartition> partitionsList = new ArrayList<>(producer.getPartitions(topicUri));

        // sort the partitions by partition id to make sure the fetched partition list is the same across subtasks
        Collections.sort(partitionsList, new Comparator<TopicUriPartition>() {
            @Override
            public int compare(TopicUriPartition o1, TopicUriPartition o2) {
                return o1.compareTo(o2);
            }
        });

        int[] partitions = new int[partitionsList.size()];
        for (int i = 0; i < partitions.length; i++) {
            partitions[i] = partitionsList.get(i).getPartition();
        }

        return partitions;
    }

    @VisibleForTesting
    protected long numPendingRecords() {
        synchronized (pendingRecordsLock) {
            return pendingRecords;
        }
    }
}
