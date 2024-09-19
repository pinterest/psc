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

import com.pinterest.flink.streaming.connectors.psc.internals.FlinkPscInternalProducer;
import com.pinterest.flink.streaming.connectors.psc.internals.PscSerializationSchemaWrapper;
import com.pinterest.flink.streaming.connectors.psc.internals.PscSimpleTypeSerializerSnapshot;
import com.pinterest.flink.streaming.connectors.psc.internals.TransactionalIdsGenerator;
import com.pinterest.flink.streaming.connectors.psc.internals.metrics.FlinkPscStateRecoveryMetricConstants;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.producer.Callback;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.producer.PscProducerTransactionalProperties;
import com.pinterest.psc.serde.ByteArraySerializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscMetricMutableWrapper;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkFixedPartitioner;
import com.pinterest.flink.streaming.util.serialization.psc.KeyedSerializationSchema;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Flink Sink to produce data into a Kafka topic. By default producer
 * will use {@link FlinkPscProducer.Semantic#AT_LEAST_ONCE} semantic.
 * Before using {@link FlinkPscProducer.Semantic#EXACTLY_ONCE} please refer to Flink's
 * Kafka connector documentation.
 */
@PublicEvolving
public class FlinkPscProducer<IN>
        extends TwoPhaseCommitSinkFunction<IN, FlinkPscProducer.PscTransactionState, FlinkPscProducer.PscTransactionContext> {

    /**
     * Semantics that can be chosen.
     * <li>{@link #EXACTLY_ONCE}</li>
     * <li>{@link #AT_LEAST_ONCE}</li>
     * <li>{@link #NONE}</li>
     */
    public enum Semantic {

        /**
         * Semantic.EXACTLY_ONCE the Flink producer will write all messages in a Kafka transaction that will be
         * committed to Kafka on a checkpoint.
         *
         * <p>In this mode {@link FlinkPscProducer} sets up a pool of {@link FlinkPscInternalProducer}. Between each
         * checkpoint a Kafka transaction is created, which is committed on
         * {@link FlinkPscProducer#notifyCheckpointComplete(long)}. If checkpoint complete notifications are
         * running late, {@link FlinkPscProducer} can run out of {@link FlinkPscInternalProducer}s in the pool. In that
         * case any subsequent {@link FlinkPscProducer#snapshotState(FunctionSnapshotContext)} requests will fail
         * and {@link FlinkPscProducer} will keep using the {@link FlinkPscInternalProducer}
         * from the previous checkpoint.
         * To decrease the chance of failing checkpoints there are four options:
         * <li>decrease number of max concurrent checkpoints</li>
         * <li>make checkpoints more reliable (so that they complete faster)</li>
         * <li>increase the delay between checkpoints</li>
         * <li>increase the size of {@link FlinkPscInternalProducer}s pool</li>
         */
        EXACTLY_ONCE,

        /**
         * Semantic.AT_LEAST_ONCE the Flink producer will wait for all outstanding messages in the Kafka buffers
         * to be acknowledged by the Kafka producer on a checkpoint.
         */
        AT_LEAST_ONCE,

        /**
         * Semantic.NONE means that nothing will be guaranteed. Messages can be lost and/or duplicated in case
         * of failure.
         */
        NONE
    }

    private static final Logger LOG = LoggerFactory.getLogger(FlinkPscProducer.class);

    private static final long serialVersionUID = 1L;

    /**
     * Number of characters to truncate the taskName to for the Kafka transactionalId. The maximum
     * this can possibly be set to is 32,767 - (length of operatorUniqueId).
     */
    private static final short maxTaskNameSize = 1_000;

    /**
     * This coefficient determines what is the safe scale down factor.
     *
     * <p>If the Flink application previously failed before first checkpoint completed or we are starting new batch
     * of {@link FlinkPscProducer} from scratch without clean shutdown of the previous one,
     * {@link FlinkPscProducer} doesn't know what was the set of previously used Kafka's transactionalId's. In
     * that case, it will try to play safe and abort all of the possible transactionalIds from the range of:
     * {@code [0, getNumberOfParallelSubtasks() * kafkaProducersPoolSize * SAFE_SCALE_DOWN_FACTOR) }
     *
     * <p>The range of available to use transactional ids is:
     * {@code [0, getNumberOfParallelSubtasks() * kafkaProducersPoolSize) }
     *
     * <p>This means that if we decrease {@code getNumberOfParallelSubtasks()} by a factor larger than
     * {@code SAFE_SCALE_DOWN_FACTOR} we can have a left some lingering transaction.
     */
    public static final int SAFE_SCALE_DOWN_FACTOR = 5;

    /**
     * Default number of KafkaProducers in the pool. See {@link FlinkPscProducer.Semantic#EXACTLY_ONCE}.
     */
    public static final int DEFAULT_PSC_PRODUCERS_POOL_SIZE = 5;

    /**
     * Default value for kafka transaction timeout.
     */
    public static final Time DEFAULT_PSC_TRANSACTION_TIMEOUT = Time.hours(1);

    /**
     * Configuration key for disabling the metrics reporting.
     */
    public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

    /**
     * Descriptor of the transactional IDs list.
     * Note: This state is serialized by Kryo Serializer and it has compatibility problem that will be removed later.
     * Please use NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR_V2.
     */
    @Deprecated
    private static final ListStateDescriptor<FlinkPscProducer.NextTransactionalIdHint> NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR =
            new ListStateDescriptor<>("next-psc-transactional-id-hint", TypeInformation.of(NextTransactionalIdHint.class));

    private static final ListStateDescriptor<FlinkPscProducer.NextTransactionalIdHint> NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR_V2 =
            new ListStateDescriptor<>("next-psc-transactional-id-hint-v2", new NextTransactionalIdHintSerializer());

    /**
     * State for nextTransactionalIdHint.
     */
    private transient ListState<FlinkPscProducer.NextTransactionalIdHint> nextTransactionalIdHintState;

    /**
     * Generator for Transactional IDs.
     */
    private transient TransactionalIdsGenerator transactionalIdsGenerator;

    /**
     * Hint for picking next transactional id.
     */
    private transient FlinkPscProducer.NextTransactionalIdHint nextTransactionalIdHint;

    /**
     * User defined properties for the Producer.
     */
    protected final Properties producerConfig;

    /**
     * The name of the default topic URI this producer is writing data to.
     */
    protected final String defaultTopicUri;

    /**
     * (Serializable) SerializationSchema for turning objects used with Flink into.
     * byte[] for Kafka.
     */
    @Nullable
    private final KeyedSerializationSchema<IN> keyedSchema;

    /**
     * (Serializable) serialization schema for serializing messages to
     * {@link PscProducerMessage ProducerMessages}.
     */
    @Nullable
    private final PscSerializationSchema<IN> pscSerializationSchema;

    /**
     * User-provided partitioner for assigning an object to a PSC partition for each topic URI.
     */
    @Nullable
    private final FlinkPscPartitioner<IN> flinkPscPartitioner;

    /**
     * Partitions of each topic URI.
     */
    protected final Map<String, int[]> topicUriPartitionsMap;

    /**
     * Max number of producers in the pool. If all producers are in use, snapshotting state will throw an exception.
     */
    private final int pscProducersPoolSize;

    /**
     * Pool of available transactional ids.
     */
    private final BlockingDeque<String> availableTransactionalIds = new LinkedBlockingDeque<>();

    /**
     * Flag controlling whether we are writing the Flink record's timestamp into the backend pubsub.
     */
    protected boolean writeTimestampToPubsub = false;

    /** The transactional.id prefix to be used by the producers when communicating with Kafka. */
    @Nullable
    private String transactionalIdPrefix = null;

    /**
     * Flag indicating whether to accept failures (and log them), or to fail on failures.
     */
    private boolean logFailuresOnly;

    private boolean registerMetrics = false;

    private boolean hasInitializedMetrics = false;

    /**
     * Semantic chosen for this instance.
     */
    protected FlinkPscProducer.Semantic semantic;

    // -------------------------------- Runtime fields ------------------------------------------

    /**
     * The callback than handles error propagation or logging callbacks.
     */
    @Nullable
    protected transient Callback callback;

    /**
     * Errors encountered in the async producer are stored here.
     */
    @Nullable
    protected transient volatile Exception asyncException;

    /**
     * Number of unacknowledged records.
     */
    protected final AtomicLong pendingRecords = new AtomicLong();

    /**
     * Cache of metrics to replace already registered metrics instead of overwriting existing ones.
     */
    private final Map<String, PscMetricMutableWrapper> previouslyCreatedMetrics = new HashMap<>();

    private transient AtomicBoolean pscMetricsInitialized = new AtomicBoolean(false);

    private transient PscConfigurationInternal pscConfigurationInternal;

    /**
     * Creates a FlinkPscProducer for a given topic. The sink produces a DataStream to
     * the topic.
     *
     * @param topicUri            PSC topic URI.
     * @param serializationSchema User defined (keyless) serialization schema.
     */
    public FlinkPscProducer(String topicUri, SerializationSchema<IN> serializationSchema) {
        this(topicUri, serializationSchema, new Properties());
    }

    /**
     * Creates a FlinkPscProducer for a given topic. The sink produces a DataStream to
     * the topic.
     *
     * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
     * the partitioner. This default partitioner maps each sink subtask to a single PSC topic
     * URI partition (i.e. all messages received by a sink subtask will end up in the same
     * PSC topic URI partition).
     *
     * <p>To use a custom partitioner, please use
     * {@link #FlinkPscProducer(String, SerializationSchema, Properties, Optional)} instead.
     *
     * @param topicUri            PSC topic URI.
     * @param serializationSchema User defined key-less serialization schema.
     * @param pscProducerConfig   Properties with the PSC producer configuration.
     */
    public FlinkPscProducer(String topicUri, SerializationSchema<IN> serializationSchema, Properties pscProducerConfig) {
        this(topicUri, serializationSchema, pscProducerConfig, Optional.of(new FlinkFixedPartitioner<>()));
    }

    /**
     * Creates a FlinkPscProducer for a given topic URI. The sink produces its input to
     * the topic URI. It accepts a key-less {@link SerializationSchema} and possibly a custom {@link FlinkPscPartitioner}.
     *
     * <p>Since a key-less {@link SerializationSchema} is used, all messages sent via PSC will not have an
     * attached key. Therefore, if a partitioner is also not provided, messages will be distributed to PSC
     * topic URI partitions in a round-robin fashion.
     *
     * @param topicUri            The topic URI to write data to
     * @param serializationSchema A key-less serializable serialization schema for turning user objects into a
     *                            PSC-consumable byte[]
     * @param pscProducerConfig   Properties properties for the PscProducer.
     * @param customPartitioner   A serializable partitioner for assigning messages to PSC topic URI partitions.
     *                            If a partitioner is not provided, messages will be distributed to PSC topic URI
     *                            partitions in a round-robin fashion.
     */
    public FlinkPscProducer(
            String topicUri,
            SerializationSchema<IN> serializationSchema,
            Properties pscProducerConfig,
            Optional<FlinkPscPartitioner<IN>> customPartitioner) {
        this(
                topicUri,
                serializationSchema,
                pscProducerConfig,
                customPartitioner.orElse(null),
                Semantic.AT_LEAST_ONCE,
                DEFAULT_PSC_PRODUCERS_POOL_SIZE);
    }

    /**
     * Creates a FlinkPscProducer for a given topic. The sink produces its input to
     * the topic. It accepts a key-less {@link SerializationSchema} and possibly a custom {@link FlinkPscPartitioner}.
     *
     * <p>Since a key-less {@link SerializationSchema} is used, all records sent to Kafka will not have an
     * attached key. Therefore, if a partitioner is also not provided, records will be distributed to Kafka
     * partitions in a round-robin fashion.
     *
     * @param topicUri             The topic to write data to
     * @param serializationSchema  A key-less serializable serialization schema for turning user objects into a kafka-consumable byte[]
     * @param pscProducerConfig    Properties properties for the KafkaProducer.
     * @param customPartitioner    A serializable partitioner for assigning messages to Kafka partitions. If a partitioner is not
     *                             provided, records will be distributed to Kafka partitions in a round-robin fashion.
     * @param semantic             Defines semantic that will be used by this producer (see {@link FlinkPscProducer.Semantic}).
     * @param pscProducersPoolSize Overwrite default KafkaProducers pool size (see {@link FlinkPscProducer.Semantic#EXACTLY_ONCE}).
     */
    public FlinkPscProducer(
            String topicUri,
            SerializationSchema<IN> serializationSchema,
            Properties pscProducerConfig,
            @Nullable FlinkPscPartitioner<IN> customPartitioner,
            Semantic semantic,
            int pscProducersPoolSize) {
        this(
                topicUri,
                null,
                null,
                new PscSerializationSchemaWrapper<>(topicUri, customPartitioner, false, serializationSchema),
                pscProducerConfig,
                semantic,
                pscProducersPoolSize
        );
    }

    // ------------------- Key/Value serialization schema constructors ----------------------

    /**
     * Creates a FlinkPscProducer for a given topic. The sink produces a DataStream to
     * the topic.
     *
     * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
     * the partitioner. This default partitioner maps each sink subtask to a single Kafka
     * partition (i.e. all records received by a sink subtask will end up in the same
     * Kafka partition).
     *
     * <p>To use a custom partitioner, please use
     * {@link #FlinkPscProducer(String, KeyedSerializationSchema, Properties, Optional)} instead.
     *
     * @param topicUri            PSC topic URI.
     * @param serializationSchema User defined serialization schema supporting key/value messages
     * @deprecated use {@link #FlinkPscProducer(String, PscSerializationSchema, Properties, FlinkPscProducer.Semantic)}
     */
    @Deprecated
    public FlinkPscProducer(String topicUri, KeyedSerializationSchema<IN> serializationSchema) {
        this(
                topicUri,
                serializationSchema,
                new Properties(),
                Optional.of(new FlinkFixedPartitioner<IN>()));
    }

    /**
     * Creates a FlinkPscProducer for a given topic. The sink produces a DataStream to
     * the topic.
     *
     * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
     * the partitioner. This default partitioner maps each sink subtask to a single Kafka
     * partition (i.e. all records received by a sink subtask will end up in the same
     * Kafka partition).
     *
     * <p>To use a custom partitioner, please use
     * {@link #FlinkPscProducer(String, KeyedSerializationSchema, Properties, Optional)} instead.
     *
     * @param topicUri            ID of the Kafka topic.
     * @param serializationSchema User defined serialization schema supporting key/value messages
     * @param pscProducerConfig   Properties with the producer configuration.
     * @deprecated use {@link #FlinkPscProducer(String, PscSerializationSchema, Properties, FlinkPscProducer.Semantic)}
     */
    @Deprecated
    public FlinkPscProducer(String topicUri, KeyedSerializationSchema<IN> serializationSchema, Properties pscProducerConfig) {
        this(
                topicUri,
                serializationSchema,
                pscProducerConfig,
                Optional.of(new FlinkFixedPartitioner<IN>()));
    }

    /**
     * Creates a FlinkPscProducer for a given topic. The sink produces a DataStream to
     * the topic.
     *
     * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
     * the partitioner. This default partitioner maps each sink subtask to a single Kafka
     * partition (i.e. all records received by a sink subtask will end up in the same
     * Kafka partition).
     *
     * @param topicUri            ID of the Kafka topic.
     * @param serializationSchema User defined serialization schema supporting key/value messages
     * @param pscProducerConfig   Properties with the producer configuration.
     * @param semantic            Defines semantic that will be used by this producer (see {@link FlinkPscProducer.Semantic}).
     * @deprecated use {@link #FlinkPscProducer(String, PscSerializationSchema, Properties, FlinkPscProducer.Semantic)}
     */
    @Deprecated
    public FlinkPscProducer(
            String topicUri,
            KeyedSerializationSchema<IN> serializationSchema,
            Properties pscProducerConfig,
            FlinkPscProducer.Semantic semantic) {
        this(topicUri,
                serializationSchema,
                pscProducerConfig,
                Optional.of(new FlinkFixedPartitioner<IN>()),
                semantic,
                DEFAULT_PSC_PRODUCERS_POOL_SIZE);
    }


    /**
     * Creates a FlinkPscProducer for a given topic. The sink produces its input to
     * the topic. It accepts a keyed {@link KeyedSerializationSchema} and possibly a custom {@link FlinkPscPartitioner}.
     *
     * <p>If a partitioner is not provided, written records will be partitioned by the attached key of each
     * record (as determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If written records do not
     * have a key (i.e., {@link KeyedSerializationSchema#serializeKey(Object)} returns {@code null}), they
     * will be distributed to Kafka partitions in a round-robin fashion.
     *
     * @param defaultTopicUri     The default topic to write data to
     * @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
     * @param producerConfig      Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
     * @param customPartitioner   A serializable partitioner for assigning messages to Kafka partitions.
     *                            If a partitioner is not provided, records will be partitioned by the key of each record
     *                            (determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If the keys
     *                            are {@code null}, then records will be distributed to Kafka partitions in a
     *                            round-robin fashion.
     * @deprecated use {@link #FlinkPscProducer(String, PscSerializationSchema, Properties, FlinkPscProducer.Semantic)}
     */
    @Deprecated
    public FlinkPscProducer(
            String defaultTopicUri,
            KeyedSerializationSchema<IN> serializationSchema,
            Properties producerConfig,
            Optional<FlinkPscPartitioner<IN>> customPartitioner
    ) {
        this(
                defaultTopicUri,
                serializationSchema,
                producerConfig,
                customPartitioner,
                FlinkPscProducer.Semantic.AT_LEAST_ONCE,
                DEFAULT_PSC_PRODUCERS_POOL_SIZE
        );
    }

    /**
     * Creates a FlinkPscProducer for a given topic. The sink produces its input to
     * the topic. It accepts a keyed {@link KeyedSerializationSchema} and possibly a custom {@link FlinkPscPartitioner}.
     *
     * <p>If a partitioner is not provided, written records will be partitioned by the attached key of each
     * record (as determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If written records do not
     * have a key (i.e., {@link KeyedSerializationSchema#serializeKey(Object)} returns {@code null}), they
     * will be distributed to Kafka partitions in a round-robin fashion.
     *
     * @param defaultTopicUri      The default topic to write data to
     * @param serializationSchema  A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
     * @param producerConfig       Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
     * @param customPartitioner    A serializable partitioner for assigning messages to Kafka partitions.
     *                             If a partitioner is not provided, records will be partitioned by the key of each record
     *                             (determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If the keys
     *                             are {@code null}, then records will be distributed to Kafka partitions in a
     *                             round-robin fashion.
     * @param semantic             Defines semantic that will be used by this producer (see {@link FlinkPscProducer.Semantic}).
     * @param pscProducersPoolSize Overwrite default KafkaProducers pool size (see {@link FlinkPscProducer.Semantic#EXACTLY_ONCE}).
     * @deprecated use {@link #FlinkPscProducer(String, PscSerializationSchema, Properties, FlinkPscProducer.Semantic)}
     */
    @Deprecated
    public FlinkPscProducer(
            String defaultTopicUri,
            KeyedSerializationSchema<IN> serializationSchema,
            Properties producerConfig,
            Optional<FlinkPscPartitioner<IN>> customPartitioner,
            FlinkPscProducer.Semantic semantic,
            int pscProducersPoolSize
    ) {
        this(
                defaultTopicUri,
                serializationSchema,
                customPartitioner.orElse(null),
                null, /* kafka serialization schema */
                producerConfig,
                semantic,
                pscProducersPoolSize);
    }

    /**
     * Creates a {@link FlinkPscProducer} for a given topic. The sink produces its input to
     * the topic. It accepts a {@link PscSerializationSchema} for serializing records to
     * a {@link PscProducerMessage}, including partitioning information.
     *
     * @param defaultTopicUri     The default topic to write data to
     * @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
     * @param pscProducerConfig   Properties properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
     * @param semantic            Defines semantic that will be used by this producer (see {@link FlinkPscProducer.Semantic}).
     */
    public FlinkPscProducer(
            String defaultTopicUri,
            PscSerializationSchema<IN> serializationSchema,
            Properties pscProducerConfig,
            FlinkPscProducer.Semantic semantic
    ) {
        this(
                defaultTopicUri,
                serializationSchema,
                pscProducerConfig,
                semantic,
                DEFAULT_PSC_PRODUCERS_POOL_SIZE);
    }

    /**
     * Creates a FlinkPscProducer for a given topic. The sink produces its input to
     * the topic. It accepts a {@link PscSerializationSchema} and possibly a custom {@link FlinkPscPartitioner}.
     *
     * @param defaultTopicUri      The default topic to write data to
     * @param serializationSchema  A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
     * @param producerConfig       Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
     * @param semantic             Defines semantic that will be used by this producer (see {@link FlinkPscProducer.Semantic}).
     * @param pscProducersPoolSize Overwrite default KafkaProducers pool size (see {@link FlinkPscProducer.Semantic#EXACTLY_ONCE}).
     */
    public FlinkPscProducer(
            String defaultTopicUri,
            PscSerializationSchema<IN> serializationSchema,
            Properties producerConfig,
            FlinkPscProducer.Semantic semantic,
            int pscProducersPoolSize
    ) {
        this(
                defaultTopicUri,
                null, null, /* keyed schema and FlinkPscPartitioner */
                serializationSchema,
                producerConfig,
                semantic,
                pscProducersPoolSize
        );
    }

    /**
     * Creates a FlinkPscProducer for a given topic. The sink produces its input to
     * the topic. It accepts a {@link PscSerializationSchema} and possibly a custom {@link FlinkPscPartitioner}.
     *
     * <p>If a partitioner is not provided, written records will be partitioned by the attached key of each
     * record (as determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If written records do not
     * have a key (i.e., {@link KeyedSerializationSchema#serializeKey(Object)} returns {@code null}), they
     * will be distributed to Kafka partitions in a round-robin fashion.
     *
     * @param defaultTopicUri        The default topic to write data to
     * @param keyedSchema            A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
     * @param customPartitioner      A serializable partitioner for assigning messages to Kafka partitions.
     *                               If a partitioner is not provided, records will be partitioned by the key of each record
     *                               (determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If the keys
     *                               are {@code null}, then records will be distributed to Kafka partitions in a
     *                               round-robin fashion.
     * @param pscSerializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
     * @param producerConfig         Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
     * @param semantic               Defines semantic that will be used by this producer (see {@link FlinkPscProducer.Semantic}).
     * @param pscProducersPoolSize   Overwrite default KafkaProducers pool size (see {@link FlinkPscProducer.Semantic#EXACTLY_ONCE}).
     */
    private FlinkPscProducer(
            String defaultTopicUri,
            KeyedSerializationSchema<IN> keyedSchema,
            FlinkPscPartitioner<IN> customPartitioner,
            PscSerializationSchema<IN> pscSerializationSchema,
            Properties producerConfig,
            FlinkPscProducer.Semantic semantic,
            int pscProducersPoolSize
    ) {
        super(new FlinkPscProducer.TransactionStateSerializer(), new FlinkPscProducer.ContextStateSerializer());
        this.defaultTopicUri = checkNotNull(defaultTopicUri, "defaultTopicUri is null");

        if (pscSerializationSchema != null) {
            this.keyedSchema = null;
            this.pscSerializationSchema = pscSerializationSchema;
            this.flinkPscPartitioner = null;
            ClosureCleaner.clean(
                    this.pscSerializationSchema, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

            if (customPartitioner != null) {
                throw new IllegalArgumentException("Customer partitioner can only be used when" +
                        "using a KeyedSerializationSchema or SerializationSchema.");
            }
        } else if (keyedSchema != null) {
            this.pscSerializationSchema = null;
            this.keyedSchema = keyedSchema;
            this.flinkPscPartitioner = customPartitioner;
            ClosureCleaner.clean(
                    this.flinkPscPartitioner,
                    ExecutionConfig.ClosureCleanerLevel.RECURSIVE,
                    true);
            ClosureCleaner.clean(
                    this.keyedSchema, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        } else {
            throw new IllegalArgumentException(
                    "You must provide either a KafkaSerializationSchema or a" +
                            "KeyedSerializationSchema.");
        }

        this.producerConfig = checkNotNull(producerConfig, "producerConfig is null");
        this.semantic = checkNotNull(semantic, "semantic is null");
        this.pscProducersPoolSize = pscProducersPoolSize;
        checkState(pscProducersPoolSize > 0, "kafkaProducersPoolSize must be non empty");

        // set the producer configuration properties for kafka record key value serializers.
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

        if (!producerConfig.containsKey(PscConfiguration.PSC_PRODUCER_TRANSACTION_TIMEOUT_MS)) {
            long timeout = DEFAULT_PSC_TRANSACTION_TIMEOUT.toMilliseconds();
            checkState(timeout < Integer.MAX_VALUE && timeout > 0, "timeout does not fit into 32 bit integer");
            this.producerConfig.put(PscConfiguration.PSC_PRODUCER_TRANSACTION_TIMEOUT_MS, (int) timeout);
            LOG.warn("Property [{}] not specified. Setting it to {}", PscConfiguration.PSC_PRODUCER_TRANSACTION_TIMEOUT_MS, DEFAULT_PSC_TRANSACTION_TIMEOUT);
        }

        // Enable transactionTimeoutWarnings to avoid silent data loss
        // See KAFKA-6119 (affects versions 0.11.0.0 and 0.11.0.1):
        // The PscProducer may not throw an exception if the transaction failed to commit
        if (semantic == FlinkPscProducer.Semantic.EXACTLY_ONCE) {
            final Object object = this.producerConfig.get(PscConfiguration.PSC_PRODUCER_TRANSACTION_TIMEOUT_MS);
            final long transactionTimeout;
            if (object instanceof String && StringUtils.isNumeric((String) object)) {
                transactionTimeout = Long.parseLong((String) object);
            } else if (object instanceof Number) {
                transactionTimeout = ((Number) object).longValue();
            } else {
                throw new IllegalArgumentException(PscConfiguration.PSC_PRODUCER_TRANSACTION_TIMEOUT_MS
                        + " must be numeric, was " + object);
            }
            super.setTransactionTimeout(transactionTimeout);
            super.enableTransactionTimeoutWarnings(0.8);
        }

        this.topicUriPartitionsMap = new HashMap<>();

        if (pscMetricsInitialized.compareAndSet(false, true)) {
            PscConfiguration pscConfiguration = new PscConfiguration();
            this.producerConfig.keySet().stream().map(Object::toString).filter(key -> key.startsWith("psc.")).forEach(key ->
                    pscConfiguration.setProperty(key, this.producerConfig.get(key))
            );
            try {
                PscMetricRegistryManager.getInstance().initialize(
                        new PscConfigurationInternal(pscConfiguration, PscConfiguration.PSC_CLIENT_TYPE_PRODUCER, true)
                );
            } catch (ConfigurationException configurationException) {
                throw new IllegalArgumentException(configurationException);
            }
        }
    }

    // ---------------------------------- Properties --------------------------

    /**
     * If set to true, Flink will write the (event time) timestamp attached to each record into Kafka.
     * Timestamps must be positive for Kafka to accept them.
     *
     * @param writeTimestampToPubsub Flag indicating if Flink's internal timestamps are written to Kafka.
     */
    public void setWriteTimestampToPubsub(boolean writeTimestampToPubsub) {
        this.writeTimestampToPubsub = writeTimestampToPubsub;
        if (pscSerializationSchema instanceof PscSerializationSchemaWrapper) {
            ((PscSerializationSchemaWrapper<IN>) pscSerializationSchema).setWriteTimestamp(writeTimestampToPubsub);
        }
    }

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
     * Specifies the prefix of the transactional.id property to be used by the producers when
     * communicating with Kafka. If not set, the transactional.id will be prefixed with {@code
     * taskName + "-" + operatorUid}.
     *
     * <p>Note that, if we change the prefix when the Flink application previously failed before
     * first checkpoint completed or we are starting new batch of {@link FlinkKafkaProducer} from
     * scratch without clean shutdown of the previous one, since we don't know what was the
     * previously used transactional.id prefix, there will be some lingering transactions left.
     *
     * @param transactionalIdPrefix the transactional.id prefix
     * @throws NullPointerException Thrown, if the transactionalIdPrefix was null.
     */
    public void setTransactionalIdPrefix(String transactionalIdPrefix) {
        this.transactionalIdPrefix = Preconditions.checkNotNull(transactionalIdPrefix);
    }

    /**
     * Disables the propagation of exceptions thrown when committing presumably timed out Kafka
     * transactions during recovery of the job. If a Kafka transaction is timed out, a commit will
     * never be successful. Hence, use this feature to avoid recovery loops of the Job. Exceptions
     * will still be logged to inform the user that data loss might have occurred.
     *
     * <p>Note that we use {@link System#currentTimeMillis()} to track the age of a transaction.
     * Moreover, only exceptions thrown during the recovery are caught, i.e., the producer will
     * attempt at least one commit of the transaction before giving up.</p>
     */
    @Override
    public FlinkPscProducer<IN> ignoreFailuresAfterTransactionTimeout() {
        super.ignoreFailuresAfterTransactionTimeout();
        return this;
    }

    // ----------------------------------- Utilities --------------------------

    /**
     * Initializes the connection to Kafka.
     */
    @Override
    public void open(Configuration configuration) throws Exception {
        if (logFailuresOnly) {
            callback = new Callback() {
                @Override
                public void onCompletion(MessageId messageId, Exception exception) {
                    if (exception != null) {
                        LOG.error("Error while sending a PSC messages: " + exception.getMessage(), exception);
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

        RuntimeContext ctx = this.getRuntimeContext();
        if (this.flinkPscPartitioner != null) {
            this.flinkPscPartitioner.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
        }

        if (this.pscSerializationSchema instanceof PscContextAware) {
            PscContextAware<IN> contextAwareSchema = (PscContextAware)this.pscSerializationSchema;
            contextAwareSchema.setParallelInstanceId(ctx.getIndexOfThisSubtask());
            contextAwareSchema.setNumParallelInstances(ctx.getNumberOfParallelSubtasks());
        }

        if (this.pscSerializationSchema != null) {
            this.pscSerializationSchema.open(RuntimeContextInitializationContextAdapters.serializationAdapter(this.getRuntimeContext(), (metricGroup) -> {
                return metricGroup.addGroup("user");
            }));
        }

        super.open(configuration);
    }

    @Override
    public void invoke(FlinkPscProducer.PscTransactionState transaction, IN next, Context context) throws FlinkPscException {
        checkErroneous();

        PscProducerMessage<byte[], byte[]> pscProducerMessage;
        if (keyedSchema != null) {
            byte[] serializedKey = keyedSchema.serializeKey(next);
            byte[] serializedValue = keyedSchema.serializeValue(next);
            String targetTopicUri = keyedSchema.getTargetTopic(next);
            if (targetTopicUri == null) {
                targetTopicUri = defaultTopicUri;
            }

            Long timestamp = null;
            if (this.writeTimestampToPubsub) {
                timestamp = context.timestamp();
            }

            int[] partitions = topicUriPartitionsMap.get(targetTopicUri);
            if (null == partitions) {
                partitions = getPartitionsByTopicUri(targetTopicUri, transaction.producer);
                topicUriPartitionsMap.put(targetTopicUri, partitions);
            }
            if (flinkPscPartitioner != null) {
                pscProducerMessage = timestamp == null ?
                        new PscProducerMessage<>(
                                targetTopicUri,
                                flinkPscPartitioner.partition(next, serializedKey, serializedValue, targetTopicUri, partitions),
                                serializedKey,
                                serializedValue) :
                        new PscProducerMessage<>(
                                targetTopicUri,
                                flinkPscPartitioner.partition(next, serializedKey, serializedValue, targetTopicUri, partitions),
                                serializedKey,
                                serializedValue,
                                timestamp);
            } else {
                pscProducerMessage = timestamp == null ?
                        new PscProducerMessage<>(
                                targetTopicUri,
                                serializedKey,
                                serializedValue) :
                        new PscProducerMessage<>(
                                targetTopicUri,
                                serializedKey,
                                serializedValue,
                                timestamp);
            }
        }  else {
            if (this.pscSerializationSchema == null) {
                throw new RuntimeException("We have neither KafkaSerializationSchema nor KeyedSerializationSchema, thisis a bug.");
            }

            if (this.pscSerializationSchema instanceof PscContextAware) {
                PscContextAware<IN> contextAwareSchema = (PscContextAware)this.pscSerializationSchema;
                String targetTopicUri = contextAwareSchema.getTargetTopicUri(next);
                if (targetTopicUri == null) {
                    targetTopicUri = this.defaultTopicUri;
                }

                int[] partitions = (int[])this.topicUriPartitionsMap.get(targetTopicUri);
                if (null == partitions) {
                    partitions = getPartitionsByTopicUri(targetTopicUri, transaction.producer);
                    this.topicUriPartitionsMap.put(targetTopicUri, partitions);
                }

                contextAwareSchema.setPartitions(partitions);
            }

            pscProducerMessage = this.pscSerializationSchema.serialize(next, context.timestamp());
        }

        this.pendingRecords.incrementAndGet();
        try {
            transaction.producer.send(pscProducerMessage, this.callback);
            if (transaction.needsTransactionalStateCompletion())
                transaction.setProducerIdAndEpoch(pscProducerMessage);
            if (!hasInitializedMetrics) {
                initializeMetrics(transaction.producer);
                hasInitializedMetrics = true;
            }
        } catch (ProducerException | ConfigurationException e) {
            throw new FlinkPscException(
                    FlinkPscErrorCode.EXTERNAL_ERROR,
                    String.format("Failed to send PSC producer message with spec %s.", pscProducerMessage.toString(false)),
                    e
            );
        }
    }

    @Override
    public void close() throws FlinkPscException {
        // First close the producer for current transaction.
        try {
            final PscTransactionState currentTransaction = currentTransaction();
            if (currentTransaction != null) {
                // to avoid exceptions on aborting transactions with some pending records
                flush(currentTransaction);

                // normal abort for AT_LEAST_ONCE and NONE do not clean up resources because of producer reusing, thus
                // we need to close it manually
                switch (semantic) {
                    case EXACTLY_ONCE:
                        break;
                    case AT_LEAST_ONCE:
                    case NONE:
                        currentTransaction.producer.flush();
                        currentTransaction.producer.close(Duration.ofSeconds(0));
                        break;
                }
            }
            super.close();
        } catch (Exception e) {
            asyncException = ExceptionUtils.firstOrSuppressed(e, asyncException);
        } finally {
            // We may have to close producer of the current transaction in case some exception was thrown before
            // the normal close routine finishes.
            if (currentTransaction() != null) {
                try {
                    closeProducer(currentTransaction().producer);
                } catch (Throwable t) {
                    LOG.warn("Error closing producer.", t);
                }
            }
            // Make sure all the producers for pending transactions are closed.
            pendingTransactions().forEach(transaction -> {
                try {
                    closeProducer(transaction.getValue().producer);
                } catch (Throwable t) {
                    LOG.warn("Error closing producer.", t);
                }
            });

            if (pscConfigurationInternal == null) {
                pscConfigurationInternal = PscConfigurationUtils.propertiesToPscConfigurationInternal(producerConfig, PscConfiguration.PSC_CLIENT_TYPE_PRODUCER);
            }

            if (pscMetricsInitialized != null && pscMetricsInitialized.compareAndSet(true, false))
                PscMetricRegistryManager.getInstance().shutdown(pscConfigurationInternal);

            // make sure we propagate pending errors
            checkErroneous();
        }
    }

    // ------------------- Logic for handling checkpoint flushing -------------------------- //

    @Override
    protected FlinkPscProducer.PscTransactionState beginTransaction() throws FlinkPscException {
        switch (semantic) {
            case EXACTLY_ONCE:
                FlinkPscInternalProducer<byte[], byte[]> producer = createTransactionalProducer();
                beginTransaction(producer);
                return new FlinkPscProducer.PscTransactionState(producer.getTransactionalId(), producer);
            case AT_LEAST_ONCE:
            case NONE:
                // Do not create new producer on each beginTransaction() if it is not necessary
                final FlinkPscProducer.PscTransactionState currentTransaction = currentTransaction();
                if (currentTransaction != null && currentTransaction.producer != null) {
                    return new FlinkPscProducer.PscTransactionState(currentTransaction.producer);
                }
                return new FlinkPscProducer.PscTransactionState(initNonTransactionalProducer(true));
            default:
                throw new UnsupportedOperationException("Not implemented semantic");
        }
    }

    @Override
    protected void preCommit(FlinkPscProducer.PscTransactionState transaction) throws FlinkPscException {
        switch (semantic) {
            case EXACTLY_ONCE:
            case AT_LEAST_ONCE:
                flush(transaction);
                break;
            case NONE:
                return;
            default:
                throw new UnsupportedOperationException("Not implemented semantic");
        }
        checkErroneous();
    }

    @Override
    protected void commit(FlinkPscProducer.PscTransactionState transaction) {
        if (transaction.isTransactional()) {
            try {
                commitTransaction(transaction.producer);
            } catch (FlinkPscException e) {
                LOG.error("Failed to commit transaction {} via PSC producer.", transaction, e);
            } finally {
                recycleTransactionalProducer(transaction.producer);
            }
        }
    }

    @Override
    protected void recoverAndCommit(FlinkPscProducer.PscTransactionState transaction) {
        if (transaction.isTransactional()) {
            FlinkPscInternalProducer<byte[], byte[]> producer = null;
            try {
                producer = initTransactionalProducer(transaction.transactionalId, false);
                //TODO: Generalize
                PscProducerTransactionalProperties pscProducerTransactionalProperties = new PscProducerTransactionalProperties(
                        transaction.producerId,
                        transaction.epoch
                );
                Set<String> topicUris = new HashSet<>(topicUriPartitionsMap.keySet());
                topicUris.add(defaultTopicUri);
                producer.resumeTransaction(pscProducerTransactionalProperties, topicUris);
                commitTransaction(producer);
            } catch (FlinkPscException | ProducerException e) {
                LOG.warn("Encountered exception during recoverAndCommit()", e);
                if (e.getCause() != null) {
                    // That means we may have committed this transaction before.
                    if (e.getCause().getClass().isAssignableFrom(InvalidTxnStateException.class)) {
                        LOG.warn(
                                "Unable to commit recovered transaction ({}) because it's in an invalid state. "
                                        + "Most likely the transaction has been aborted for some reason. Please check the Kafka logs for more details.",
                                transaction,
                                e);
                    }
                    if (e.getCause().getClass().isAssignableFrom(ProducerFencedException.class)) {
                        LOG.warn(
                                "Unable to commit recovered transaction ({}) because its producer is already fenced."
                                        + " This means that you either have a different producer with the same '{}' or"
                                        + " recovery took longer than '{}' ({}ms). In both cases this most likely signals data loss,"
                                        + " please consult the Flink documentation for more details.",
                                transaction,
                                ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                                producerConfig.getProperty(PscConfiguration.PSC_PRODUCER_TRANSACTION_TIMEOUT_MS),
                                e);
                    }
                }
            } finally {
                if (producer != null) {
                    closeProducer(producer);
                }
            }
        }
    }

    @Override
    protected void abort(FlinkPscProducer.PscTransactionState transaction) {
        if (transaction.isTransactional()) {
            try {
                abortTransaction(transaction.producer);
            } catch (FlinkPscException e) {
                LOG.error("Failed to abort transaction with id {}.", transaction.transactionalId, e);
            }
            recycleTransactionalProducer(transaction.producer);
        }
    }

    @Override
    protected void recoverAndAbort(FlinkPscProducer.PscTransactionState transaction) {
        if (transaction.isTransactional()) {
            FlinkPscInternalProducer<byte[], byte[]> producer = null;
            try {
                producer = initTransactionalProducer(transaction.transactionalId, false);
                initTransactions(producer);
            } catch (FlinkPscException e) {
                LOG.error(
                        "Failed to initialize transactional PSC producer for transaction id {}," +
                                "or initializing transactions by that producer (id {})",
                        transaction.transactionalId,
                        producer.getClientId(),
                        e
                );
            } finally {
                if (producer != null) {
                    closeProducer(producer);
                }
            }
        }
    }

    /**
     * <b>ATTENTION to subclass implementors:</b> When overriding this method, please always call
     * {@code super.acknowledgeMessage()} to keep the invariants of the internal bookkeeping of the producer.
     * If not, be sure to know what you are doing.
     */
    protected void acknowledgeMessage() {
        pendingRecords.decrementAndGet();
    }

    /**
     * Flush pending records.
     *
     * @param transaction
     */
    private void flush(FlinkPscProducer.PscTransactionState transaction) throws FlinkPscException {
        if (transaction.producer != null) {
            flushProducer(transaction.producer);
        }
        long pendingRecordsCount = pendingRecords.get();
        if (pendingRecordsCount != 0) {
            throw new IllegalStateException("Pending record count must be zero at this point: " + pendingRecordsCount);
        }

        // if the flushed requests has errors, we should propagate it also and fail the checkpoint
        // Flink 1.15 FlinkKafkaProducer has this in an else statement, but due to comment above we are leaving as is
        checkErroneous();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        supersSnapshotState(context);

        nextTransactionalIdHintState.clear();
        // To avoid duplication only first subtask keeps track of next transactional id hint.
        // Otherwise all of the
        // subtasks would write exactly same information.
        if (getRuntimeContext().getIndexOfThisSubtask() == 0
                && semantic == FlinkPscProducer.Semantic.EXACTLY_ONCE) {
            checkState(
                    nextTransactionalIdHint != null,
                    "nextTransactionalIdHint must be set for EXACTLY_ONCE");
            long nextFreeTransactionalId = nextTransactionalIdHint.nextFreeTransactionalId;

            // If we scaled up, some (unknown) subtask must have created new transactional ids from scratch. In that
            // case we adjust nextFreeTransactionalId by the range of transactionalIds that could be used for this
            // scaling up.
            if (getRuntimeContext().getNumberOfParallelSubtasks()
                    > nextTransactionalIdHint.lastParallelism) {
                nextFreeTransactionalId +=
                        getRuntimeContext().getNumberOfParallelSubtasks() * pscProducersPoolSize;
            }

            nextTransactionalIdHintState.add(
                    new FlinkPscProducer.NextTransactionalIdHint(
                            getRuntimeContext().getNumberOfParallelSubtasks(),
                            nextFreeTransactionalId));
        }
    }

    private void supersSnapshotState(FunctionSnapshotContext context) throws Exception {
        try {
            Class superClass = getClass().getSuperclass();
            Field currentTransactionHolderField;
            Field finishedField;
            while (true) {
                // loop is needed for the case of a child class instance
                try {
                    currentTransactionHolderField = superClass.getDeclaredField("currentTransactionHolder");
                    finishedField = superClass.getDeclaredField("finished");
                    break;
                } catch (NoSuchFieldException exception) {
                    superClass = superClass.getSuperclass();
                }
            }

            currentTransactionHolderField.setAccessible(true);
            finishedField.setAccessible(true);
            TwoPhaseCommitSinkFunction.TransactionHolder<PscTransactionState> currentTransactionHolder =
                    (TwoPhaseCommitSinkFunction.TransactionHolder<PscTransactionState>) currentTransactionHolderField.get(this);
            boolean finished = (boolean) finishedField.get(this);

            Method nameMethod = superClass.getDeclaredMethod("name");
            nameMethod.setAccessible(true);

            Method beginTransactionInternalMethod = superClass.getDeclaredMethod("beginTransactionInternal");
            beginTransactionInternalMethod.setAccessible(true);

            PscTransactionState handle = currentTransaction();

            long checkpointId = context.getCheckpointId();
            LOG.debug("{} - checkpoint {} triggered, flushing transaction '{}'", new Object[]{nameMethod.invoke(this), context.getCheckpointId(), currentTransactionHolder});

            if (currentTransactionHolder != null) {
                this.preCommit(handle);
                this.pendingCommitTransactions.put(checkpointId, currentTransactionHolder);
                LOG.debug("{} - stored pending transactions {}", nameMethod.invoke(this), this.pendingCommitTransactions);
            }

            if (!finished) {
                currentTransactionHolder = (TransactionHolder<PscTransactionState>) beginTransactionInternalMethod.invoke(this);
            } else {
                currentTransactionHolder = null;
            }

            if (currentTransactionHolder != null) {
                Field handleField = currentTransactionHolder.getClass().getDeclaredField("handle");
                handleField.setAccessible(true);
                PscTransactionState newHandle = (PscTransactionState) handleField.get(currentTransactionHolder);
                if (newHandle.isTransactional()) {
                    PscProducerTransactionalProperties pscProducerTransactionalProperties = initTransactions(newHandle.producer);
                    newHandle.setProducerTransactionalProperties(pscProducerTransactionalProperties);

                    handleField = superClass.getDeclaredField("currentTransactionHolder");
                    handleField.setAccessible(true);
                    handleField.set(this, currentTransactionHolder);
                }
            }

            LOG.debug("{} - started new transaction '{}'", nameMethod.invoke(this), currentTransactionHolder);
            this.state.clear();
            this.state.add(new TwoPhaseCommitSinkFunction.State(currentTransactionHolder, new ArrayList(this.pendingCommitTransactions.values()), this.userContext));
        } catch (InvocationTargetException exception) {
            throw (Exception) exception.getTargetException();
        }

        PscMetricRegistryManager.getInstance().updateHistogramMetric(
                null, FlinkPscStateRecoveryMetricConstants.PSC_SINK_STATE_SNAPSHOT_PSC_PENDING_TRANSACTIONS, pendingCommitTransactions.size(), pscConfigurationInternal
        );
        PscMetricRegistryManager.getInstance().updateHistogramMetric(
                null, FlinkPscStateRecoveryMetricConstants.PSC_SINK_STATE_SNAPSHOT_PSC_STATE_SIZE, getSize(state), pscConfigurationInternal
        );
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (semantic != FlinkPscProducer.Semantic.NONE
                && !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
            LOG.warn(
                    "Using {} semantic, but checkpointing is not enabled. Switching to {} semantic.",
                    semantic,
                    FlinkPscProducer.Semantic.NONE);
            semantic = FlinkPscProducer.Semantic.NONE;
        }

        Set<String> registeredStateNames = context.getOperatorStateStore().getRegisteredStateNames();
        if (registeredStateNames == null || registeredStateNames.isEmpty() ||
                (registeredStateNames.equals(Collections.singleton("state")))) {
            nextTransactionalIdHintState = context.getOperatorStateStore().getUnionListState(NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR_V2);
            String actualTransactionalIdPrefix;
            if (this.transactionalIdPrefix != null) {
                actualTransactionalIdPrefix = this.transactionalIdPrefix;
            } else {
                actualTransactionalIdPrefix = getActualTransactionalIdPrefix();
            }
            initializeTransactionState(actualTransactionalIdPrefix);
            super.initializeState(context);
            return;
        }

        if (pscConfigurationInternal == null) {
            pscConfigurationInternal = PscConfigurationUtils.propertiesToPscConfigurationInternal(producerConfig, PscConfiguration.PSC_CLIENT_TYPE_PRODUCER);
        }

        if (context.getOperatorStateStore()
                   .getRegisteredStateNames()
                   .contains(NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR_V2.getName())) {
            // psc checkpoint
            LOG.info("Detected a flink-psc checkpoint.");
            try {
                nextTransactionalIdHintState = context.getOperatorStateStore().getUnionListState(NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR_V2);
                String actualTransactionalIdPrefix;
                if (this.transactionalIdPrefix != null) {
                    actualTransactionalIdPrefix = this.transactionalIdPrefix;
                } else {
                    actualTransactionalIdPrefix = getActualTransactionalIdPrefix();
                }
                initializeTransactionState(actualTransactionalIdPrefix);
                super.initializeState(context);
                LOG.info("Producer subtask {} restored state from a flink-psc state.", getRuntimeContext().getIndexOfThisSubtask());
                PscMetricRegistryManager.getInstance().incrementCounterMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SINK_STATE_RECOVERY_PSC_SUCCESS, pscConfigurationInternal
                );
                PscMetricRegistryManager.getInstance().updateHistogramMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SINK_STATE_RECOVERY_PSC_TRANSACTIONS, 1, pscConfigurationInternal
                );
            } catch (Exception e) {
                LOG.error("Producer subtask {} failed to restore from a PSC state.", getRuntimeContext().getIndexOfThisSubtask());
                PscMetricRegistryManager.getInstance().incrementCounterMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SINK_STATE_RECOVERY_PSC_FAILURE, pscConfigurationInternal
                );
                throw e;
            }
        } else if (context.getOperatorStateStore()
                          .getRegisteredStateNames()
                          .contains(((ListStateDescriptor<FlinkKafkaProducer.NextTransactionalIdHint>)
                                          PscCommon.getField(FlinkKafkaProducer.class, "NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR_V2"))
                                            .getName()) ||
                context.getOperatorStateStore()
                       .getRegisteredStateNames()
                       .contains(((ListStateDescriptor<FlinkKafkaProducer.NextTransactionalIdHint>)
                               PscCommon.getField(FlinkKafkaProducer.class, "NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR"))
                                         .getName())) {
            LOG.info("Detected a flink-kafka checkpoint.");
            try {
                ListState<FlinkKafkaProducer.NextTransactionalIdHint> nextKafkaTransactionalIdHintState =
                        context.getOperatorStateStore().getUnionListState((ListStateDescriptor<FlinkKafkaProducer.NextTransactionalIdHint>) PscCommon.getField(FlinkKafkaProducer.class, "NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR_V2"));
                ListState<FlinkKafkaProducer.NextTransactionalIdHint> oldNextTransactionalIdHintState =
                        context.getOperatorStateStore().getUnionListState((ListStateDescriptor<FlinkKafkaProducer.NextTransactionalIdHint>) PscCommon.getField(FlinkKafkaProducer.class, "NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR"));

                ArrayList<FlinkKafkaProducer.NextTransactionalIdHint> oldTransactionalIdHints =
                        Lists.newArrayList(oldNextTransactionalIdHintState.get());
                if (!oldTransactionalIdHints.isEmpty()) {
                    nextKafkaTransactionalIdHintState.addAll(oldTransactionalIdHints);
                    //clear old state
                    oldNextTransactionalIdHintState.clear();
                }

                PscMetricRegistryManager.getInstance().updateHistogramMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SINK_STATE_RECOVERY_KAFKA_TRANSACTIONS, getSize(nextKafkaTransactionalIdHintState), pscConfigurationInternal
                );

                nextTransactionalIdHintState = context.getOperatorStateStore().getUnionListState(NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR_V2);
                for (FlinkKafkaProducer.NextTransactionalIdHint nextKafkaTransactionalIdHint : nextKafkaTransactionalIdHintState.get()) {
                    nextTransactionalIdHintState.add(new NextTransactionalIdHint(
                            nextKafkaTransactionalIdHint.lastParallelism,
                            nextKafkaTransactionalIdHint.nextFreeTransactionalId
                    ));
                }
                String actualTransactionalIdPrefix;
                if (this.transactionalIdPrefix != null) {
                    actualTransactionalIdPrefix = this.transactionalIdPrefix;
                } else {
                    actualTransactionalIdPrefix = getActualTransactionalIdPrefix();
                }
                initializeTransactionState(actualTransactionalIdPrefix);
                supersInitializeState(context);

                LOG.info("Producer subtask {} restored state from a flink-kafka state.", getRuntimeContext().getIndexOfThisSubtask());
                PscMetricRegistryManager.getInstance().incrementCounterMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SINK_STATE_RECOVERY_KAFKA_SUCCESS, pscConfigurationInternal
                );
                PscMetricRegistryManager.getInstance().updateHistogramMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SINK_STATE_RECOVERY_PSC_TRANSACTIONS, getSize(nextTransactionalIdHintState), pscConfigurationInternal
                );
            } catch (Exception e) {
                LOG.info("Producer subtask {} failed to restore state from a flink-kafka state.", getRuntimeContext().getIndexOfThisSubtask());
                PscMetricRegistryManager.getInstance().incrementCounterMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SINK_STATE_RECOVERY_KAFKA_FAILURE, pscConfigurationInternal
                );
                throw e;
            }
        } else {
            // fresh state
            LOG.info("Producer subtask {} has no restored state.", getRuntimeContext().getIndexOfThisSubtask());
            PscMetricRegistryManager.getInstance().incrementCounterMetric(
                    null, FlinkPscStateRecoveryMetricConstants.PSC_SINK_STATE_FRESH, pscConfigurationInternal
            );
            PscMetricRegistryManager.getInstance().updateHistogramMetric(
                    null, FlinkPscStateRecoveryMetricConstants.PSC_SINK_STATE_RECOVERY_KAFKA_TRANSACTIONS, 0, pscConfigurationInternal
            );
            PscMetricRegistryManager.getInstance().updateHistogramMetric(
                    null, FlinkPscStateRecoveryMetricConstants.PSC_SINK_STATE_RECOVERY_PSC_TRANSACTIONS, 0, pscConfigurationInternal
            );
        }
    }

    private void initializeTransactionState(String actualTransactionalIdPrefix) throws Exception {
        transactionalIdsGenerator =
                new TransactionalIdsGenerator(
                        actualTransactionalIdPrefix,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getNumberOfParallelSubtasks(),
                        pscProducersPoolSize,
                        SAFE_SCALE_DOWN_FACTOR);

        if (semantic != Semantic.EXACTLY_ONCE) {
            nextTransactionalIdHint = null;
        } else {
            ArrayList<NextTransactionalIdHint> transactionalIdHints =
                    Lists.newArrayList(nextTransactionalIdHintState.get());
            if (transactionalIdHints.size() > 1) {
                throw new IllegalStateException(
                        "There should be at most one next transactional id hint written by the first subtask");
            } else if (transactionalIdHints.size() == 0) {
                nextTransactionalIdHint = new NextTransactionalIdHint(0, 0);

                // this means that this is either:
                // (1) the first execution of this application
                // (2) previous execution has failed before first checkpoint completed
                //
                // in case of (2) we have to abort all previous transactions
                abortTransactions(transactionalIdsGenerator.generateIdsToAbort());
            } else {
                nextTransactionalIdHint = transactionalIdHints.get(0);
            }
        }
    }

    private String getActualTransactionalIdPrefix() {
        String actualTransactionalIdPrefix;
        String taskName = getRuntimeContext().getTaskName();
        // Kafka transactional IDs are limited in length to be less than the max value of
        // a short, so we truncate here if necessary to a more reasonable length string.
        if (taskName.length() > maxTaskNameSize) {
            taskName = taskName.substring(0, maxTaskNameSize);
            LOG.warn(
                    "Truncated task name for Kafka TransactionalId from {} to {}.",
                    getRuntimeContext().getTaskName(),
                    taskName);
        }
        actualTransactionalIdPrefix =
                taskName
                        + "-"
                        + ((StreamingRuntimeContext) getRuntimeContext()).getOperatorUniqueID();
        return actualTransactionalIdPrefix;
    }

    private long getSize(ListState<?> transactionalIdHintState) throws Exception {
        AtomicLong size = new AtomicLong(0);
        transactionalIdHintState.get().iterator().forEachRemaining(state -> size.incrementAndGet());
        return size.get();
    }

    private void supersInitializeState(FunctionInitializationContext context) throws Exception {
        Field field = getClass().getSuperclass().getDeclaredField("stateDescriptor");
        field.setAccessible(true);
        ListStateDescriptor<State<PscTransactionState, PscTransactionContext>> stateDescriptor =
                (ListStateDescriptor<State<PscTransactionState, PscTransactionContext>>) field.get(this);
        state = context.getOperatorStateStore().getListState(stateDescriptor);

        boolean recoveredUserContext = false;
        if (context.isRestored()) {
            Method method = getClass().getSuperclass().getDeclaredMethod("name");
            method.setAccessible(true);
            LOG.info("{} - restoring state", method.invoke(this));
            for (State<PscTransactionState, FlinkPscProducer.PscTransactionContext> operatorState : state.get()) {
                LOG.info("Operator state: {}", operatorState);
                userContext = operatorState.getContext();
                List<TransactionHolder<FlinkPscProducer.PscTransactionState>> pendingTransactions = operatorState.getPendingCommitTransactions();
                List<TransactionHolder<FlinkPscProducer.PscTransactionState>> recoveredTransactions = new ArrayList<>();
                for (TransactionHolder<FlinkPscProducer.PscTransactionState> pendingTransaction : pendingTransactions) {
                    // convert from Kafka to PSC transaction state
                    Object handle = PscCommon.getField(pendingTransaction, "handle");
                    if (handle.getClass().getName().endsWith("FlinkKafkaProducer$KafkaTransactionState")) {
                        FlinkKafkaProducer.KafkaTransactionState kafkaTransactionState = (FlinkKafkaProducer.KafkaTransactionState) handle;
                        LOG.info("Kafka transaction state (pending transactions): {}", kafkaTransactionState);
                        PscTransactionState pscTransactionState = new PscTransactionState(
                                (String) PscCommon.getField(kafkaTransactionState, "transactionalId"),
                                (long) PscCommon.getField(kafkaTransactionState, "producerId"),
                                (short) PscCommon.getField(kafkaTransactionState, "epoch"),
                                null
                        );
                        LOG.info("PSC transaction state (pending transactions): {}", pscTransactionState);
                        recoveredTransactions.add(
                                new TransactionHolder<>(
                                        pscTransactionState,
                                        (long) PscCommon.getField(pendingTransaction, "transactionStartTime")
                                )
                        );
                    }
                }


                List<FlinkPscProducer.PscTransactionState> handledTransactions = new ArrayList<>(recoveredTransactions.size() + 1);
                for (TransactionHolder<FlinkPscProducer.PscTransactionState> recoveredTransaction : recoveredTransactions) {
                    // If this fails to succeed eventually, there is actually data loss
                    method = getClass().getSuperclass().getDeclaredMethod("recoverAndCommitInternal", TwoPhaseCommitSinkFunction.TransactionHolder.class);
                    method.setAccessible(true);
                    method.invoke(this, recoveredTransaction);
                    handledTransactions.add((FlinkPscProducer.PscTransactionState) PscCommon.getField(recoveredTransaction, "handle"));
                    method = getClass().getSuperclass().getDeclaredMethod("name");
                    method.setAccessible(true);
                    LOG.info("{} committed recovered transaction {}", method.invoke(this), recoveredTransaction);
                }


                {
                    TransactionHolder<FlinkPscProducer.PscTransactionState> pendingTransaction = operatorState.getPendingTransaction();
                    FlinkPscProducer.PscTransactionState pscTransactionState = null;
                    Object handle = PscCommon.getField(pendingTransaction, "handle");
                    if (handle.getClass().getName().endsWith("FlinkKafkaProducer$KafkaTransactionState")) {
                        FlinkKafkaProducer.KafkaTransactionState kafkaTransactionState = (FlinkKafkaProducer.KafkaTransactionState) handle;
                        LOG.info("Kafka transaction state: {}", kafkaTransactionState);
                        pscTransactionState = new PscTransactionState(
                                (String) PscCommon.getField(kafkaTransactionState, "transactionalId"),
                                (long) PscCommon.getField(kafkaTransactionState, "producerId"),
                                (short) PscCommon.getField(kafkaTransactionState, "epoch"),
                                null
                        );
                        LOG.info("PSC transaction state: {}", pscTransactionState);
                    }

                    if (pscTransactionState != null) {
                        TransactionHolder<FlinkPscProducer.PscTransactionState> transactionHolder = new TransactionHolder<>(
                                pscTransactionState,
                                (long) PscCommon.getField(pendingTransaction, "transactionStartTime")
                        );

                        recoverAndAbort(pscTransactionState);
                        handledTransactions.add(pscTransactionState);
                        method = getClass().getSuperclass().getDeclaredMethod("name");
                        method.setAccessible(true);
                        LOG.info("{} aborted recovered transaction {}", method.invoke(this), transactionHolder);
                    }
                }

                Optional operatorStateContextOptional = operatorState.getContext();
                if (operatorStateContextOptional != null && operatorStateContextOptional.isPresent()) {
                    LOG.info("operatorStateContextOptional is present: {}", operatorStateContextOptional);
                    String contextClassName = operatorStateContextOptional.get().getClass().getName();
                    if (contextClassName.endsWith("kafka.FlinkKafkaProducer$KafkaTransactionContext")) {
                        FlinkKafkaProducer.KafkaTransactionContext kafkaTransactionContext =
                                (FlinkKafkaProducer.KafkaTransactionContext) operatorStateContextOptional.get();
                        LOG.info("Kafka transaction context: {}", kafkaTransactionContext);
                        FlinkPscProducer.PscTransactionContext pscTransactionContext = new FlinkPscProducer.PscTransactionContext(
                                (Set<String>) PscCommon.getField(kafkaTransactionContext, "transactionalIds")
                        );
                        LOG.info("PSC transaction context: {}", pscTransactionContext);
                        userContext = Optional.of(pscTransactionContext);
                    }
                }

                if (userContext == null) { // default - PscTransactionContext type
                    userContext = operatorState.getContext();
                }

                if (userContext.isPresent()) {
                    finishRecoveringContext(handledTransactions);
                    recoveredUserContext = true;
                }
            }
        }

        // if in restore we didn't get any userContext or we are initializing from scratch
        if (!recoveredUserContext) {
            Method method = getClass().getSuperclass().getDeclaredMethod("name");
            method.setAccessible(true);
            LOG.info("{} - no state to restore", method.invoke(this));

            userContext = initializeUserContext();
        }
        this.pendingCommitTransactions.clear();

        Method method = getClass().getSuperclass().getDeclaredMethod("beginTransactionInternal");
        method.setAccessible(true);

        field = getClass().getSuperclass().getDeclaredField("currentTransactionHolder");
        field.setAccessible(true);
        field.set(this, method.invoke(this));

        method = getClass().getSuperclass().getDeclaredMethod("name");
        method.setAccessible(true);
        LOG.debug("{} - started new transaction '{}'", method.invoke(this), field.get(this));
    }

    @Override
    protected Optional<PscTransactionContext> initializeUserContext() {
        if (semantic != FlinkPscProducer.Semantic.EXACTLY_ONCE) {
            return Optional.empty();
        }

        Set<String> transactionalIds = generateNewTransactionalIds();
        resetAvailableTransactionalIdsPool(transactionalIds);
        return Optional.of(new PscTransactionContext(transactionalIds));
    }

    private Set<String> generateNewTransactionalIds() {
        checkState(nextTransactionalIdHint != null, "nextTransactionalIdHint must be present for EXACTLY_ONCE");

        Set<String> transactionalIds = transactionalIdsGenerator.generateIdsToUse(nextTransactionalIdHint.nextFreeTransactionalId);
        LOG.info("Generated new transactionalIds {}", transactionalIds);
        return transactionalIds;
    }

    @Override
    protected void finishRecoveringContext(Collection<FlinkPscProducer.PscTransactionState> handledTransactions) {
        try {
            cleanUpUserContext(handledTransactions);
            Set<String> transactionalIds = getTransactionalIdsFromUserContext(getUserContext());
            resetAvailableTransactionalIdsPool(transactionalIds);
            LOG.info("Recovered transactionalIds {}", transactionalIds);
        } catch (FlinkPscException flinkPscException) {
            LOG.error("Failed to recover transactionalIds from user context.", flinkPscException);
        }
    }

    private Set<String> getTransactionalIdsFromUserContext(Optional<FlinkPscProducer.PscTransactionContext> userContext) throws FlinkPscException {
        Object restoredUserContext = userContext.get();
        if (restoredUserContext instanceof FlinkPscProducer.PscTransactionContext) {
            FlinkPscProducer.PscTransactionContext pscUserContext = (FlinkPscProducer.PscTransactionContext) restoredUserContext;
            return pscUserContext.transactionalIds;
        } else if (restoredUserContext instanceof FlinkKafkaProducer.KafkaTransactionContext) {
            FlinkKafkaProducer.KafkaTransactionContext kafkaUserContext = (FlinkKafkaProducer.KafkaTransactionContext) restoredUserContext;
            return (Set<String>) PscCommon.getField(kafkaUserContext, "transactionalIds");
        } else
            throw new FlinkPscException(FlinkPscErrorCode.EXTERNAL_ERROR, "Unsupported user context: " + restoredUserContext.getClass().getName());
    }

    protected FlinkPscInternalProducer<byte[], byte[]> createProducer() throws FlinkPscException {
        try {
            return new FlinkPscInternalProducer<>(this.producerConfig);
        } catch (ProducerException | ConfigurationException e) {
            throw new FlinkPscException(
                    FlinkPscErrorCode.EXTERNAL_ERROR,
                    String.format("Failed to create a FlinkPscInternalProducer with config: %s", producerConfig),
                    e
            );
        }
    }

    /**
     * After initialization make sure that all previous transactions from the current user context have been completed.
     *
     * @param handledTransactions transactions which were already committed or aborted and do not need further handling
     */
    private void cleanUpUserContext(Collection<FlinkPscProducer.PscTransactionState> handledTransactions) throws FlinkPscException {
        if (!getUserContext().isPresent()) {
            return;
        }

        Set<String> abortTransactions = new HashSet<>();
        Set<String> transactionalIds = getTransactionalIdsFromUserContext(getUserContext());
        abortTransactions.addAll(transactionalIds);

        handledTransactions.forEach(
                pscTransactionState -> abortTransactions.remove(pscTransactionState.transactionalId));
        abortTransactions(abortTransactions);
    }

    private void resetAvailableTransactionalIdsPool(Collection<String> transactionalIds) {
        availableTransactionalIds.clear();
        availableTransactionalIds.addAll(transactionalIds);
    }

    // ----------------------------------- Utilities --------------------------

    private void abortTransactions(final Set<String> transactionalIds) {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        for (String transactionalId : transactionalIds) {
            // The parallelStream executes the consumer in a separated thread pool.
            // Because the consumer(e.g. Kafka) uses the context classloader to construct some class
            // we should set the correct classloader for it.
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
                // don't mess with the original configuration or any other properties of the
                // original object
                // -> create an internal kafka producer on our own and do not rely on
                //    initTransactionalProducer().
                final Properties myConfig = new Properties();
                myConfig.putAll(producerConfig);
                initTransactionalProducerConfig(myConfig, transactionalId);
                FlinkPscInternalProducer<byte[], byte[]> flinkPscInternalProducer = null;
                try {
                    flinkPscInternalProducer = new FlinkPscInternalProducer<>(myConfig);
                    // it suffices to call initTransactions - this will abort any lingering transactions
                    initTransactions(flinkPscInternalProducer);
                } catch (ProducerException | ConfigurationException | FlinkPscException e) { // | FlinkPscException e) {
                    LOG.error("Failed to init transaction with id {}.", transactionalId, e);
                } finally {
                    if (flinkPscInternalProducer != null) {
                        closeProducer(flinkPscInternalProducer);
                    }
                }
            }
        }
    }

    private PscProducerTransactionalProperties initTransactions(FlinkPscInternalProducer producer) throws FlinkPscException {
        try {
            return producer.initTransactions(defaultTopicUri);
        } catch (ProducerException | ConfigurationException e) {
            throw new FlinkPscException(
                    FlinkPscErrorCode.EXTERNAL_ERROR,
                    String.format("Failed to init transaction for FlinkPscInternalProducer with id %s.", producer.getClientId()),
                    e
            );
        }
    }

    private void beginTransaction(FlinkPscInternalProducer producer) throws FlinkPscException {
        try {
            producer.beginTransaction();
        } catch (ProducerException e) {
            throw new FlinkPscException(
                    FlinkPscErrorCode.EXTERNAL_ERROR,
                    String.format("Failed to begin transaction for FlinkPscInternalProducer with id %s.", producer.getClientId()),
                    e
            );
        }

    }

    private void commitTransaction(FlinkPscInternalProducer producer) throws FlinkPscException {
        try {
            producer.commitTransaction();
        } catch (ProducerException e) {
            throw new FlinkPscException(
                    FlinkPscErrorCode.EXTERNAL_ERROR,
                    String.format("Failed to commit transaction for FlinkPscInternalProducer with id %s.", producer.getClientId()),
                    e
            );
        }
    }

    private void abortTransaction(FlinkPscInternalProducer producer) throws FlinkPscException {
        try {
            producer.abortTransaction();
        } catch (ProducerException e) {
            throw new FlinkPscException(
                    FlinkPscErrorCode.EXTERNAL_ERROR,
                    String.format("Failed to abort transaction for FlinkPscInternalProducer with id %s.", producer.getClientId()),
                    e
            );
        }
    }

    Set<Integer> getTransactionCoordinatorId() throws ProducerException {
        final FlinkPscProducer.PscTransactionState currentTransaction = currentTransaction();
        if (currentTransaction == null || currentTransaction.producer == null) {
            throw new IllegalArgumentException();
        }
        return currentTransaction.producer.getTransactionCoordinatorIds();
    }

    /**
     * For each checkpoint we create new {@link FlinkPscInternalProducer} so that new transactions will not clash
     * with transactions created during previous checkpoints ({@code producer.initTransactions()} assures that we
     * obtain new producerId and epoch counters).
     */
    private FlinkPscInternalProducer<byte[], byte[]> createTransactionalProducer() throws FlinkPscException {
        String transactionalId = availableTransactionalIds.poll();
        if (transactionalId == null) {
            throw new FlinkPscException(
                    FlinkPscErrorCode.PRODUCERS_POOL_EMPTY,
                    "Too many ongoing snapshots. Increase kafka producers pool size or decrease number of concurrent checkpoints.");
        }
        FlinkPscInternalProducer<byte[], byte[]> producer = initTransactionalProducer(transactionalId, true);
        //initTransactions(producer);
        return producer;
    }

    private void recycleTransactionalProducer(FlinkPscInternalProducer<byte[], byte[]> producer) {
        availableTransactionalIds.add(producer.getTransactionalId());
        flushProducer(producer);
        closeProducer(producer);
    }

    private FlinkPscInternalProducer<byte[], byte[]> initTransactionalProducer(String transactionalId, boolean registerMetrics) throws FlinkPscException {
        initTransactionalProducerConfig(producerConfig, transactionalId);
        return initProducer(registerMetrics);
    }

    private static void initTransactionalProducerConfig(Properties producerConfig, String transactionalId) {
        producerConfig.setProperty(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID, transactionalId);
    }

    private FlinkPscInternalProducer<byte[], byte[]> initNonTransactionalProducer(boolean registerMetrics) throws FlinkPscException {
        producerConfig.remove(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID);
        return initProducer(registerMetrics);
    }

    private FlinkPscInternalProducer<byte[], byte[]> initProducer(boolean registerMetrics) throws FlinkPscException {
        FlinkPscInternalProducer<byte[], byte[]> producer = createProducer();

        LOG.info("Starting FlinkPscInternalProducer ({}/{}) to produce into default topic {}",
                getRuntimeContext().getIndexOfThisSubtask() + 1, getRuntimeContext().getNumberOfParallelSubtasks(), defaultTopicUri);
        this.registerMetrics = registerMetrics;

        return producer;
    }

    private void initializeMetrics(FlinkPscInternalProducer<byte[], byte[]> producer)
            throws FlinkPscException {
        // register Kafka metrics to Flink accumulators
        if (registerMetrics && !Boolean.parseBoolean(producerConfig.getProperty(KEY_DISABLE_METRICS, "false"))) {
            Map<MetricName, ? extends Metric> metrics;
            try {
                metrics = producer.metrics();
            } catch (ClientException exception) {
                throw new FlinkPscException(
                        FlinkPscErrorCode.EXTERNAL_ERROR,
                        exception.getMessage());
            }

            if (metrics == null) {
                // MapR's Kafka implementation returns null here.
                LOG.info("Producer implementation does not support metrics");
            } else {
                // PscProducer metric group may break metrics if migrating from Kafka (KafkaProducer metric group)
                final MetricGroup pscMetricGroup = getRuntimeContext().getMetricGroup().addGroup("PscProducer");
                for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
                    if (!(entry.getValue().metricValue() instanceof Double))
                        continue;

                    String name = entry.getKey().name();
                    Metric metric = entry.getValue();

                    PscMetricMutableWrapper wrapper = previouslyCreatedMetrics.get(name);
                    if (wrapper != null) {
                        wrapper.setPscMetric(metric);
                    } else {
                        wrapper = new PscMetricMutableWrapper(metric);
                        previouslyCreatedMetrics.put(name, wrapper);
                        pscMetricGroup.gauge(name, wrapper);
                    }
                }
            }
        }
    }

    protected void checkErroneous() throws FlinkPscException {
        Exception e = asyncException;
        if (e != null) {
            // prevent double throwing
            asyncException = null;
            throw new FlinkPscException(
                    FlinkPscErrorCode.EXTERNAL_ERROR,
                    "Failed to send data via PSC producer: " + e.getMessage(),
                    e);
        }
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
    }

    private void migrateNextTransactionalIdHindState(FunctionInitializationContext context) throws Exception {
        ListState<NextTransactionalIdHint> oldNextTransactionalIdHintState = context.getOperatorStateStore().getUnionListState(
                NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR);
        nextTransactionalIdHintState = context.getOperatorStateStore().getUnionListState(NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR_V2);

        ArrayList<NextTransactionalIdHint> oldTransactionalIdHints = Lists.newArrayList(oldNextTransactionalIdHintState.get());
        if (!oldTransactionalIdHints.isEmpty()) {
            nextTransactionalIdHintState.addAll(oldTransactionalIdHints);
            //clear old state
            oldNextTransactionalIdHintState.clear();
        }
    }

    protected static int[] getPartitionsByTopicUri(String topicUri, PscProducer<byte[], byte[]> pscProducer) {
        // the fetched list is immutable, so we're creating a mutable copy in order to sort it
        List<TopicUriPartition> partitionsList = null;
        try {
            partitionsList = new ArrayList<>(pscProducer.getPartitions(topicUri));
        } catch (ConfigurationException | ProducerException e) {
            LOG.error("Failed to get partitions of {} from PscProducer object.", topicUri, e);
            return new int[0];
        }

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

    private void flushProducer(FlinkPscInternalProducer producer) {
        try {
            producer.flush();
        } catch (ProducerException e) {
            LOG.error("Failed to flush FlinkPscInternalProducer with id {}", producer.getClientId(), e);
        }
    }

    private void closeProducer(FlinkPscInternalProducer producer) {
        try {
            producer.close(Duration.ZERO);
        } catch (IOException e) {
            LOG.warn("Failed to close FlinkPscInternalProducer with id {}", producer.getClientId(), e);
        }
    }

    /**
     * State for handling transactions.
     */
    @VisibleForTesting
    @Internal
    public static class PscTransactionState {

        private final transient FlinkPscInternalProducer<byte[], byte[]> producer;

        @Nullable
        String transactionalId;

        long producerId;

        short epoch;

        @VisibleForTesting
        public PscTransactionState(String transactionalId, FlinkPscInternalProducer<byte[], byte[]> producer) {
            this(transactionalId, -1, (short) -1, producer);
        }

        @VisibleForTesting
        public PscTransactionState(FlinkPscInternalProducer<byte[], byte[]> producer) {
            this(null, -1, (short) -1, producer);
        }

        @VisibleForTesting
        public PscTransactionState(
                @Nullable String transactionalId,
                long producerId,
                short epoch,
                FlinkPscInternalProducer<byte[], byte[]> producer) {
            this.transactionalId = transactionalId;
            this.producerId = producerId;
            this.epoch = epoch;
            this.producer = producer;
        }

        boolean isTransactional() {
            return transactionalId != null;
        }

        public boolean needsTransactionalStateCompletion() {
            return isTransactional() && producerId == -1;
        }

        public void setProducerIdAndEpoch(PscProducerMessage<byte[], byte[]> pscProducerMessage) throws ProducerException {
            if (isTransactional() && this.producerId == -1) {
                this.producerId = producer.getProducerId(pscProducerMessage);
                this.epoch = producer.getEpoch(pscProducerMessage);
            } else
                LOG.warn("Cannot reset producer id/epoch of FlinkPscProducer: {}", this);
        }

        private void setProducerTransactionalProperties(PscProducerTransactionalProperties pscProducerTransactionalProperties) {
            if (isTransactional() && this.producerId == -1) {
                this.producerId = pscProducerTransactionalProperties.getProducerId();
                this.epoch = pscProducerTransactionalProperties.getEpoch();
            } else
                LOG.warn("Cannot reset producer id/epoch of FlinkPscProducer: {}", this);
        }

        public FlinkPscInternalProducer<byte[], byte[]> getProducer() {
            return producer;
        }

        @Override
        public String toString() {
            return String.format(
                    "%s [transactionalId=%s, producerId=%s, epoch=%s]",
                    this.getClass().getSimpleName(),
                    transactionalId,
                    producerId,
                    epoch);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FlinkPscProducer.PscTransactionState that = (FlinkPscProducer.PscTransactionState) o;

            if (producerId != that.producerId) {
                return false;
            }
            if (epoch != that.epoch) {
                return false;
            }
            return transactionalId != null ? transactionalId.equals(that.transactionalId) : that.transactionalId == null;
        }

        @Override
        public int hashCode() {
            int result = transactionalId != null ? transactionalId.hashCode() : 0;
            result = 31 * result + (int) (producerId ^ (producerId >>> 32));
            result = 31 * result + (int) epoch;
            return result;
        }
    }

    /**
     * Context associated to this instance of the {@link FlinkPscProducer}. User for keeping track of the
     * transactionalIds.
     */
    @VisibleForTesting
    @Internal
    public static class PscTransactionContext {
        final Set<String> transactionalIds;

        @VisibleForTesting
        public PscTransactionContext(Set<String> transactionalIds) {
            checkNotNull(transactionalIds);
            this.transactionalIds = transactionalIds;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PscTransactionContext that = (PscTransactionContext) o;

            return transactionalIds.equals(that.transactionalIds);
        }

        @Override
        public int hashCode() {
            return transactionalIds.hashCode();
        }
    }

    /**
     * {@link org.apache.flink.api.common.typeutils.TypeSerializer} for
     * {@link FlinkPscProducer.PscTransactionState}.
     */
    @VisibleForTesting
    @Internal
    public static class TransactionStateSerializer extends TypeSerializerSingleton<FlinkPscProducer.PscTransactionState> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public FlinkPscProducer.PscTransactionState createInstance() {
            return null;
        }

        @Override
        public FlinkPscProducer.PscTransactionState copy(FlinkPscProducer.PscTransactionState from) {
            return from;
        }

        @Override
        public FlinkPscProducer.PscTransactionState copy(
                FlinkPscProducer.PscTransactionState from,
                FlinkPscProducer.PscTransactionState reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(
                FlinkPscProducer.PscTransactionState record,
                DataOutputView target) throws IOException {
            if (record.transactionalId == null) {
                target.writeBoolean(false);
            } else {
                target.writeBoolean(true);
                target.writeUTF(record.transactionalId);
            }
            target.writeLong(record.producerId);
            target.writeShort(record.epoch);
        }

        @Override
        public FlinkPscProducer.PscTransactionState deserialize(DataInputView source) throws IOException {
            String transactionalId = null;
            if (source.readBoolean()) {
                transactionalId = source.readUTF();
            }
            long producerId = source.readLong();
            short epoch = source.readShort();
            return new FlinkPscProducer.PscTransactionState(transactionalId, producerId, epoch, null);
        }

        @Override
        public FlinkPscProducer.PscTransactionState deserialize(
                FlinkPscProducer.PscTransactionState reuse,
                DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(
                DataInputView source, DataOutputView target) throws IOException {
            boolean hasTransactionalId = source.readBoolean();
            target.writeBoolean(hasTransactionalId);
            if (hasTransactionalId) {
                target.writeUTF(source.readUTF());
            }
            target.writeLong(source.readLong());
            target.writeShort(source.readShort());
        }

        // -----------------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<FlinkPscProducer.PscTransactionState> snapshotConfiguration() {
            return new TransactionStateSerializerSnapshot();
        }

        /**
         * Serializer configuration snapshot for compatibility and format evolution.
         */
        @SuppressWarnings("WeakerAccess")
        public static final class TransactionStateSerializerSnapshot extends
                PscSimpleTypeSerializerSnapshot<PscTransactionState> {

            public TransactionStateSerializerSnapshot() {
                super(TransactionStateSerializer::new);
            }
        }
    }

    /**
     * {@link org.apache.flink.api.common.typeutils.TypeSerializer} for
     * {@link PscTransactionContext}.
     */
    @VisibleForTesting
    @Internal
    public static class ContextStateSerializer extends TypeSerializerSingleton<PscTransactionContext> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public PscTransactionContext createInstance() {
            return null;
        }

        @Override
        public PscTransactionContext copy(PscTransactionContext from) {
            return from;
        }

        @Override
        public PscTransactionContext copy(
                PscTransactionContext from,
                PscTransactionContext reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(
                PscTransactionContext record,
                DataOutputView target) throws IOException {
            int numIds = record.transactionalIds.size();
            target.writeInt(numIds);
            for (String id : record.transactionalIds) {
                target.writeUTF(id);
            }
        }

        @Override
        public PscTransactionContext deserialize(DataInputView source) throws IOException {
            int numIds = source.readInt();
            Set<String> ids = new HashSet<>(numIds);
            for (int i = 0; i < numIds; i++) {
                ids.add(source.readUTF());
            }
            return new PscTransactionContext(ids);
        }

        @Override
        public PscTransactionContext deserialize(
                PscTransactionContext reuse,
                DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(
                DataInputView source,
                DataOutputView target) throws IOException {
            int numIds = source.readInt();
            target.writeInt(numIds);
            for (int i = 0; i < numIds; i++) {
                target.writeUTF(source.readUTF());
            }
        }

        // -----------------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<PscTransactionContext> snapshotConfiguration() {
            return new ContextStateSerializerSnapshot();
        }

        /**
         * Serializer configuration snapshot for compatibility and format evolution.
         */
        @SuppressWarnings("WeakerAccess")
        public static final class ContextStateSerializerSnapshot extends PscSimpleTypeSerializerSnapshot<PscTransactionContext> {

            public ContextStateSerializerSnapshot() {
                super(ContextStateSerializer::new);
            }
        }
    }

    /**
     * Keep information required to deduce next safe to use transactional id.
     */
    public static class NextTransactionalIdHint {
        public int lastParallelism = 0;
        public long nextFreeTransactionalId = 0;

        public NextTransactionalIdHint() {
            this(0, 0);
        }

        public NextTransactionalIdHint(int parallelism, long nextFreeTransactionalId) {
            this.lastParallelism = parallelism;
            this.nextFreeTransactionalId = nextFreeTransactionalId;
        }

        @Override
        public String toString() {
            return "NextTransactionalIdHint[" +
                    "lastParallelism=" + lastParallelism +
                    ", nextFreeTransactionalId=" + nextFreeTransactionalId +
                    ']';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            NextTransactionalIdHint that = (NextTransactionalIdHint) o;

            if (lastParallelism != that.lastParallelism) {
                return false;
            }
            return nextFreeTransactionalId == that.nextFreeTransactionalId;
        }

        @Override
        public int hashCode() {
            int result = lastParallelism;
            result = 31 * result + (int) (nextFreeTransactionalId ^ (nextFreeTransactionalId >>> 32));
            return result;
        }
    }

    /**
     * {@link org.apache.flink.api.common.typeutils.TypeSerializer} for
     * {@link FlinkPscProducer.NextTransactionalIdHint}.
     */
    @VisibleForTesting
    @Internal
    public static class NextTransactionalIdHintSerializer extends TypeSerializerSingleton<NextTransactionalIdHint> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public NextTransactionalIdHint createInstance() {
            return new NextTransactionalIdHint();
        }

        @Override
        public NextTransactionalIdHint copy(NextTransactionalIdHint from) {
            return from;
        }

        @Override
        public NextTransactionalIdHint copy(NextTransactionalIdHint from, NextTransactionalIdHint reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return Long.BYTES + Integer.BYTES;
        }

        @Override
        public void serialize(NextTransactionalIdHint record, DataOutputView target) throws IOException {
            target.writeLong(record.nextFreeTransactionalId);
            target.writeInt(record.lastParallelism);
        }

        @Override
        public NextTransactionalIdHint deserialize(DataInputView source) throws IOException {
            long nextFreeTransactionalId = source.readLong();
            int lastParallelism = source.readInt();
            return new NextTransactionalIdHint(lastParallelism, nextFreeTransactionalId);
        }

        @Override
        public NextTransactionalIdHint deserialize(NextTransactionalIdHint reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
            target.writeInt(source.readInt());
        }

        @Override
        public TypeSerializerSnapshot<NextTransactionalIdHint> snapshotConfiguration() {
            return new NextTransactionalIdHintSerializerSnapshot();
        }

        /**
         * Serializer configuration snapshot for compatibility and format evolution.
         */
        @SuppressWarnings("WeakerAccess")
        public static final class NextTransactionalIdHintSerializerSnapshot extends PscSimpleTypeSerializerSnapshot<NextTransactionalIdHint> {

            public NextTransactionalIdHintSerializerSnapshot() {
                super(NextTransactionalIdHintSerializer::new);
            }
        }
    }
}
