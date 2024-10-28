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
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.ByteArraySerializer;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.UserCodeClassLoader;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.pinterest.flink.connector.psc.testutils.PscTestUtils.injectDiscoveryConfigs;
import static com.pinterest.flink.connector.psc.testutils.PscUtil.createKafkaContainer;
import static com.pinterest.flink.connector.psc.testutils.PscUtil.drainAllRecordsFromTopic;
import static org.apache.flink.util.DockerImageVersions.KAFKA;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the standalone PscWriter. */
@ExtendWith(TestLoggerExtension.class)
public class PscWriterITCase {

    private static final Logger LOG = LoggerFactory.getLogger(PscWriterITCase.class);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();
    private static final String PSC_METRIC_WITH_GROUP_NAME = "PscProducer.incoming-byte-total";
    private static final SinkWriter.Context SINK_WRITER_CONTEXT = new DummySinkWriterContext();
    private String topic;
    private String topicUriStr;

    private MetricListener metricListener;
    private TriggerTimeService timeService;

    private static final KafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(KAFKA, LOG)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @BeforeAll
    public static void beforeAll() {
        KAFKA_CONTAINER.start();
    }

    @AfterAll
    public static void afterAll() {
        KAFKA_CONTAINER.stop();
    }

    @BeforeEach
    public void setUp(TestInfo testInfo) {
        metricListener = new MetricListener();
        timeService = new TriggerTimeService();
        topic = testInfo.getDisplayName().replaceAll("\\W", "");
        topicUriStr = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + topic;
    }

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    public void testRegisterMetrics(DeliveryGuarantee guarantee) throws Exception {
        try (final PscWriter<Integer> ignored =
                createWriterWithConfiguration(getPscClientConfiguration(), guarantee)) {
            ignored.write(1, SINK_WRITER_CONTEXT);  // write one record to trigger backendProducer creation
            assertThat(metricListener.getGauge(PSC_METRIC_WITH_GROUP_NAME).isPresent()).isTrue();
        }
    }

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    public void testNotRegisterMetrics(DeliveryGuarantee guarantee) throws Exception {
        assertPscMetricNotPresent(guarantee, "flink.disable-metrics", "true");
        assertPscMetricNotPresent(guarantee, "register.producer.metrics", "false");
    }

    @Test
    public void testIncreasingRecordBasedCounters() throws Exception {
        final OperatorIOMetricGroup operatorIOMetricGroup =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup().getIOMetricGroup();
        final InternalSinkWriterMetricGroup metricGroup =
                InternalSinkWriterMetricGroup.mock(
                        metricListener.getMetricGroup(), operatorIOMetricGroup);
        try (final PscWriter<Integer> writer =
                createWriterWithConfiguration(
                        getPscClientConfiguration(), DeliveryGuarantee.NONE, metricGroup)) {
            final Counter numBytesOut = metricGroup.getIOMetricGroup().getNumBytesOutCounter();
            final Counter numRecordsOut = metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
            final Counter numRecordsOutErrors = metricGroup.getNumRecordsOutErrorsCounter();
            final Counter numRecordsSendErrors = metricGroup.getNumRecordsSendErrorsCounter();
            assertThat(numBytesOut.getCount()).isEqualTo(0L);
            assertThat(numRecordsOut.getCount()).isEqualTo(0);
            assertThat(numRecordsOutErrors.getCount()).isEqualTo(0);
            assertThat(numRecordsSendErrors.getCount()).isEqualTo(0);

            writer.write(1, SINK_WRITER_CONTEXT);
            timeService.trigger();
            assertThat(numRecordsOut.getCount()).isEqualTo(1);
            assertThat(numRecordsOutErrors.getCount()).isEqualTo(0);
            assertThat(numRecordsSendErrors.getCount()).isEqualTo(0);
            assertThat(numBytesOut.getCount()).isGreaterThan(0L);
        }
    }

    @Test
    public void testCurrentSendTimeMetric() throws Exception {
        final InternalSinkWriterMetricGroup metricGroup =
                InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup());
        try (final PscWriter<Integer> writer =
                createWriterWithConfiguration(
                        getPscClientConfiguration(),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        metricGroup)) {
            IntStream.range(0, 100)
                    .forEach(
                            (run) -> {
                                try {
                                    writer.write(1, SINK_WRITER_CONTEXT);
                                    // Manually flush the records to generate a sendTime
                                    if (run % 10 == 0) {
                                        writer.flush(false);
                                    }
                                } catch (IOException | InterruptedException e) {
                                    throw new RuntimeException("Failed writing PSC record.");
                                }
                            });
            Thread.sleep(500L);
            final Optional<Gauge<Long>> currentSendTime =
                    metricListener.getGauge("currentSendTime");
            assertThat(currentSendTime.isPresent()).isTrue();
            assertThat(currentSendTime.get().getValue()).isGreaterThan(0L);
        }
    }

    @Test
    void testNumRecordsOutErrorsCounterMetric() throws Exception {
        Properties properties = getPscClientConfiguration();
        final InternalSinkWriterMetricGroup metricGroup =
                InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup());

        try (final PscWriter<Integer> writer =
                createWriterWithConfiguration(
                        properties, DeliveryGuarantee.EXACTLY_ONCE, metricGroup)) {
            final Counter numRecordsOutErrors = metricGroup.getNumRecordsOutErrorsCounter();
            assertThat(numRecordsOutErrors.getCount()).isEqualTo(0L);

            writer.write(1, SINK_WRITER_CONTEXT);
            assertThat(numRecordsOutErrors.getCount()).isEqualTo(0L);

            final String transactionalId = writer.getCurrentProducer().getTransactionalId();

            try (FlinkPscInternalProducer<byte[], byte[]> producer =
                    new FlinkPscInternalProducer<>(properties, transactionalId)) {

                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new PscProducerMessage<>(topicUriStr, "2".getBytes()));
                producer.commitTransaction();
            }

            writer.write(3, SINK_WRITER_CONTEXT);
            writer.flush(false);
            writer.prepareCommit();
            assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);
        }
    }

    @Test
    public void testMetadataPublisher() throws Exception {
        List<String> metadataList = new ArrayList<>();
        try (final PscWriter<Integer> writer =
                createWriterWithConfiguration(
                        getPscClientConfiguration(),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup()),
                        meta -> metadataList.add(meta.getTopicUriPartition().getTopicUriAsString() + "-" + meta.getTopicUriPartition().getPartition() + "@" + meta.getOffset()))) {
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                writer.write(1, SINK_WRITER_CONTEXT);
                expected.add(topicUriStr + "-0@" + i);
            }
            writer.flush(false);
            assertThat(metadataList).usingRecursiveComparison().isEqualTo(expected);
        }
    }

    /** Test that producer is not accidentally recreated or pool is used. */
    @Test
    void testLingeringTransaction() throws Exception {
        final PscWriter<Integer> failedWriter =
                createWriterWithConfiguration(
                        getPscClientConfiguration(), DeliveryGuarantee.EXACTLY_ONCE);

        // create two lingering transactions
        failedWriter.flush(false);
        failedWriter.prepareCommit();
        failedWriter.snapshotState(1);
        failedWriter.flush(false);
        failedWriter.prepareCommit();
        failedWriter.snapshotState(2);

        try (final PscWriter<Integer> recoveredWriter =
                createWriterWithConfiguration(
                        getPscClientConfiguration(), DeliveryGuarantee.EXACTLY_ONCE)) {
            recoveredWriter.write(1, SINK_WRITER_CONTEXT);

            recoveredWriter.flush(false);
            Collection<PscCommittable> committables = recoveredWriter.prepareCommit();
            recoveredWriter.snapshotState(1);
            assertThat(committables).hasSize(1);
            final PscCommittable committable = committables.stream().findFirst().get();
            assertThat(committable.getProducer().isPresent()).isTrue();

            committable.getProducer().get().getObject().commitTransaction();

            List<PscConsumerMessage<byte[], byte[]>> records =
                    drainAllRecordsFromTopic(topicUriStr, getPscClientConfiguration(), true);
            assertThat(records).hasSize(1);
        }

        failedWriter.close();
    }

    /** Test that producer is not accidentally recreated or pool is used. */
    @ParameterizedTest
    @EnumSource(
            value = DeliveryGuarantee.class,
            names = "EXACTLY_ONCE",
            mode = EnumSource.Mode.EXCLUDE)
    void useSameProducerForNonTransactional(DeliveryGuarantee guarantee) throws Exception {
        try (final PscWriter<Integer> writer =
                createWriterWithConfiguration(getPscClientConfiguration(), guarantee)) {
            assertThat(writer.getProducerPool()).hasSize(0);

            FlinkPscInternalProducer<byte[], byte[]> firstProducer = writer.getCurrentProducer();
            writer.flush(false);
            Collection<PscCommittable> committables = writer.prepareCommit();
            writer.snapshotState(0);
            assertThat(committables).hasSize(0);

            assertThat(writer.getCurrentProducer() == firstProducer)
                    .as("Expected same producer")
                    .isTrue();
            assertThat(writer.getProducerPool()).hasSize(0);
        }
    }

    /** Test that producers are reused when committed. */
    @Test
    void usePoolForTransactional() throws Exception {
        try (final PscWriter<Integer> writer =
                createWriterWithConfiguration(
                        getPscClientConfiguration(), DeliveryGuarantee.EXACTLY_ONCE)) {
            assertThat(writer.getProducerPool()).hasSize(0);

            writer.flush(false);
            Collection<PscCommittable> committables0 = writer.prepareCommit();
            writer.snapshotState(1);
            assertThat(committables0).hasSize(1);
            final PscCommittable committable = committables0.stream().findFirst().get();
            assertThat(committable.getProducer().isPresent()).isTrue();

            FlinkPscInternalProducer<?, ?> firstProducer =
                    committable.getProducer().get().getObject();
            assertThat(firstProducer != writer.getCurrentProducer())
                    .as("Expected different producer")
                    .isTrue();

            // recycle first producer, PscCommitter would commit it and then return it
            assertThat(writer.getProducerPool()).hasSize(0);
            firstProducer.commitTransaction();
            committable.getProducer().get().close();
            assertThat(writer.getProducerPool()).hasSize(1);

            writer.flush(false);
            Collection<PscCommittable> committables1 = writer.prepareCommit();
            writer.snapshotState(2);
            assertThat(committables1).hasSize(1);
            final PscCommittable committable1 = committables1.stream().findFirst().get();
            assertThat(committable1.getProducer().isPresent()).isTrue();

            assertThat(firstProducer == writer.getCurrentProducer())
                    .as("Expected recycled producer")
                    .isTrue();
        }
    }

    /**
     * Tests that open transactions are automatically aborted on close such that successive writes
     * succeed.
     */
    @Test
    void testAbortOnClose() throws Exception {
        Properties properties = getPscClientConfiguration();
        try (final PscWriter<Integer> writer =
                createWriterWithConfiguration(properties, DeliveryGuarantee.EXACTLY_ONCE)) {
            writer.write(1, SINK_WRITER_CONTEXT);
            assertThat(drainAllRecordsFromTopic(topicUriStr, properties, true)).hasSize(0);
        }

        try (final PscWriter<Integer> writer =
                createWriterWithConfiguration(properties, DeliveryGuarantee.EXACTLY_ONCE)) {
            writer.write(2, SINK_WRITER_CONTEXT);
            writer.flush(false);
            Collection<PscCommittable> committables = writer.prepareCommit();
            writer.snapshotState(1L);

            // manually commit here, which would only succeed if the first transaction was aborted
            assertThat(committables).hasSize(1);
            final PscCommittable committable = committables.stream().findFirst().get();
            String transactionalId = committable.getTransactionalId();
            try (FlinkPscInternalProducer<byte[], byte[]> producer =
                    new FlinkPscInternalProducer<>(properties, transactionalId)) {
                producer.resumeTransaction(committable.getProducerId(), committable.getEpoch());
                producer.commitTransaction();
            }

            assertThat(drainAllRecordsFromTopic(topicUriStr, properties, true)).hasSize(1);
        }
    }

    private void assertPscMetricNotPresent(
            DeliveryGuarantee guarantee, String configKey, String configValue) throws Exception {
        final Properties config = getPscClientConfiguration();
        config.put(configKey, configValue);
        try (final PscWriter<Integer> ignored =
                createWriterWithConfiguration(config, guarantee)) {
            Assertions.assertFalse(
                    metricListener.getGauge(PSC_METRIC_WITH_GROUP_NAME).isPresent());
        }
    }

    private PscWriter<Integer> createWriterWithConfiguration(
            Properties config, DeliveryGuarantee guarantee) {
        return createWriterWithConfiguration(
                config,
                guarantee,
                InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup()));
    }

    private PscWriter<Integer> createWriterWithConfiguration(
            Properties config,
            DeliveryGuarantee guarantee,
            SinkWriterMetricGroup sinkWriterMetricGroup) {
        return createWriterWithConfiguration(config, guarantee, sinkWriterMetricGroup, null);
    }

    private PscWriter<Integer> createWriterWithConfiguration(
            Properties config,
            DeliveryGuarantee guarantee,
            SinkWriterMetricGroup sinkWriterMetricGroup,
            @Nullable Consumer<MessageId> metadataConsumer) {
        try {
            return new PscWriter<>(
                    guarantee,
                    config,
                    "test-prefix",
                    new SinkInitContext(sinkWriterMetricGroup, timeService, metadataConsumer),
                    new DummyRecordSerializer(),
                    new DummySchemaContext(),
                    ImmutableList.of());
        } catch (ConfigurationException | ClientException | TopicUriSyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static Properties getPscClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put(PscConfiguration.PSC_CONSUMER_GROUP_ID, "pscWriter-tests");
        standardProps.put(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, false);
        standardProps.put(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, ByteArraySerializer.class.getName());
        standardProps.put(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, ByteArraySerializer.class.getName());
        standardProps.put(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "pscWriter-tests");
        standardProps.put(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, false);
        standardProps.put(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        standardProps.put(PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX);
        injectDiscoveryConfigs(standardProps, KAFKA_CONTAINER.getBootstrapServers(), PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX);
        return standardProps;
    }

    private static class SinkInitContext implements Sink.InitContext {

        private final SinkWriterMetricGroup metricGroup;
        private final ProcessingTimeService timeService;
        @Nullable private final Consumer<MessageId> metadataConsumer;

        SinkInitContext(
                SinkWriterMetricGroup metricGroup,
                ProcessingTimeService timeService,
                @Nullable Consumer<MessageId> metadataConsumer) {
            this.metricGroup = metricGroup;
            this.timeService = timeService;
            this.metadataConsumer = metadataConsumer;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return new SyncMailboxExecutor();
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return timeService;
        }

        @Override
        public int getSubtaskId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 1;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return metricGroup;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return null;
        }

        @Override
        public <MetaT> Optional<Consumer<MetaT>> metadataConsumer() {
            return Optional.ofNullable((Consumer<MetaT>) metadataConsumer);
        }
    }

    private class DummyRecordSerializer implements PscRecordSerializationSchema<Integer> {
        @Override
        public PscProducerMessage<byte[], byte[]> serialize(
                Integer element, PscSinkContext context, Long timestamp) {
            return new PscProducerMessage<>(topicUriStr, ByteBuffer.allocate(4).putInt(element).array());
        }
    }

    private static class DummySchemaContext implements SerializationSchema.InitializationContext {

        @Override
        public MetricGroup getMetricGroup() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }

    private static class DummySinkWriterContext implements SinkWriter.Context {
        @Override
        public long currentWatermark() {
            return 0;
        }

        @Override
        public Long timestamp() {
            return null;
        }
    }

    private static class TriggerTimeService implements ProcessingTimeService {

        private final PriorityQueue<Tuple2<Long, ProcessingTimeCallback>> registeredCallbacks =
                new PriorityQueue<>(Comparator.comparingLong(o -> o.f0));

        @Override
        public long getCurrentProcessingTime() {
            return 0;
        }

        @Override
        public ScheduledFuture<?> registerTimer(
                long time, ProcessingTimeCallback processingTimerCallback) {
            registeredCallbacks.add(new Tuple2<>(time, processingTimerCallback));
            return null;
        }

        public void trigger() throws Exception {
            final Tuple2<Long, ProcessingTimeCallback> registered = registeredCallbacks.poll();
            if (registered == null) {
                LOG.warn("Triggered time service but no callback was registered.");
                return;
            }
            registered.f1.onProcessingTime(registered.f0);
        }
    }
}
