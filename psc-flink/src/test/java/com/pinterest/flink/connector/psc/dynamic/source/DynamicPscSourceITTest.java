/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.connector.psc.dynamic.source;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.pinterest.flink.connector.psc.dynamic.metadata.ClusterMetadata;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscMetadataService;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import com.pinterest.flink.connector.psc.dynamic.metadata.SingleClusterTopicUriMetadataService;
import com.pinterest.flink.connector.psc.source.PscSourceOptions;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import com.pinterest.flink.connector.psc.testutils.DynamicPscSourceExternalContextFactory;
import com.pinterest.flink.connector.psc.testutils.MockPscMetadataService;
import com.pinterest.flink.connector.psc.testutils.TwoKafkaContainers;
import com.pinterest.flink.connector.psc.testutils.YamlFileMetadataService;
import com.pinterest.flink.streaming.connectors.psc.DynamicPscSourceTestHelperWithKafkaAsPubSub;
import com.pinterest.flink.streaming.connectors.psc.PscTestBaseWithKafkaAsPubSub;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.serde.IntegerDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.external.DefaultContainerizedExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.pinterest.flink.connector.psc.dynamic.source.metrics.PscClusterMetricGroup.DYNAMIC_PSC_SOURCE_METRIC_GROUP;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link DynamicPscSource}.
 */
public class DynamicPscSourceITTest extends TestLogger {

    private static final String TOPIC = "DynamicPscSourceITTest";
    private static final String TOPIC_URI = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + TOPIC;
    private static final int NUM_PARTITIONS = 3;
    private static final int NUM_RECORDS_PER_SPLIT = 5;

    private static PscTestBaseWithKafkaAsPubSub.ClusterTestEnvMetadata kafkaClusterTestEnvMetadata0;
    private static PscTestBaseWithKafkaAsPubSub.ClusterTestEnvMetadata kafkaClusterTestEnvMetadata1;
    MiniClusterWithClientResource miniClusterResource;
    static InMemoryReporter reporter;

    @TempDir File testDir;

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class DynamicPscSourceSpecificTests {
        @BeforeAll
        void beforeAll() throws Throwable {
            DynamicPscSourceTestHelperWithKafkaAsPubSub.setup();
            DynamicPscSourceTestHelperWithKafkaAsPubSub.createTopic(TOPIC, NUM_PARTITIONS, 1);
            DynamicPscSourceTestHelperWithKafkaAsPubSub.produceToKafka(
                    TOPIC_URI, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT);

            kafkaClusterTestEnvMetadata0 =
                    DynamicPscSourceTestHelperWithKafkaAsPubSub.getClusterTestEnvMetadata(0);
            kafkaClusterTestEnvMetadata1 =
                    DynamicPscSourceTestHelperWithKafkaAsPubSub.getClusterTestEnvMetadata(1);
        }

        @BeforeEach
        void beforeEach() throws Exception {
            reporter = InMemoryReporter.create();
            miniClusterResource =
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(2)
                                    .setConfiguration(
                                            reporter.addToConfiguration(new Configuration()))
                                    .build());
            miniClusterResource.before();
        }

        @AfterEach
        void afterEach() {
            reporter.close();
            miniClusterResource.after();
        }

        @AfterAll
        void afterAll() throws Exception {
            DynamicPscSourceTestHelperWithKafkaAsPubSub.tearDown();
        }

        @Test
        void testBasicMultiClusterRead() throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);

            Properties properties = new Properties();
            properties.setProperty(PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
            properties.setProperty(
                    DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");
            MockPscMetadataService mockPscMetadataService =
                    new MockPscMetadataService(
                            Collections.singleton(
                                    DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC)));

            DynamicPscSource<Integer> dynamicKafkaSource =
                    DynamicPscSource.<Integer>builder()
                            .setStreamIds(
                                    mockPscMetadataService.getAllStreams().stream()
                                            .map(PscStream::getStreamId)
                                            .collect(Collectors.toSet()))
                            .setPscMetadataService(mockPscMetadataService)
                            .setDeserializer(
                                    PscRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();

            DataStreamSource<Integer> stream =
                    env.fromSource(
                            dynamicKafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "dynamic-kafka-src");
            CloseableIterator<Integer> iterator = stream.executeAndCollect();
            List<Integer> results = new ArrayList<>();
            while (results.size()
                            < DynamicPscSourceTestHelperWithKafkaAsPubSub.NUM_PUBSUB_CLUSTERS
                                    * NUM_PARTITIONS
                                    * NUM_RECORDS_PER_SPLIT
                    && iterator.hasNext()) {
                results.add(iterator.next());
            }

            iterator.close();

            // check that all test records have been consumed
            assertThat(results)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(
                                            0,
                                            DynamicPscSourceTestHelperWithKafkaAsPubSub.NUM_PUBSUB_CLUSTERS
                                                    * NUM_PARTITIONS
                                                    * NUM_RECORDS_PER_SPLIT)
                                    .boxed()
                                    .collect(Collectors.toList()));
        }

        @Test
        void testSingleClusterTopicMetadataService() throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);

            Properties properties = new Properties();
            properties.setProperty(PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
            properties.setProperty(
                    DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");

            PscMetadataService pscMetadataService =
                    new SingleClusterTopicUriMetadataService(
                            kafkaClusterTestEnvMetadata0.getPubSubClusterId(),
                            kafkaClusterTestEnvMetadata0.getStandardProperties());

            DynamicPscSource<Integer> dynamicKafkaSource =
                    DynamicPscSource.<Integer>builder()
                            .setStreamIds(
                                    // use topics as stream ids
                                    Collections.singleton(TOPIC))
                            .setPscMetadataService(pscMetadataService)
                            .setDeserializer(
                                    PscRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();

            DataStreamSource<Integer> stream =
                    env.fromSource(
                            dynamicKafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "dynamic-kafka-src");
            CloseableIterator<Integer> iterator = stream.executeAndCollect();
            List<Integer> results = new ArrayList<>();
            while (results.size() < NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT && iterator.hasNext()) {
                results.add(iterator.next());
            }

            iterator.close();

            // check that all test records have been consumed
            assertThat(results)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(0, NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT)
                                    .boxed()
                                    .collect(Collectors.toList()));
        }

        @Test
        void testMigrationUsingFileMetadataService() throws Throwable {
            // setup topics on two clusters
            String fixedTopic = "test-file-metadata-service";
            String fixedTopicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + fixedTopic;
            DynamicPscSourceTestHelperWithKafkaAsPubSub.createTopic(fixedTopic, NUM_PARTITIONS);

            // Flink job config and env
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRestartStrategy(RestartStrategies.noRestart());
            env.setParallelism(2);
            Properties properties = new Properties();
            properties.setProperty(
                    PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "1000");
            properties.setProperty(
                    DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "5000");
            properties.setProperty(
                    DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD.key(),
                    "2");
            properties.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "dynamic-psc-src");

            // create new metadata file to consume from 1 cluster
            String testStreamId = "test-file-metadata-service-stream";
            File metadataFile = File.createTempFile(testDir.getPath() + "/metadata", ".yaml");
            YamlFileMetadataService yamlFileMetadataService =
                    new YamlFileMetadataService(metadataFile.getPath(), Duration.ofMillis(100));
            writeClusterMetadataToFile(
                    metadataFile,
                    testStreamId,
                    fixedTopic,
                    ImmutableList.of(
                            DynamicPscSourceTestHelperWithKafkaAsPubSub.getClusterTestEnvMetadata(0)));

            DynamicPscSource<Integer> dynamicKafkaSource =
                    DynamicPscSource.<Integer>builder()
                            .setStreamIds(Collections.singleton(testStreamId))
                            .setPscMetadataService(yamlFileMetadataService)
                            .setDeserializer(
                                    PscRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();

            DataStreamSource<Integer> stream =
                    env.fromSource(
                            dynamicKafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "dynamic-psc-src");
            List<Integer> results = new ArrayList<>();

            AtomicInteger latestValueOffset =
                    new AtomicInteger(
                            DynamicPscSourceTestHelperWithKafkaAsPubSub.produceToKafka(
                                    0, fixedTopicUri, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, 0));

            try (CloseableIterator<Integer> iterator = stream.executeAndCollect()) {
                CommonTestUtils.waitUtil(
                        () -> {
                            try {
                                results.add(iterator.next());

                                // trigger metadata update to consume from two clusters
                                if (results.size() == NUM_RECORDS_PER_SPLIT) {
                                    latestValueOffset.set(
                                            DynamicPscSourceTestHelperWithKafkaAsPubSub.produceToKafka(
                                                    0,
                                                    fixedTopicUri,
                                                    NUM_PARTITIONS,
                                                    NUM_RECORDS_PER_SPLIT,
                                                    latestValueOffset.get()));
                                    writeClusterMetadataToFile(
                                            metadataFile,
                                            testStreamId,
                                            fixedTopic,
                                            ImmutableList.of(
                                                    DynamicPscSourceTestHelperWithKafkaAsPubSub
                                                            .getClusterTestEnvMetadata(0),
                                                    DynamicPscSourceTestHelperWithKafkaAsPubSub
                                                            .getClusterTestEnvMetadata(1)));
                                }

                                // trigger another metadata update to remove old cluster
                                if (results.size() == latestValueOffset.get()) {
                                    latestValueOffset.set(
                                            DynamicPscSourceTestHelperWithKafkaAsPubSub.produceToKafka(
                                                    1,
                                                    fixedTopicUri,
                                                    NUM_PARTITIONS,
                                                    NUM_RECORDS_PER_SPLIT,
                                                    latestValueOffset.get()));
                                    writeClusterMetadataToFile(
                                            metadataFile,
                                            testStreamId,
                                            fixedTopic,
                                            ImmutableList.of(
                                                    DynamicPscSourceTestHelperWithKafkaAsPubSub
                                                            .getClusterTestEnvMetadata(1)));
                                }
                            } catch (NoSuchElementException e) {
                                // swallow and wait
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }

                            // we will produce 3x
                            return results.size() == NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT * 3;
                        },
                        Duration.ofSeconds(15),
                        "Could not schedule callable within timeout");
            }

            // verify no data loss / duplication in metadata changes
            // cluster0 contains 0-10
            // cluster 1 contains 10-30
            assertThat(results)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(0, NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT * 3)
                                    .boxed()
                                    .collect(Collectors.toList()));
        }

        @Test
        void testStreamPatternSubscriber() throws Throwable {
            DynamicPscSourceTestHelperWithKafkaAsPubSub.createTopic(0, "stream-pattern-test-1", NUM_PARTITIONS);
            int lastValueOffset =
                    DynamicPscSourceTestHelperWithKafkaAsPubSub.produceToKafka(
                            0, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + "stream-pattern-test-1", NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, 0);
            DynamicPscSourceTestHelperWithKafkaAsPubSub.createTopic(0, "stream-pattern-test-2", NUM_PARTITIONS);
            lastValueOffset =
                    DynamicPscSourceTestHelperWithKafkaAsPubSub.produceToKafka(
                            0,
                            PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + "stream-pattern-test-2",
                            NUM_PARTITIONS,
                            NUM_RECORDS_PER_SPLIT,
                            lastValueOffset);
            DynamicPscSourceTestHelperWithKafkaAsPubSub.createTopic(1, "stream-pattern-test-3", NUM_PARTITIONS);
            final int totalRecords =
                    DynamicPscSourceTestHelperWithKafkaAsPubSub.produceToKafka(
                            1,
                            PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + "stream-pattern-test-3",
                            NUM_PARTITIONS,
                            NUM_RECORDS_PER_SPLIT,
                            lastValueOffset);

            // create new metadata file to consume from 1 cluster
            File metadataFile = File.createTempFile(testDir.getPath() + "/metadata", ".yaml");
            YamlFileMetadataService yamlFileMetadataService =
                    new YamlFileMetadataService(metadataFile.getPath(), Duration.ofMillis(100));

            Set<PscStream> pscStreams =
                    getPscStreams(
                            kafkaClusterTestEnvMetadata0.getPubSubClusterId(),
                            kafkaClusterTestEnvMetadata0.getStandardProperties(),
                            ImmutableSet.of("stream-pattern-test-1", "stream-pattern-test-2"));

            writeClusterMetadataToFile(metadataFile, pscStreams, kafkaClusterTestEnvMetadata0.getBrokerConnectionStrings());

            // Flink job config and env
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);
            Properties properties = new Properties();
            properties.setProperty(
                    PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "1000");
            properties.setProperty(
                    DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "5000");
            properties.setProperty(
                    DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD.key(),
                    "2");
            properties.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "dynamic-psc-src");

            DynamicPscSource<Integer> dynamicPscSource =
                    DynamicPscSource.<Integer>builder()
                            .setStreamPattern(Pattern.compile("stream-pattern-test-.+"))
                            .setPscMetadataService(yamlFileMetadataService)
                            .setDeserializer(
                                    PscRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();

            DataStreamSource<Integer> stream =
                    env.fromSource(
                            dynamicPscSource,
                            WatermarkStrategy.noWatermarks(),
                            "dynamic-psc-src");
            List<Integer> results = new ArrayList<>();

            try (CloseableIterator<Integer> iterator = stream.executeAndCollect()) {
                CommonTestUtils.waitUtil(
                        () -> {
                            try {
                                Integer record = iterator.next();
                                results.add(record);

                                // add third stream that matches the regex
                                if (results.size() == NUM_RECORDS_PER_SPLIT) {
                                    pscStreams.add(
                                            getPscStream(
                                                    kafkaClusterTestEnvMetadata1
                                                            .getPubSubClusterId(),
                                                    kafkaClusterTestEnvMetadata1
                                                            .getStandardProperties(),
                                                    "stream-pattern-test-3"));
                                    writeClusterMetadataToFile(metadataFile, pscStreams, kafkaClusterTestEnvMetadata1.getBrokerConnectionStrings());
                                }
                            } catch (NoSuchElementException e) {
                                // swallow
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }

                            return results.size() == totalRecords;
                        },
                        Duration.ofSeconds(15),
                        "Could not obtain the required records within the timeout");
            }
            // verify no data loss / duplication in metadata changes
            assertThat(results)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(0, totalRecords).boxed().collect(Collectors.toList()));
        }

        @Test
        void testMetricsLifecycleManagement() throws Throwable {
            // setup topics on two clusters
            String fixedTopic = "test-metrics-lifecycle-mgmt";
            String fixedTopicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + fixedTopic;
            DynamicPscSourceTestHelperWithKafkaAsPubSub.createTopic(fixedTopic, NUM_PARTITIONS);

            // Flink job config and env
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);
            env.setRestartStrategy(RestartStrategies.noRestart());
            Properties properties = new Properties();
            properties.setProperty(
                    PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "1000");
            properties.setProperty(
                    DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "5000");
            properties.setProperty(
                    DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD.key(),
                    "2");
            properties.setProperty(
                    PscConfiguration.PSC_CONSUMER_GROUP_ID, "testMetricsLifecycleManagement");

            // create new metadata file to consume from 1 cluster
            String testStreamId = "test-file-metadata-service-stream";
            File metadataFile = File.createTempFile(testDir.getPath() + "/metadata", ".yaml");
            YamlFileMetadataService yamlFileMetadataService =
                    new YamlFileMetadataService(metadataFile.getPath(), Duration.ofMillis(100));
            writeClusterMetadataToFile(
                    metadataFile,
                    testStreamId,
                    fixedTopic,
                    ImmutableList.of(
                            DynamicPscSourceTestHelperWithKafkaAsPubSub.getClusterTestEnvMetadata(0)));

            DynamicPscSource<Integer> dynamicKafkaSource =
                    DynamicPscSource.<Integer>builder()
                            .setStreamIds(Collections.singleton(testStreamId))
                            .setPscMetadataService(yamlFileMetadataService)
                            .setDeserializer(
                                    PscRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();

            DataStreamSource<Integer> stream =
                    env.fromSource(
                            dynamicKafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "dynamic-kafka-src");

            int latestValueOffset =
                    DynamicPscSourceTestHelperWithKafkaAsPubSub.produceToKafka(
                            0, fixedTopicUri, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, 0);
            List<Integer> results = new ArrayList<>();
            try (CloseableIterator<Integer> iterator = stream.executeAndCollect()) {
                while (results.size() < latestValueOffset && iterator.hasNext()) {
                    results.add(iterator.next());
                }

                assertThat(results)
                        .containsOnlyOnceElementsOf(
                                IntStream.range(0, latestValueOffset)
                                        .boxed()
                                        .collect(Collectors.toList()));

                // should contain cluster 0 metrics
                assertThat(findMetrics(reporter, DYNAMIC_PSC_SOURCE_METRIC_GROUP))
                        .allSatisfy(
                                metricName ->
                                        assertThat(metricName)
                                                .containsPattern(
                                                        ".*"
                                                                + DYNAMIC_PSC_SOURCE_METRIC_GROUP
                                                                + "\\.pscCluster\\.pubsub-cluster-0.*"));

                // setup test data for cluster 1 and stop consuming from cluster 0
                latestValueOffset =
                        DynamicPscSourceTestHelperWithKafkaAsPubSub.produceToKafka(
                                1,
                                fixedTopic,
                                NUM_PARTITIONS,
                                NUM_RECORDS_PER_SPLIT,
                                latestValueOffset);
                writeClusterMetadataToFile(
                        metadataFile,
                        testStreamId,
                        fixedTopic,
                        ImmutableList.of(
                                DynamicPscSourceTestHelperWithKafkaAsPubSub.getClusterTestEnvMetadata(1)));
                while (results.size() < latestValueOffset && iterator.hasNext()) {
                    results.add(iterator.next());
                }

                // cluster 0 is not being consumed from, metrics should not appear
                assertThat(findMetrics(reporter, DYNAMIC_PSC_SOURCE_METRIC_GROUP))
                        .allSatisfy(
                                metricName ->
                                        assertThat(metricName)
                                                .doesNotContainPattern(
                                                        ".*"
                                                                + DYNAMIC_PSC_SOURCE_METRIC_GROUP
                                                                + "\\.pscCluster\\.pubsub-cluster-0.*"));

                assertThat(findMetrics(reporter, DYNAMIC_PSC_SOURCE_METRIC_GROUP))
                        .allSatisfy(
                                metricName ->
                                        assertThat(metricName)
                                                .containsPattern(
                                                        ".*"
                                                                + DYNAMIC_PSC_SOURCE_METRIC_GROUP
                                                                + "\\.pscCluster\\.pubsub-cluster-1.*"));
            }
        }

        private void writeClusterMetadataToFile(File metadataFile, Set<PscStream> pscStreams, String bootstrapServers)
                throws IOException {
            List<YamlFileMetadataService.StreamMetadata> streamMetadataList = new ArrayList<>();
            for (PscStream pscStream : pscStreams) {
                List<YamlFileMetadataService.StreamMetadata.ClusterMetadata> clusterMetadataList =
                        new ArrayList<>();

                for (Map.Entry<String, ClusterMetadata> entry :
                        pscStream.getClusterMetadataMap().entrySet()) {
                    YamlFileMetadataService.StreamMetadata.ClusterMetadata clusterMetadata =
                            new YamlFileMetadataService.StreamMetadata.ClusterMetadata();
                    clusterMetadata.setClusterId(entry.getKey());

                    clusterMetadata.setBootstrapServers(bootstrapServers);
                    clusterMetadata.setTopics(new ArrayList<>(entry.getValue().getTopics()));
                    clusterMetadataList.add(clusterMetadata);
                }

                YamlFileMetadataService.StreamMetadata streamMetadata =
                        new YamlFileMetadataService.StreamMetadata();
                streamMetadata.setStreamId(pscStream.getStreamId());
                streamMetadata.setClusterMetadataList(clusterMetadataList);
                streamMetadataList.add(streamMetadata);
            }

            YamlFileMetadataService.saveToYaml(streamMetadataList, metadataFile);
        }

        private void writeClusterMetadataToFile(
                File metadataFile,
                String streamId,
                String topic,
                List<PscTestBaseWithKafkaAsPubSub.ClusterTestEnvMetadata> kafkaClusterTestEnvMetadataList)
                throws IOException {
            List<YamlFileMetadataService.StreamMetadata.ClusterMetadata> clusterMetadata =
                    kafkaClusterTestEnvMetadataList.stream()
                            .map(
                                    clusterTestEnvMetadata ->
                                            new YamlFileMetadataService.StreamMetadata
                                                    .ClusterMetadata(
                                                    clusterTestEnvMetadata.getPubSubClusterId(),
                                                    clusterTestEnvMetadata.getClusterUriStr(),
                                                    ImmutableList.of(topic),
                                                    clusterTestEnvMetadata.getBrokerConnectionStrings()))
                            .collect(Collectors.toList());
            YamlFileMetadataService.StreamMetadata streamMetadata =
                    new YamlFileMetadataService.StreamMetadata(streamId, clusterMetadata);
            YamlFileMetadataService.saveToYaml(
                    Collections.singletonList(streamMetadata), metadataFile);
        }

        private Set<String> findMetrics(InMemoryReporter inMemoryReporter, String groupPattern) {
            Optional<MetricGroup> groups = inMemoryReporter.findGroup(groupPattern);
            assertThat(groups).isPresent();
            return inMemoryReporter.getMetricsByGroup(groups.get()).keySet().stream()
                    .map(metricName -> groups.get().getMetricIdentifier(metricName))
                    .collect(Collectors.toSet());
        }

        private Set<PscStream> getPscStreams(
                String kafkaClusterId, Properties properties, Collection<String> topics) {
            return topics.stream()
                    .map(topic -> getPscStream(kafkaClusterId, properties, topic))
                    .collect(Collectors.toSet());
        }

        private PscStream getPscStream(
                String kafkaClusterId, Properties properties, String topic) {
            return new PscStream(
                    topic,
                    Collections.singletonMap(
                            kafkaClusterId,
                            new ClusterMetadata(Collections.singleton(topic), properties)));
        }
    }

    /** Integration test based on connector testing framework. */
    @Nested
    class IntegrationTests extends SourceTestSuiteBase<String> {
        @TestSemantics
        CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

        // Defines test environment on Flink MiniCluster
        @SuppressWarnings("unused")
        @TestEnv
        MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

        @TestExternalSystem
        DefaultContainerizedExternalSystem<TwoKafkaContainers> twoKafkas =
                DefaultContainerizedExternalSystem.builder()
                        .fromContainer(new TwoKafkaContainers())
                        .build();

        @SuppressWarnings("unused")
        @TestContext
        DynamicPscSourceExternalContextFactory twoClusters =
                new DynamicPscSourceExternalContextFactory(
                        twoKafkas.getContainer().getKafka0(),
                        twoKafkas.getContainer().getKafka1(),
                        Collections.emptyList());
    }
}
