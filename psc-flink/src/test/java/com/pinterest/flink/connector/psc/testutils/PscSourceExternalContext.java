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

package com.pinterest.flink.connector.psc.testutils;

import com.pinterest.flink.connector.psc.source.PscSource;
import com.pinterest.flink.connector.psc.source.PscSourceBuilder;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.serde.ByteArraySerializer;
import com.pinterest.psc.serde.StringDeserializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import static com.pinterest.flink.connector.psc.testutils.PscTestUtils.injectDiscoveryConfigs;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

/** External context for testing {@link PscSource}. */
public class PscSourceExternalContext implements DataStreamSourceExternalContext<String> {

    private static final Logger LOG = LoggerFactory.getLogger(PscSourceExternalContext.class);
    private static final String TOPIC_NAME_PREFIX = "kafka-test-topic-";
    private static final Pattern TOPIC_NAME_PATTERN = Pattern.compile(TOPIC_NAME_PREFIX + ".*");
    private static final String GROUP_ID_PREFIX = "kafka-source-external-context-";
    private static final int NUM_RECORDS_UPPER_BOUND = 500;
    private static final int NUM_RECORDS_LOWER_BOUND = 100;

    private final List<URL> connectorJarPaths;
    private final String bootstrapServers;
    private final String topicName;
    private final String topicUriStr;
    private final SplitMappingMode splitMappingMode;
    private final AdminClient adminClient;
    private final List<PscTopicUriPartitionDataWriter> writers = new ArrayList<>();

    protected PscSourceExternalContext(
            String bootstrapServers,
            SplitMappingMode splitMappingMode,
            List<URL> connectorJarPaths) {
        this.connectorJarPaths = connectorJarPaths;
        this.bootstrapServers = bootstrapServers;
        this.topicName = randomize(TOPIC_NAME_PREFIX);
        this.topicUriStr = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + this.topicName;
        this.splitMappingMode = splitMappingMode;
        this.adminClient = createAdminClient();
    }

    @Override
    public List<URL> getConnectorJarPaths() {
        return this.connectorJarPaths;
    }

    @Override
    public Source<String, ?, ?> createSource(TestingSourceSettings sourceSettings) {
        final PscSourceBuilder<String> builder = PscSource.builder();

        Properties props = new Properties();
        props.setProperty(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, "false");
        injectDiscoveryConfigs(props, bootstrapServers, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX);

        builder
//                .setBootstrapServers(bootstrapServers)
                .setClusterUri(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX)
                .setProperties(props)
                .setTopicUriPattern(TOPIC_NAME_PATTERN)
                .setGroupId(randomize(GROUP_ID_PREFIX))
                .setDeserializer(
                        PscRecordDeserializationSchema.valueOnly(StringDeserializer.class));

        if (sourceSettings.getBoundedness().equals(Boundedness.BOUNDED)) {
            builder.setBounded(OffsetsInitializer.latest());
        }

        return builder.build();
    }

    @Override
    public ExternalSystemSplitDataWriter<String> createSourceSplitDataWriter(
            TestingSourceSettings sourceSettings) {
        PscTopicUriPartitionDataWriter writer;
        try {
            switch (splitMappingMode) {
                case TOPIC:
                    writer = createSinglePartitionTopic(writers.size());
                    break;
                case PARTITION:
                    writer = scaleOutTopic(this.topicName);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Split mode should be either TOPIC or PARTITION");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create new splits", e);
        }
        writers.add(writer);
        return writer;
    }

    @Override
    public List<String> generateTestData(
            TestingSourceSettings sourceSettings, int splitIndex, long seed) {
        Random random = new Random(seed);
        int recordNum =
                random.nextInt(NUM_RECORDS_UPPER_BOUND - NUM_RECORDS_LOWER_BOUND)
                        + NUM_RECORDS_LOWER_BOUND;
        List<String> records = new ArrayList<>(recordNum);

        for (int i = 0; i < recordNum; i++) {
            int stringLength = random.nextInt(50) + 1;
            records.add(splitIndex + "-" + randomAlphanumeric(stringLength));
        }

        return records;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }

    @Override
    public void close() throws Exception {
        final List<String> topics = new ArrayList<>();
        writers.forEach(
                writer -> {
                    topics.add(writer.getTopicUriPartition().getTopicUri().getTopic());
                    try {
                        writer.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        adminClient.deleteTopics(topics).all().get();
    }

    @Override
    public String toString() {
        return "KafkaSource-" + splitMappingMode.toString();
    }

    private String randomize(String prefix) {
        return prefix + ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
    }

    private AdminClient createAdminClient() {
        Properties config = new Properties();
        config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(config);
    }

    private PscTopicUriPartitionDataWriter createSinglePartitionTopic(int topicIndex) throws Exception {
        String newTopicName = topicName + "-" + topicIndex;
        String newTopicUriStr = topicUriStr + "-" + topicIndex;
        LOG.info("Creating topic '{}'", newTopicName);
        adminClient
                .createTopics(Collections.singletonList(new NewTopic(newTopicName, 1, (short) 1)))
                .all()
                .get();
        return new PscTopicUriPartitionDataWriter(
                getPscProducerProperties(topicIndex), new TopicUriPartition(newTopicUriStr, 0));
    }

    private PscTopicUriPartitionDataWriter scaleOutTopic(String topicName) throws Exception {
        final String topicUriStr = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + topicName;
        final Set<String> topics = adminClient.listTopics().names().get();
        if (topics.contains(topicName)) {
            final Map<String, TopicDescription> topicDescriptions =
                    adminClient.describeTopics(Collections.singletonList(topicName)).all().get();
            final int numPartitions = topicDescriptions.get(topicName).partitions().size();
            LOG.info("Creating partition {} for topic '{}'", numPartitions + 1, topicName);
            adminClient
                    .createPartitions(
                            Collections.singletonMap(
                                    topicName, NewPartitions.increaseTo(numPartitions + 1)))
                    .all()
                    .get();
            return new PscTopicUriPartitionDataWriter(
                    getPscProducerProperties(numPartitions),
                    new TopicUriPartition(topicUriStr, numPartitions));
        } else {
            LOG.info("Creating topic '{}'", topicName);
            adminClient
                    .createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short) 1)))
                    .all()
                    .get();
            return new PscTopicUriPartitionDataWriter(
                    getPscProducerProperties(0), new TopicUriPartition(topicUriStr, 0));
        }
    }

    private Properties getPscProducerProperties(int producerId) {
        Properties pscProducerProperties = new Properties();
//        kafkaProducerProperties.setProperty(
//                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        pscProducerProperties.setProperty(
                PscConfiguration.PSC_PRODUCER_CLIENT_ID,
                String.join(
                        "-",
                        "flink-psc-split-writer",
                        Integer.toString(producerId),
                        Long.toString(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))));
        pscProducerProperties.setProperty(
                PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, ByteArraySerializer.class.getName());
        pscProducerProperties.setProperty(
                PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, ByteArraySerializer.class.getName());
        injectDiscoveryConfigs(pscProducerProperties, bootstrapServers, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX);
        return pscProducerProperties;
    }

    /** Mode of mapping split to Kafka components. */
    public enum SplitMappingMode {
        /** Use a single-partitioned topic as a split. */
        TOPIC,

        /** Use a partition in topic as a split. */
        PARTITION
    }
}
