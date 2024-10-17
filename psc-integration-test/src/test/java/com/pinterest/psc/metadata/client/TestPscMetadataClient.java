package com.pinterest.psc.metadata.client;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.integration.KafkaCluster;
import com.pinterest.psc.metadata.MetadataUtils;
import com.pinterest.psc.metadata.TopicRnMetadata;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.IntegerDeserializer;
import com.pinterest.psc.serde.IntegerSerializer;
import com.pinterest.psc.utils.PscTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the functionality and correctness of {@link PscMetadataClient}
 */
public class TestPscMetadataClient {

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource1 = new SharedKafkaTestResource()
            .withBrokers(1).registerListener(new PlainListener().onPorts(9092));
    private static final PscConfiguration metadataClientConfiguration = new PscConfiguration();
    private static String clientId;
    private static final String topic1 = "topic1";
    private static final int partitions1 = 12;
    private static final String topic2 = "topic2";
    private static final int partitions2 = 24;
    private static final String topic3 = "topic3";
    private static final int partitions3 = 36;
    private KafkaCluster kafkaCluster1;
    private String topicUriStr1, topicUriStr2, topicUriStr3, clusterUriStr;
    private TopicRn topic1Rn, topic2Rn, topic3Rn;
    private TopicUri topic1Uri, topic2Uri, topic3Uri;

    /**
     * Initializes two Kafka clusters that are commonly used by all tests, and creates a single topic on each.
     *
     * @throws IOException
     */
    @BeforeEach
    public void setup() throws IOException, InterruptedException, TopicUriSyntaxException {
        clientId = this.getClass().getSimpleName() + "-psc-metadata-client";
        metadataClientConfiguration.clear();
        metadataClientConfiguration.setProperty(PscConfiguration.PSC_METADATA_CLIENT_ID, clientId);
        metadataClientConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        metadataClientConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        kafkaCluster1 = new KafkaCluster("plaintext", "region", "cluster", 9092);
        topicUriStr1 = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster1.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster1.getRegion(), kafkaCluster1.getCluster(), topic1);

        topicUriStr2 = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster1.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster1.getRegion(), kafkaCluster1.getCluster(), topic2);

        topicUriStr3 = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster1.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster1.getRegion(), kafkaCluster1.getCluster(), topic3);

        clusterUriStr = String.format("%s:%s%s:kafka:env:cloud_%s::%s:",
                kafkaCluster1.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster1.getRegion(), kafkaCluster1.getCluster()
        );

        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource1, topic1, partitions1);
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource1, topic2, partitions2);
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource1, topic3, partitions3);

        topic1Uri = KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr1));
        topic2Uri = KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr2));
        topic3Uri = KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr3));

        topic1Rn = MetadataUtils.createTopicRn(topic1Uri, topic1);
        topic2Rn = MetadataUtils.createTopicRn(topic2Uri, topic2);
        topic3Rn = MetadataUtils.createTopicRn(topic3Uri, topic3);
    }

    /**
     * Deleted the topics that are created by default. Also, adds a slight delay to make sure cleanup is complete
     * when tests run consecutively.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @AfterEach
    public void tearDown() throws ExecutionException, InterruptedException {
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource1, topic1);
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource1, topic2);
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource1, topic3);
        Thread.sleep(1000);
    }

    /**
     * Tests that {@link PscMetadataClient#listTopicRns(TopicUri, Duration)} returns the correct list of topic RNs
     *
     * @throws Exception
     */
    @Test
    public void testListTopicRns() throws Exception {
        PscMetadataClient client = new PscMetadataClient(metadataClientConfiguration);
        List<TopicRn> topicRnList = client.listTopicRns(KafkaTopicUri.validate(BaseTopicUri.validate(clusterUriStr)), Duration.ofMillis(10000));
        List<TopicRn> expectedTopicRnList = Arrays.asList(topic1Rn, topic2Rn, topic3Rn);
        assertEquals(expectedTopicRnList, topicRnList);
        client.close();
    }

    /**
     * Tests that {@link PscMetadataClient#describeTopicRns(TopicUri, java.util.Set, Duration)} returns the correct
     * metadata for the supplied topic RNs
     *
     * @throws Exception
     */
    @Test
    public void testDescribeTopicRns() throws Exception {
        PscMetadataClient client = new PscMetadataClient(metadataClientConfiguration);
        Map<TopicRn, TopicRnMetadata> topicRnDescriptionMap = client.describeTopicRns(
                KafkaTopicUri.validate(BaseTopicUri.validate(clusterUriStr)),
                new HashSet<>(Arrays.asList(topic1Rn, topic2Rn, topic3Rn)),
                Duration.ofMillis(10000)
        );
        assertEquals(3, topicRnDescriptionMap.size());

        assertEquals(topic1Rn, topicRnDescriptionMap.get(topic1Rn).getTopicRn());
        assertEquals(partitions1, topicRnDescriptionMap.get(topic1Rn).getTopicUriPartitions().size());
        for (int i = 0; i < partitions1; i++) {
            assertEquals(topic1Rn, topicRnDescriptionMap.get(topic1Rn).getTopicUriPartitions().get(i).getTopicUri().getTopicRn());
            assertEquals(i, topicRnDescriptionMap.get(topic1Rn).getTopicUriPartitions().get(i).getPartition());
        }
        assertEquals(topic2Rn, topicRnDescriptionMap.get(topic2Rn).getTopicRn());
        assertEquals(partitions2, topicRnDescriptionMap.get(topic2Rn).getTopicUriPartitions().size());
        for (int i = 0; i < partitions2; i++) {
            assertEquals(topic2Rn, topicRnDescriptionMap.get(topic2Rn).getTopicUriPartitions().get(i).getTopicUri().getTopicRn());
            assertEquals(i, topicRnDescriptionMap.get(topic2Rn).getTopicUriPartitions().get(i).getPartition());
        }
        assertEquals(topic3Rn, topicRnDescriptionMap.get(topic3Rn).getTopicRn());
        assertEquals(partitions3, topicRnDescriptionMap.get(topic3Rn).getTopicUriPartitions().size());
        for (int i = 0; i < partitions3; i++) {
            assertEquals(topic3Rn, topicRnDescriptionMap.get(topic3Rn).getTopicUriPartitions().get(i).getTopicUri().getTopicRn());
            assertEquals(i, topicRnDescriptionMap.get(topic3Rn).getTopicUriPartitions().get(i).getPartition());
        }
        client.close();
    }

    /**
     * Tests that {@link PscMetadataClient#listOffsets(TopicUri, Map, Duration)} returns the correct offsets for the
     * supplied topic partitions and specs
     *
     * @throws Exception
     */
    @Test
    public void testListOffsets() throws Exception {
        PscMetadataClient client = new PscMetadataClient(metadataClientConfiguration);
        Map<TopicUriPartition, PscMetadataClient.MetadataClientOption> topicUriPartitionsAndOptions = new HashMap<>();
        topicUriPartitionsAndOptions.put(new TopicUriPartition(topic1Uri, 0), PscMetadataClient.MetadataClientOption.OFFSET_SPEC_EARLIEST);
        topicUriPartitionsAndOptions.put(new TopicUriPartition(topic2Uri, 0), PscMetadataClient.MetadataClientOption.OFFSET_SPEC_LATEST);
        TopicUri clusterUri = KafkaTopicUri.validate(BaseTopicUri.validate(clusterUriStr));
        Map<TopicUriPartition, Long> offsets = client.listOffsets(
                clusterUri,
                topicUriPartitionsAndOptions,
                Duration.ofMillis(10000)
        );
        assertEquals(2, offsets.size());

        // ensure that the offsets are 0 when the topic is empty
        assertEquals(0, (long) offsets.get(new TopicUriPartition(topic1Uri, 0)));
        assertEquals(0, (long) offsets.get(new TopicUriPartition(topic2Uri, 0)));

        // send one message to partition 0 for both topics
        PscProducer<Integer, Integer> pscProducer = getPscProducer();
        pscProducer.send(new PscProducerMessage<>(topicUriStr1, 0,0,0));
        pscProducer.send(new PscProducerMessage<>(topicUriStr2, 0,0,0));
        pscProducer.flush();

        offsets = client.listOffsets(
                clusterUri,
                topicUriPartitionsAndOptions,
                Duration.ofMillis(10000)
        );

        // ensure sent offsets are captured in next metadataClient call
        assertEquals(2, offsets.size());
        assertEquals(0, (long) offsets.get(new TopicUriPartition(topic1Uri, 0)));    // earliest offset
        assertEquals(1, (long) offsets.get(new TopicUriPartition(topic2Uri, 0)));    // latest offset

        // now change the spec to latest for both topics, and add topic1 partitions 5 and 10 to the query
        topicUriPartitionsAndOptions.put(new TopicUriPartition(topic1Uri, 0), PscMetadataClient.MetadataClientOption.OFFSET_SPEC_LATEST);
        topicUriPartitionsAndOptions.put(new TopicUriPartition(topic2Uri, 0), PscMetadataClient.MetadataClientOption.OFFSET_SPEC_LATEST);
        topicUriPartitionsAndOptions.put(new TopicUriPartition(topic1Uri, 5), PscMetadataClient.MetadataClientOption.OFFSET_SPEC_LATEST);
        topicUriPartitionsAndOptions.put(new TopicUriPartition(topic1Uri, 10), PscMetadataClient.MetadataClientOption.OFFSET_SPEC_LATEST);

        // send 2 messages to topic1 partition 0 - now the latest offset should be 3
        pscProducer.send(new PscProducerMessage<>(topicUriStr1, 0, 0, 0));
        pscProducer.send(new PscProducerMessage<>(topicUriStr1, 0, 0, 0));
        // send 1 message to topic1 partition 5 - now the latest offset should be 1
        pscProducer.send(new PscProducerMessage<>(topicUriStr1, 5, 0, 0));
        // send 1 message to topic2 partition 0 - now the latest offset should be 2
        pscProducer.send(new PscProducerMessage<>(topicUriStr2, 0, 0, 0));
        pscProducer.flush();

        offsets = client.listOffsets(
                clusterUri,
                topicUriPartitionsAndOptions,
                Duration.ofMillis(10000)
        );

        // ensure sent offsets are captured in next metadataClient call
        assertEquals(4, offsets.size());
        assertEquals(3, (long) offsets.get(new TopicUriPartition(topic1Uri, 0)));
        assertEquals(1, (long) offsets.get(new TopicUriPartition(topic1Uri, 5)));
        assertEquals(0, (long) offsets.get(new TopicUriPartition(topic1Uri, 10)));
        assertEquals(2, (long) offsets.get(new TopicUriPartition(topic2Uri, 0)));

        client.close();
        pscProducer.close();
    }

    /**
     * Tests that {@link PscMetadataClient#listOffsetsForConsumerGroup(TopicUri, String, Collection, Duration)} returns
     * the correct offsets for the supplied consumer group and topic partitions. Also tests correct behavior when supplied
     * with edge case scenarios such as non-existent consumerGroupId, non-existent partitions, etc.
     *
     * @throws Exception
     */
    @Test
    public void testListOffsetsForConsumerGroup() throws Exception {
        PscMetadataClient client = new PscMetadataClient(metadataClientConfiguration);
        TopicUri clusterUri = KafkaTopicUri.validate(BaseTopicUri.validate(clusterUriStr));

        String consumerGroupId = "test-psc-consumer-group";
        PscProducer<Integer, Integer> pscProducer = getPscProducer();
        PscConsumer<Integer, Integer> pscConsumer = getPscConsumer(consumerGroupId);

        sendNMessages(pscProducer, topicUriStr1, 0, 1000);
        sendNMessages(pscProducer, topicUriStr1, 1, 1000);
        sendNMessages(pscProducer, topicUriStr2, 23, 1000);
        sendNMessages(pscProducer, topicUriStr3, 0, 1000);

        TopicUriPartition t1p0 = new TopicUriPartition(topic1Uri, 0);
        TopicUriPartition t1p1 = new TopicUriPartition(topic1Uri, 1);
        TopicUriPartition t2p23 = new TopicUriPartition(topic2Uri, 23);
        TopicUriPartition t3p0 = new TopicUriPartition(topic3Uri, 0);

        List<TopicUriPartition> topicUriPartitions = Arrays.asList(
                t1p0,
                t1p1,
                t2p23,
                t3p0
        );

        // assign to t1p0, poll 1 message, commit
        pscConsumer.assign(Collections.singleton(t1p0));
        pollNMessages(1, pscConsumer);
        pscConsumer.commitSync();

        Map<TopicUriPartition, Long> offsets = client.listOffsetsForConsumerGroup(
                clusterUri,
                consumerGroupId,
                topicUriPartitions,
                Duration.ofMillis(10000)
        );

        assertEquals(1, offsets.size());
        assertTrue(offsets.containsKey(t1p0));
        assertEquals(1, (long) offsets.get(t1p0));
        assertNull(offsets.get(t1p1));
        assertNull(offsets.get(t2p23));
        assertNull(offsets.get(t3p0));

        // already assigned to t1p0, poll 900 messages, commit - this should set the offset to 901
        pollNMessages(900, pscConsumer);
        pscConsumer.commitSync();

        // query again
        offsets = client.listOffsetsForConsumerGroup(
                clusterUri,
                consumerGroupId,
                topicUriPartitions,
                Duration.ofMillis(10000)
        );

        assertEquals(901, (long) offsets.get(t1p0));
        assertNull(offsets.get(t1p1));
        assertNull(offsets.get(t2p23));
        assertNull(offsets.get(t3p0));

        // assign to t1p1, poll 500 messages, commit - this should set the offset to 500
        pscConsumer.unassign();
        pscConsumer.assign(Collections.singleton(t1p1));
        pollNMessages(500, pscConsumer);
        pscConsumer.commitSync();

        // query again
        offsets = client.listOffsetsForConsumerGroup(
                clusterUri,
                consumerGroupId,
                topicUriPartitions,
                Duration.ofMillis(10000)
        );

        assertEquals(901, (long) offsets.get(t1p0));
        assertEquals(500, (long) offsets.get(t1p1));
        assertNull(offsets.get(t2p23));
        assertNull(offsets.get(t3p0));

        // assign to t2p23, poll 1000 messages, commit - this should set the offset to 1000
        pscConsumer.unassign();
        pscConsumer.assign(Collections.singleton(t2p23));
        pollNMessages(1000, pscConsumer);
        pscConsumer.commitSync();

        // query again
        offsets = client.listOffsetsForConsumerGroup(
                clusterUri,
                consumerGroupId,
                topicUriPartitions,
                Duration.ofMillis(10000)
        );

        assertEquals(901, (long) offsets.get(t1p0));
        assertEquals(500, (long) offsets.get(t1p1));
        assertEquals(1000, (long) offsets.get(t2p23));
        assertNull(offsets.get(t3p0));

        // assign to t3p0, poll 999 messages, commit - this should set the offset to 999
        pscConsumer.unassign();
        pscConsumer.assign(Collections.singleton(t3p0));
        pollNMessages(999, pscConsumer);
        pscConsumer.commitSync();

        // query again
        offsets = client.listOffsetsForConsumerGroup(
                clusterUri,
                consumerGroupId,
                topicUriPartitions,
                Duration.ofMillis(10000)
        );

        assertEquals(901, (long) offsets.get(t1p0));
        assertEquals(500, (long) offsets.get(t1p1));
        assertEquals(1000, (long) offsets.get(t2p23));
        assertEquals(999, (long) offsets.get(t3p0));

        // query a non-existent consumer group
        offsets = client.listOffsetsForConsumerGroup(
                clusterUri,
                "non-existent-consumer-group",
                topicUriPartitions,
                Duration.ofMillis(10000)
        );

        assertEquals(0, offsets.size());
        assertNull(offsets.get(t1p0));
        assertNull(offsets.get(t1p1));
        assertNull(offsets.get(t2p23));
        assertNull(offsets.get(t3p0));

        // query a non-existent set of partitions
        offsets = client.listOffsetsForConsumerGroup(
                clusterUri,
                consumerGroupId,
                Collections.singleton(new TopicUriPartition(topic1Uri, 100)),
                Duration.ofMillis(10000)
        );

        assertEquals(0, offsets.size());
        assertNull(offsets.get(new TopicUriPartition(topic1Uri, 100)));

        pscConsumer.close();
        pscProducer.close();
        client.close();
    }

    private static void pollNMessages(int numberOfMessages, PscConsumer<Integer, Integer> pscConsumer) throws ConsumerException {
        int messagesSeen = 0;
        while (messagesSeen < numberOfMessages) {
            PscConsumerPollMessageIterator<Integer, Integer> it = pscConsumer.poll();
            while (it.hasNext()) {
                it.next();
                messagesSeen++;
            }
        }
    }

    private static void sendNMessages(PscProducer<Integer, Integer> producer, String topicUriStr, int partition, int numberOfMessages) throws ConfigurationException, ProducerException {
        for (int i = 0; i < numberOfMessages; i++) {
            producer.send(new PscProducerMessage<>(topicUriStr, partition, i, i));
        }
        producer.flush();
    }

    private static PscConsumer<Integer, Integer> getPscConsumer(String groupId) throws ConfigurationException, ConsumerException {
        PscConfiguration consumerConfiguration = new PscConfiguration();
        consumerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "test-psc-consumer");
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, groupId);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, IntegerDeserializer.class.getName());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, IntegerDeserializer.class.getName());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_POLL_MESSAGES_MAX, "1");
        return new PscConsumer<>(consumerConfiguration);
    }

    private static PscProducer<Integer, Integer> getPscProducer() throws ConfigurationException, ProducerException {
        PscConfiguration producerConfiguration = new PscConfiguration();
        String baseProducerId = "psc-producer-client";
        producerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, baseProducerId + "-" + UUID.randomUUID());
        producerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());

        return new PscProducer<>(producerConfiguration);
    }
}
