package com.pinterest.psc.metadata.client;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.integration.KafkaCluster;
import com.pinterest.psc.metadata.MetadataUtils;
import com.pinterest.psc.metadata.TopicRnMetadata;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.IntegerSerializer;
import com.pinterest.psc.utils.PscTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

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

        topic1Rn = MetadataUtils.createTopicRn(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr1)), topic1);
        topic2Rn = MetadataUtils.createTopicRn(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr2)), topic2);
        topic3Rn = MetadataUtils.createTopicRn(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr3)), topic3);
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

    @Test
    public void testListTopicRns() throws Exception {
        PscMetadataClient client = new PscMetadataClient(metadataClientConfiguration);
        List<TopicRn> topicRnList = client.listTopicRns(KafkaTopicUri.validate(BaseTopicUri.validate(clusterUriStr)), 10000, TimeUnit.MILLISECONDS);
        List<TopicRn> expectedTopicRnList = Arrays.asList(topic1Rn, topic2Rn, topic3Rn);
        assertEquals(expectedTopicRnList, topicRnList);
        client.close();
    }

    @Test
    public void testDescribeTopicRns() throws Exception {
        PscMetadataClient client = new PscMetadataClient(metadataClientConfiguration);
        Map<TopicRn, TopicRnMetadata> topicRnDescriptionMap = client.describeTopicRns(
                KafkaTopicUri.validate(BaseTopicUri.validate(clusterUriStr)),
                new HashSet<>(Arrays.asList(topic1Rn, topic2Rn, topic3Rn)),
                10000,
                TimeUnit.MILLISECONDS
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

    @Test
    public void testListOffsets() throws Exception {
        PscMetadataClient client = new PscMetadataClient(metadataClientConfiguration);
        Map<TopicUriPartition, PscMetadataClient.MetadataClientOption> topicUriPartitionsAndOptions = new HashMap<>();
        topicUriPartitionsAndOptions.put(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr1)), 0), PscMetadataClient.MetadataClientOption.OFFSET_SPEC_EARLIEST);
        topicUriPartitionsAndOptions.put(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr2)), 0), PscMetadataClient.MetadataClientOption.OFFSET_SPEC_LATEST);
        TopicUri clusterUri = KafkaTopicUri.validate(BaseTopicUri.validate(clusterUriStr));
        Map<TopicUriPartition, MessageId> offsets = client.listOffsets(
                clusterUri,
                topicUriPartitionsAndOptions,
                10000,
                TimeUnit.MILLISECONDS
        );
        assertEquals(2, offsets.size());

        // ensure that the offsets are 0 when the topic is empty
        assertEquals(0, offsets.get(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr1)), 0)).getOffset());
        assertEquals(0, offsets.get(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr2)), 0)).getOffset());

        // send one message to partition 0 for both topics
        PscProducer<Integer, Integer> pscProducer = getPscProducer();
        pscProducer.send(new PscProducerMessage(topicUriStr1, 0,0,0));
        pscProducer.send(new PscProducerMessage(topicUriStr2, 0,0,0));
        pscProducer.flush();

        offsets = client.listOffsets(
                clusterUri,
                topicUriPartitionsAndOptions,
                10000,
                TimeUnit.MILLISECONDS
        );

        // ensure sent offsets are captured in next metadataClient call
        assertEquals(2, offsets.size());
        assertEquals(0, offsets.get(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr1)), 0)).getOffset());    // earliest offset
        assertEquals(1, offsets.get(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr2)), 0)).getOffset());    // latest offset

        // now change the spec to latest for both topics, and add topic1 partitions 5 and 10 to the query
        topicUriPartitionsAndOptions.put(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr1)), 0), PscMetadataClient.MetadataClientOption.OFFSET_SPEC_LATEST);
        topicUriPartitionsAndOptions.put(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr2)), 0), PscMetadataClient.MetadataClientOption.OFFSET_SPEC_LATEST);
        topicUriPartitionsAndOptions.put(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr1)), 5), PscMetadataClient.MetadataClientOption.OFFSET_SPEC_LATEST);
        topicUriPartitionsAndOptions.put(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr1)), 10), PscMetadataClient.MetadataClientOption.OFFSET_SPEC_LATEST);

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
                10000,
                TimeUnit.MILLISECONDS
        );

        // ensure sent offsets are captured in next metadataClient call
        assertEquals(4, offsets.size());
        assertEquals(3, offsets.get(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr1)), 0)).getOffset());
        assertEquals(1, offsets.get(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr1)), 5)).getOffset());
        assertEquals(0, offsets.get(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr1)), 10)).getOffset());
        assertEquals(2, offsets.get(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr2)), 0)).getOffset());

        client.close();
        pscProducer.close();
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
