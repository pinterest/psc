package com.pinterest.psc.consumer.memq;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.consumer.MemqConsumer;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.TopicMetadata;
import com.pinterest.memq.commons.storage.LocalStorageHandler;
import com.pinterest.memq.commons.storage.s3.AbstractS3StorageHandler;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.config.EnvironmentProvider;
import com.pinterest.memq.core.config.MemqConfig;
import com.pinterest.memq.core.config.NettyServerConfig;
import com.pinterest.memq.core.rpc.MemqNettyServer;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaMessageId;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetricTagManager;
import com.pinterest.psc.serde.ByteArrayDeserializer;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestPscMemqConsumer {

    private static final String TOPIC1 = "topic1";
    private static final String testMemqTopic1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":memq:env:cloud_region::cluster:topic1";
    private static final String testMemqTopic2 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":memq:env:cloud_region::cluster:topic2";
    private static final String testKafkaTopic = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:topic3";
    protected static final long defaultPollTimeoutMs = 5000;

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokers(1).registerListener(new PlainListener().onPorts(9092));

    @Mock
    protected PscMetricRegistryManager pscMetricRegistryManager;

    @Mock
    private ServiceDiscoveryConfig discoveryConfig;

    @Mock
    protected PscMetricTagManager pscMetricTagManager;

    @Mock
    protected Environment environment;

    protected String keyDeserializerClass = ByteArrayDeserializer.class.getName();
    protected String valueDeserializerClass = ByteArrayDeserializer.class.getName();
    private short port;
    private MemqNettyServer server;
    private String notificationTopic = "test_notification_topic";

    @BeforeEach
    void setUp() throws Exception {
        port = 22311;

        Mockito.lenient().when(discoveryConfig.getConnect()).thenReturn("localhost:" + port);

        String pathname = "target/" + notificationTopic;

        server = initializeMemqServer(port, notificationTopic, pathname, TOPIC1);

        pscMetricRegistryManager = PscMetricRegistryManager.getInstance();
        pscMetricTagManager = PscMetricTagManager.getInstance();
        pscMetricRegistryManager.setPscMetricTagManager(pscMetricTagManager);
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    @Test
    void subscribeErrorScenarios() throws Exception {
        PscMemqConsumer<byte[], byte[]> pscMemqConsumer = getPscMemqConsumer(
                "test_subscribeErrorScenarios");

        pscMemqConsumer.subscribe(Collections.emptySet());
        assertEquals(Collections.emptySet(), pscMemqConsumer.subscription());

        // subscribe to multiple topics
        assertThrows(ConsumerException.class,
                () -> pscMemqConsumer
                        .subscribe(Sets.newHashSet(MemqTopicUri.validate(TopicUri.validate(testMemqTopic1)),
                                MemqTopicUri.validate(TopicUri.validate(testMemqTopic2)))));

        // subscribe to non-memq topic
        assertThrows(ConsumerException.class, () -> pscMemqConsumer
                .subscribe(Sets.newHashSet(KafkaTopicUri.validate(TopicUri.validate(testKafkaTopic)))));

        pscMemqConsumer.close();
    }

    @Test
    void subscribeAndSubscription() throws Exception {
        PscMemqConsumer<byte[], byte[]> pscMemqConsumer = getPscMemqConsumer(
                "test_subscribeErrorScenarios");

        // test empty subscribe (should return empty)
        pscMemqConsumer.subscribe(Sets.newHashSet());
        assertEquals(Sets.newHashSet(), pscMemqConsumer.subscription());

        MemqTopicUri uri1 = MemqTopicUri.validate(TopicUri.validate(testMemqTopic1));
        pscMemqConsumer.subscribe(Sets.newHashSet(uri1));
        assertEquals(Sets.newHashSet(uri1), pscMemqConsumer.subscription());

        pscMemqConsumer.close();
    }

    @Test
    void unsubscribe() throws Exception {
        PscMemqConsumer<byte[], byte[]> pscMemqConsumer = getPscMemqConsumer(
                "test_subscribeErrorScenarios");

        MemqTopicUri uri1 = MemqTopicUri.validate(TopicUri.validate(testMemqTopic1));
        pscMemqConsumer.subscribe(Sets.newHashSet(uri1));
        pscMemqConsumer.unsubscribe();
        assertEquals(Collections.emptySet(), pscMemqConsumer.subscription());

        pscMemqConsumer.close();
    }

    @Test
    void close() throws Exception {
        PscMemqConsumer<byte[], byte[]> pscMemqConsumer = getPscMemqConsumer(
                "test_subscribeErrorScenarios");

        MemqTopicUri uri1 = MemqTopicUri.validate(TopicUri.validate(testMemqTopic1));
        pscMemqConsumer.subscribe(Sets.newHashSet(uri1));
        pscMemqConsumer.close();

        assertTrue(pscMemqConsumer.subscription().isEmpty());
    }

    @Test
    void pollErrorScenarios() throws Exception {
        PscMemqConsumer<byte[], byte[]> pscMemqConsumer = getPscMemqConsumer(
                "test_subscribeErrorScenarios");

        // no subscription
        assertThrows(ConsumerException.class,
                () -> pscMemqConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)));

        // backend throws an exception
        MemqConsumer<byte[], byte[]> memqConsumer = Mockito.mock(MemqConsumer.class);
        pscMemqConsumer.setMemqConsumer(memqConsumer);
        MemqTopicUri uri1 = MemqTopicUri.validate(TopicUri.validate(testMemqTopic1));
        pscMemqConsumer.subscribe(Sets.newHashSet(uri1));
        when(pscMemqConsumer.memqConsumer.poll(any(), any())).thenThrow(IOException.class);

        assertThrows(ConsumerException.class,
                () -> pscMemqConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)));

        pscMemqConsumer.close();
    }

    @Test
    void testPoll() throws Exception {
        KafkaProducer<byte[], byte[]> producer = sharedKafkaTestResource.getKafkaTestUtils()
                .getKafkaProducer(org.apache.kafka.common.serialization.ByteArraySerializer.class,
                        org.apache.kafka.common.serialization.ByteArraySerializer.class);
        JsonObject payload = AbstractS3StorageHandler.buildPayload(TOPIC1, "test", 1024 * 1024, 100,
                LocalStorageHandler.getBatchHeaderSize().get(), "test", 0);

        producer.send(new ProducerRecord<>(notificationTopic, new Gson().toJson(payload).getBytes()));
        producer.flush();
        producer.close();

        List<ConsumerRecord<byte[], byte[]>> records = sharedKafkaTestResource.getKafkaTestUtils()
                .consumeAllRecordsFromTopic(notificationTopic);
        assertEquals(1, records.size());

        PscMemqConsumer<byte[], byte[]> pscMemqConsumer = getPscMemqConsumer(
                "test_" + System.currentTimeMillis());

        MemqTopicUri uri1 = MemqTopicUri.validate(TopicUri.validate(testMemqTopic1));
        pscMemqConsumer.subscribe(Sets.newHashSet(uri1));
        PscConsumerPollMessageIterator<byte[], byte[]> pollResult = pscMemqConsumer
                .poll(Duration.ofMillis(5000));

        int counter = 0;
        while (pollResult.hasNext()) {
            pollResult.next();
            counter++;
        }

        assertEquals(10000, counter);
        pscMemqConsumer.close();

        server.stop();
    }

    @Test
    void seekErrorScenarios() throws Exception {
        PscMemqConsumer<byte[], byte[]> pscMemqConsumer = getPscMemqConsumer(
                "test_" + System.currentTimeMillis());

        // non memq uri
        KafkaTopicUri kafkaUri = KafkaTopicUri.validate(TopicUri.validate(testKafkaTopic));
        assertThrows(ConsumerException.class, () -> pscMemqConsumer.seekToOffset(
                new HashMap<TopicUriPartition, Long>() {
                    {
                        put(new TopicUriPartition(kafkaUri.getTopicUriAsString(), 0), 0L);
                    }
                }
                )
        );

        // topic not in subscription
        MemqTopicUri memqUri = MemqTopicUri.validate(TopicUri.validate(testMemqTopic1));
        assertThrows(ConsumerException.class, () -> pscMemqConsumer.seekToOffset(
                new HashMap<TopicUriPartition, Long>() {
                    {
                        put(new TopicUriPartition(memqUri.getTopicUriAsString(), 0), 0L);
                    }
                }
        ));

        // seek to timestamp
        assertThrows(ConsumerException.class,
                () -> pscMemqConsumer.seekToTimestamp(memqUri, System.currentTimeMillis() - 100000));

        pscMemqConsumer.close();
    }

    @Test
    void seek() throws Exception {
        PscMemqConsumer<byte[], byte[]> pscMemqConsumer = getPscMemqConsumer(
                "test_" + System.currentTimeMillis());

        MemqTopicUri memqUri = MemqTopicUri.validate(TopicUri.validate(testMemqTopic1));
        pscMemqConsumer.subscribe(Sets.newHashSet(memqUri));

        MemqConsumer<byte[], byte[]> memqConsumer = Mockito.mock(MemqConsumer.class);
        pscMemqConsumer.setMemqConsumer(memqConsumer);
        doNothing().when(pscMemqConsumer.memqConsumer).seek(any());
        pscMemqConsumer.seekToOffset(new HashMap<TopicUriPartition, Long>() {
            {
                put(TestUtils.getFinalizedTopicUriPartition(memqUri, 0), 0L);
            }
        });
        verify(pscMemqConsumer.memqConsumer, times(1)).seek(any());
        pscMemqConsumer.close();
    }

    @Test
    void commitErrorScenarios() throws Exception {
        // non memq uri
        PscMemqConsumer<byte[], byte[]> pscMemqConsumer = getPscMemqConsumer(
                "test_" + System.currentTimeMillis());
        KafkaTopicUri kafkaUri = KafkaTopicUri.validate(TopicUri.validate(testKafkaTopic));
        Set<MessageId> kafkaMessageIds = Collections
                .singleton(new KafkaMessageId(new TopicUriPartition(testKafkaTopic, 0), 0));
        assertThrows(ConsumerException.class, () -> pscMemqConsumer.commitSync(kafkaMessageIds));

        // topic not in subscription
        MemqTopicUri memqUri = MemqTopicUri.validate(TopicUri.validate(testMemqTopic1));
        Set<MessageId> memqMessageIds = Collections
                .singleton(new MemqMessageId(new TopicUriPartition(testKafkaTopic, 0), 0, false));
        assertThrows(ConsumerException.class, () -> pscMemqConsumer.commitSync(memqMessageIds));

        // invalid message id
        pscMemqConsumer.subscribe(Sets.newHashSet(memqUri));
        assertThrows(ConsumerException.class, () -> pscMemqConsumer.commitSync(kafkaMessageIds));

        pscMemqConsumer.close();
    }

    @Test
    void commit() throws Exception {
        PscMemqConsumer<byte[], byte[]> pscMemqConsumer = getPscMemqConsumer(
                "test_" + System.currentTimeMillis());
        MemqTopicUri memqUri = MemqTopicUri.validate(TopicUri.validate(testMemqTopic1));
        MessageId memqMessageId1 = new MemqMessageId(
                TestUtils.getFinalizedTopicUriPartition(memqUri, 0), 0, false);
        MessageId memqMessageId2 = new MemqMessageId(
                TestUtils.getFinalizedTopicUriPartition(memqUri, 0), 10, false);
        pscMemqConsumer.subscribe(Sets.newHashSet(memqUri));

        MemqConsumer<byte[], byte[]> memqConsumer = Mockito.mock(MemqConsumer.class);
        pscMemqConsumer.setMemqConsumer(memqConsumer);
        doNothing().when(pscMemqConsumer.memqConsumer).commitOffset(any());
        pscMemqConsumer.commitSync(Sets.newHashSet(memqMessageId1, memqMessageId2));
        Map<Integer, Long> memqCommitMap = new HashMap<>();
        memqCommitMap.put(0, 10L);
        verify(pscMemqConsumer.memqConsumer, times(0)).commitOffset();
        verify(pscMemqConsumer.memqConsumer, times(1)).commitOffset(any());
        verify(pscMemqConsumer.memqConsumer, times(1)).commitOffset(memqCommitMap);
        pscMemqConsumer.close();
    }

    private PscMemqConsumer<byte[], byte[]> getPscMemqConsumer(String groupId) throws Exception {
        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "test");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, groupId);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        PscConfigurationInternal pscConfigurationInternal = new PscConfigurationInternal(
                pscConfiguration,
                PscConfiguration.PSC_CLIENT_TYPE_CONSUMER
        );
        pscConfigurationInternal.overrideDefaultConfigurations(PscConfiguration.PSC_METRICS_REPORTER_CLASS,
                TestUtils.DEFAULT_METRICS_REPORTER);
        pscConfigurationInternal.overrideDefaultConfigurations(PscConfiguration.PSC_METRICS_REPORTER_PARALLELISM, "10");
        pscConfigurationInternal.overrideDefaultConfigurations(PscConfiguration.PSC_METRICS_HOST, "host001");
        pscConfigurationInternal.overrideDefaultConfigurations(PscConfiguration.PSC_METRICS_PORT, "9999");
        pscConfigurationInternal.overrideDefaultConfigurations(PscConfiguration.PSC_METRICS_FREQUENCY_MS, "30000");
        pscConfigurationInternal.overrideDefaultConfigurations(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER,
                keyDeserializerClass);
        pscConfigurationInternal.overrideDefaultConfigurations(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER,
                valueDeserializerClass);

        pscMetricRegistryManager.initialize(pscConfigurationInternal);

        PscMemqConsumer<byte[], byte[]> pscMemqConsumer = new PscMemqConsumer<>();
                pscMemqConsumer.initialize(pscConfigurationInternal, discoveryConfig,
                MemqTopicUri.validate(TopicUri.validate(testMemqTopic1)));

        return pscMemqConsumer;
    }

    private MemqNettyServer initializeMemqServer(short port,
                                                 String notificationTopic,
                                                 String pathname,
                                                 String memqTopic) throws Exception {
        AdminClient adminClient = sharedKafkaTestResource.getKafkaTestUtils().getAdminClient();
        adminClient
                .createTopics(Collections.singletonList(new NewTopic(notificationTopic, 1, (short) 1)));
        adminClient.close();

        MemqConfig configuration = new MemqConfig();
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setPort(port);
        configuration.setNettyServerConfig(nettyServerConfig);
        TopicConfig topicConfig = new TopicConfig("test", "localstorage");
        topicConfig.setBufferSize(1024 * 1024);
        topicConfig.setBatchSizeMB(2);
        topicConfig.setRingBufferSize(128);
        topicConfig.setTickFrequencyMillis(10);
        topicConfig.setBatchMilliSeconds(2000);
        Properties storageConfig = new Properties();
        storageConfig.setProperty("delay.min.millis", "1");
        storageConfig.setProperty("delay.max.millis", "2");

        Files.write("localhost:9092".getBytes(), new File(pathname));
        storageConfig.setProperty("notificationServerset", pathname);
        storageConfig.setProperty("notificationTopic", notificationTopic);
        topicConfig.setStorageHandlerConfig(storageConfig);
        configuration.setTopicConfig(new TopicConfig[]{topicConfig});
        MemqManager memqManager = new MemqManager(null, configuration, new HashMap<>());
        memqManager.init();

        EnvironmentProvider provider = new TestEnvironmentProvider();
        MemqGovernor governor = new MemqGovernor(memqManager, configuration, provider);

        TopicMetadata md = new TopicMetadata(memqTopic, "localstorage", storageConfig);
        md.getReadBrokers().add(new Broker("127.0.0.1", port, "2xl", "us-east-1a", Broker.BrokerType.READ, new HashSet<>()));
        md.getWriteBrokers().add(new Broker("127.0.0.1", port, "2xl", "us-east-1a", Broker.BrokerType.WRITE, new HashSet<>()));
        governor.getTopicMetadataMap().put(memqTopic, md);

        MemqNettyServer server = new MemqNettyServer(configuration, memqManager, governor,
                new HashMap<>(), null);
        server.initialize();
        return server;
    }

    public static final class TestEnvironmentProvider extends EnvironmentProvider {
        @Override
        public String getRack() {
            return "local";
        }

        @Override
        public String getInstanceType() {
            return "2xl";
        }

        @Override
        public String getIP() {
            return "127.0.0.1";
        }
    }
}