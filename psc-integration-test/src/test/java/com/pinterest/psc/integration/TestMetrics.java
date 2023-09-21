package com.pinterest.psc.integration;

import com.codahale.metrics.ScheduledReporter;
import com.google.common.util.concurrent.AtomicDouble;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.consumer.PscConsumerUtils;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.ExceptionMessage;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import com.pinterest.psc.metrics.MetricTags;
import com.pinterest.psc.metrics.NoOpMetricsReporter;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetricTag;
import com.pinterest.psc.metrics.PscMetricsUtil;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.producer.PscProducerUtils;
import com.pinterest.psc.serde.StringSerializer;
import com.pinterest.psc.utils.PscTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.errors.InterruptException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestMetrics {
    private static final PscLogger logger = PscLogger.getLogger(TestProducerAndConsumerWithKafkaBackend.class);
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource().withBrokers(1);

    private static final int TEST_TIMEOUT_SECONDS = 60;
    private static final PscConfiguration producerConfiguration = new PscConfiguration();
    private static final PscConfiguration consumerConfiguration = new PscConfiguration();
    private static final PscConfiguration discoveryConfiguration = new PscConfiguration();
    private static PscConfigurationInternal producerInternalConfiguration;
    private static PscConfigurationInternal consumerInternalConfiguration;
    private static String baseProducerClientId;
    private static String baseConsumerClientId;
    private static final String topic1 = "topic1";
    private static final int partitions1 = 12;
    private static final String topic2 = "topic2";
    private static final int partitions2 = 1;
    private String topicUriPrefix1, topicUriStr1, topicUriPrefix2, topicUriStr2;
    private AdminClient adminClient;

    /**
     * Initializes a Kafka cluster that is commonly used by all tests, and creates a single topic on it.
     *
     * @throws InterruptedException
     */
    @BeforeEach
    public void setup() throws InterruptedException, ConfigurationException {
        topicUriPrefix1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:";
        topicUriStr1 = topicUriPrefix1 + topic1;
        topicUriPrefix2 = topicUriPrefix1;
        topicUriStr2 = topicUriPrefix2 + topic2;

        discoveryConfiguration.clear();
        discoveryConfiguration.setProperty("psc.discovery.topic.uri.prefixes", topicUriPrefix1);
        discoveryConfiguration.setProperty("psc.discovery.connection.urls", sharedKafkaTestResource.getKafkaConnectString());
        discoveryConfiguration.setProperty("psc.discovery.security.protocols", "plaintext");

        baseProducerClientId = this.getClass().getSimpleName() + "-psc-producer-client";
        producerConfiguration.clear();
        producerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, baseProducerClientId + "-" + UUID.randomUUID());
        producerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        producerInternalConfiguration = new PscConfigurationInternal(producerConfiguration, PscConfiguration.PSC_CLIENT_TYPE_PRODUCER);

        baseConsumerClientId = this.getClass().getSimpleName() + "-psc-consumer-client";
        consumerConfiguration.clear();
        consumerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseConsumerClientId + "-" + UUID.randomUUID());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, baseConsumerClientId);
        consumerInternalConfiguration = new PscConfigurationInternal(consumerConfiguration, PscConfigurationInternal.PSC_CLIENT_TYPE_CONSUMER);

        discoveryConfiguration.getKeys().forEachRemaining(key -> {
            producerConfiguration.setProperty(key, discoveryConfiguration.getString(key));
            consumerConfiguration.setProperty(key, discoveryConfiguration.getString(key));
        });

        adminClient = sharedKafkaTestResource.getKafkaTestUtils().getAdminClient();

        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource, topic1, partitions1);
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource, topic2, partitions2);
        PscMetricsUtil.cleanup(PscMetricRegistryManager.getInstance());
    }

    /**
     * Deleted the topic that is created by default. Also, adds a slight delay to make sure cleanup is complete
     * when tests run consecutively.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @AfterEach
    public void tearDown() throws ExecutionException, InterruptedException {
        PscMetricsUtil.cleanup(PscMetricRegistryManager.getInstance());
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource, topic1);
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource, topic2);
        adminClient.close();
        Thread.sleep(2000);
    }

    /**
     * Verifies that not initializing the metrics registry manager will be gracefully handled and will not interrupt the
     * main behavior of PSC consumers and producers.
     *
     * @throws TopicUriSyntaxException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testGracefulHandlingOfMetricsWithoutInitialization() throws TopicUriSyntaxException {
        PscMetricRegistryManager.getInstance().incrementCounterMetric(TopicUri.validate(topicUriStr1), "counter.metric", consumerInternalConfiguration);
    }

    /**
     * Verifies that producer/consumer metrics API returns metrics as expected and the proper backend tag is injected
     * into all returned metrics. Also, verifies fluidity of metric values; i.e. that they change over time based on
     * metrics coming from the backend consumer.
     *
     * @throws ConfigurationException
     * @throws ClientException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testMetrics() throws ConfigurationException, ClientException, InterruptedException {
        int messageCount = 5000;
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_METRICS_FREQUENCY_MS, 1000);
        PscProducer<String, String> pscProducer = new PscProducer<>(producerConfiguration);
        Map<MetricName, Metric> metrics = pscProducer.metrics();
        assertNotNull(metrics);
        assertTrue(metrics.isEmpty());
        PscProducerMessage<String, String> pscProducerMessage;
        List<Future<MessageId>> sendResponseFutures = new ArrayList<>();
        for (int i = 0; i < messageCount; ++i) {
            pscProducerMessage = new PscProducerMessage<>(topicUriStr1, "" + i, "" + i, System.currentTimeMillis());
            sendResponseFutures.add(pscProducer.send(pscProducerMessage));
        }

        Thread.sleep(5000);

        metrics = pscProducer.metrics();
        assertNotNull(metrics);
        assertFalse(metrics.isEmpty());
        metrics.forEach((name, metric) -> {
            assertTrue(name.tags().containsKey("backend"));
            assertEquals(PscUtils.BACKEND_TYPE_KAFKA, name.tags().get("backend"));
        });
        pscProducer.close();
        ProducerException producerException = assertThrows(ProducerException.class, () -> pscProducer.metrics());
        assertEquals(ExceptionMessage.ALREADY_CLOSED_EXCEPTION, producerException.getMessage());

        while (!sendResponseFutures.isEmpty()) {
            sendResponseFutures.removeIf(Future::isDone);
            Thread.sleep(100);
        }

        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group-" + UUID.randomUUID());
        consumerConfiguration.setProperty(PscConfiguration.PSC_METRICS_FREQUENCY_MS, 1000);
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(consumerConfiguration);
        metrics = pscConsumer.metrics();
        assertNotNull(metrics);
        assertTrue(metrics.isEmpty());

        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        int count = 0;
        while (true) {
            PscConsumerPollMessageIterator<String, String> messageIterator = pscConsumer.poll();
            while (messageIterator.hasNext()) {
                messageIterator.next();
                ++count;
            }

            if (count >= messageCount)
                break;
        }

        Thread.sleep(5000);
        metrics = pscConsumer.metrics();
        assertNotNull(metrics);
        assertFalse(metrics.isEmpty());
        AtomicReference<Metric> randomMetric = new AtomicReference<>(null);
        AtomicDouble randomMetricValue = new AtomicDouble(Double.MIN_VALUE);
        metrics.forEach((name, metric) -> {
            if (name.name().equals("heartbeat-total") && randomMetric.get() == null) {
                randomMetric.set(metric);
                randomMetricValue.set((double) metric.metricValue());
                logger.info("Read value {} for metric name {}.", randomMetricValue.get(), name.name());
            }
            assertTrue(name.tags().containsKey("backend"));
            assertEquals(PscUtils.BACKEND_TYPE_KAFKA, name.tags().get("backend"));
        });

        // run a few more polls
        boolean metricValueChanged = false;
        for (int i = 0; i < 20; ++i) {
            pscConsumer.poll();
            Thread.sleep(250);
            logger.info("{}: Read following value {} for metric name {}.", i, randomMetric.get().metricValue(), randomMetric.get().metricName().name());
            if (((double) randomMetric.get().metricValue()) != randomMetricValue.get())
                metricValueChanged = true;
        }

        // verify that metric value changes over time
        assertTrue(metricValueChanged);

        pscConsumer.close();
        ConsumerException consumerException = assertThrows(ConsumerException.class, () -> pscConsumer.metrics());
        assertEquals(ExceptionMessage.ALREADY_CLOSED_EXCEPTION, consumerException.getMessage());
    }

    /**
     * Verifies that when metrics reporting is disabled for the consumer, no metric reporter is being created (meaning
     * no metrics is collected).
     *
     * @throws ConfigurationException
     * @throws ClientException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testConsumerWithDisabledMetricsReporting() throws ConfigurationException, ConsumerException, InterruptedException {
        consumerConfiguration.setProperty(PscConfiguration.PSC_METRIC_REPORTING_ENABLED, "false");
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group");
        PscConsumer<byte[], byte[]> pscConsumer = new PscConsumer<>(consumerConfiguration);
        PscMetricsUtil.cleanup(PscMetricRegistryManager.getInstance());
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        // consume for 10 secs
        long startMs = System.currentTimeMillis();
        while (System.currentTimeMillis() - startMs < 10000) {
            pscConsumer.poll();
            Thread.sleep(50);
        }

        PscMetricRegistryManager pscMetricRegistryManager = PscConsumerUtils.getMetricRegistryManager(pscConsumer);
        Map<String, ScheduledReporter> metricReporterMap = PscMetricsUtil.getPscMetricReporterMap(pscMetricRegistryManager);
        assertTrue(metricReporterMap.isEmpty());
        pscConsumer.close();
    }

    /**
     * Verifies that when metrics reporting is disabled for the producer, no metric reporter is being created (meaning
     * no metrics is collected).
     *
     * @throws ConfigurationException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testProducerWithDisabledMetricsReporting() throws ConfigurationException, ProducerException, InterruptedException {
        producerConfiguration.setProperty(PscConfiguration.PSC_METRIC_REPORTING_ENABLED, "false");
        PscProducer<byte[], byte[]> pscProducer = new PscProducer<>(producerConfiguration);
        PscMetricsUtil.cleanup(PscMetricRegistryManager.getInstance());
        // consume for 10 secs
        long startMs = System.currentTimeMillis();
        while (System.currentTimeMillis() - startMs < 10000) {
            PscProducerMessage pscProducerMessage = new PscProducerMessage(topicUriStr1, "test".getBytes(StandardCharsets.UTF_8));
            pscProducer.send(pscProducerMessage);
            Thread.sleep(50);
        }

        PscMetricRegistryManager pscMetricRegistryManager = PscProducerUtils.getMetricRegistryManager(pscProducer);
        Map<String, ScheduledReporter> metricReporterMap = PscMetricsUtil.getPscMetricReporterMap(pscMetricRegistryManager);
        assertTrue(metricReporterMap.isEmpty());
        pscProducer.close();
    }


    /**
     * Verifies that generated metrics are correct. This test specifically verifies that a single metrics registry is
     * used when multiple clients run in different threads; that the configured metric reporter is used; and that the
     * reported thread id tag matches the thread under which the client was running.
     *
     * @throws InterruptedException
     */
    @Disabled // flaky test
    @Test
    public void testMetricsAccuracy() throws InterruptedException {
        int numThreads = partitions1;
        producerConfiguration.setProperty(PscConfiguration.PSC_METRICS_FREQUENCY_MS, 1000);
        producerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, NoOpMetricsReporter.class.getName());

        Map<PscProducerThread, Long> producerThreadToThreadId = new HashMap<>();
        Map<String, Long> idToThreadId = new HashMap<>();
        for (int i = 0; i < numThreads; ++i) {
            String id = Integer.toString(i);
            producerConfiguration.setProperty(PscConfiguration.PSC_PROJECT, id);
            producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, id);
            PscProducerThread pscProducerThread = new PscProducerThread(producerConfiguration, topicUriStr1, i);
            pscProducerThread.start();
            Thread.sleep(1000);
            producerThreadToThreadId.put(pscProducerThread, pscProducerThread.getThreadId());
            idToThreadId.put(id, pscProducerThread.getThreadId());
        }

        // verify all producers use the same metric registry manager
        PscMetricRegistryManager pscMetricRegistryManager = null;
        for (PscProducerThread pscProducerThread : producerThreadToThreadId.keySet()) {
            PscMetricRegistryManager pscMetricRegistryManager1 = PscProducerUtils.getMetricRegistryManager(pscProducerThread.getPscProducer());
            if (pscMetricRegistryManager == null)
                pscMetricRegistryManager = pscMetricRegistryManager1;
            else
                assertEquals(pscMetricRegistryManager, pscMetricRegistryManager1);
        }
        assertNotNull(pscMetricRegistryManager);

        Thread.sleep(2000);
        // wait until all producers have produced messages
        boolean ready = false;
        while (!ready) {
            ready = true;
            for (PscProducerThread pscProducerThread : producerThreadToThreadId.keySet()) {
                if (!pscProducerThread.produced()) {
                    ready = false;
                    break;
                }
            }
            Thread.sleep(1000);
        }

        // print the map
        logger.info("[Main thread: {}]", Thread.currentThread().getId());
        for (int i = 0; i < numThreads; ++i) {
            logger.info("[Producer {} thread: {}]", i, idToThreadId.get(Integer.toString(i)));
        }

        Map<String, ScheduledReporter> metricReporterMap = new HashMap<>(PscMetricsUtil.getPscMetricReporterMap(pscMetricRegistryManager));

        // stop the threads
        for (PscProducerThread pscProducerThread : producerThreadToThreadId.keySet()) {
            Thread.sleep(100);
            pscProducerThread.interrupt();
            pscProducerThread.join();
        }

        // verify that all reporters are of the configured type
        assertFalse(metricReporterMap.isEmpty());

        for (ScheduledReporter reporter : metricReporterMap.values()) {
            assertTrue(String.format("Metric reporter %s is not an instance of %s.", reporter.getClass().getName(), NoOpMetricsReporter.class.getName()),
                    reporter instanceof NoOpMetricsReporter);
            NoOpMetricsReporter noOpMetricsReporter = (NoOpMetricsReporter) reporter;
            List<MetricTags> metricTags = noOpMetricsReporter.getMetrics().stream().map(tuple -> tuple._4).collect(Collectors.toList());
            for (MetricTags metricTag : metricTags) {
                if (metricTag == null)
                    continue;

                String id = metricTag.getProject(); // project id and client id and partition should all match this
                long expectedThreadId = idToThreadId.get(id);

                long threadId = metricTag.getThreadId();
                assertEquals(expectedThreadId, threadId);

                assertEquals(PscUtils.getVersion(), metricTag.getVersion());

                if (metricTag.getOthers() == null)
                    continue;

                if (!metricTag.getOthers().containsKey(PscMetricTag.PSC_TAG_PARTITION)) {
                    assertEquals(PscUtils.NO_TOPIC_URI, metricTag.getUri());
                }

                String producerId = metricTag.getOthers().get(PscMetricTag.PSC_TAG_CLIENT_ID);
                assertEquals(id, producerId);
            }
        }

        metricReporterMap.clear();
        idToThreadId.clear();
        producerThreadToThreadId.clear();
        logger.info("Completed the metrics accuracy test.");
    }

    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testVersionTagValue() throws ConfigurationException, ConsumerException, InterruptedException {
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group");
        consumerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, NoOpMetricsReporter.class.getName());
        PscConsumer<byte[], byte[]> pscConsumer = new PscConsumer<>(consumerConfiguration);
        PscMetricsUtil.cleanup(PscMetricRegistryManager.getInstance());
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        // consume for 10 secs
        long startMs = System.currentTimeMillis();
        while (System.currentTimeMillis() - startMs < 10000) {
            pscConsumer.poll();
            Thread.sleep(50);
        }

        PscMetricRegistryManager pscMetricRegistryManager = PscConsumerUtils.getMetricRegistryManager(pscConsumer);
        Map<String, ScheduledReporter> metricReporterMap = PscMetricsUtil.getPscMetricReporterMap(pscMetricRegistryManager);
        assertFalse(metricReporterMap.isEmpty());

        for (Map.Entry<String, ScheduledReporter> entry: metricReporterMap.entrySet()) {
            assertEquals(NoOpMetricsReporter.class, entry.getValue().getClass());
            NoOpMetricsReporter noOpMetricsReporter = (NoOpMetricsReporter) entry.getValue();
            List<MetricTags> metricTags = noOpMetricsReporter.getMetrics().stream().map(tuple -> tuple._4).collect(Collectors.toList());
            for (MetricTags metricTag : metricTags) {
                if (metricTag == null)
                    continue;

                assertEquals(PscUtils.getVersion(), metricTag.getVersion());
            }
        }
        pscConsumer.close();
    }

    /**
     * Verifies that when metrics reporting is disabled, no metric reporter is being created (meaning no metrics is
     * collected).
     *
     * @throws ConfigurationException
     * @throws ConsumerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testDisabledMetricsReporting() throws ConfigurationException, ConsumerException, InterruptedException {
        consumerConfiguration.setProperty(PscConfiguration.PSC_METRIC_REPORTING_ENABLED, "false");
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group");
        consumerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, NoOpMetricsReporter.class.getName());
        PscConsumer<byte[], byte[]> pscConsumer = new PscConsumer<>(consumerConfiguration);
        PscMetricsUtil.cleanup(PscMetricRegistryManager.getInstance());
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        // consume for 10 secs
        long startMs = System.currentTimeMillis();
        while (System.currentTimeMillis() - startMs < 5000) {
            pscConsumer.poll();
            Thread.sleep(50);
        }

        PscMetricRegistryManager pscMetricRegistryManager = PscConsumerUtils.getMetricRegistryManager(pscConsumer);
        Map<String, ScheduledReporter> metricReporterMap = PscMetricsUtil.getPscMetricReporterMap(pscMetricRegistryManager);
        assertTrue(metricReporterMap.isEmpty());
        pscConsumer.close();
    }

    static class PscProducerThread extends Thread {
        private boolean active = true;
        private final AtomicInteger count = new AtomicInteger(0);
        private final PscConfiguration pscConfiguration;
        private final String topicUriStr;
        private int partition = PscUtils.NO_PARTITION;
        private long threadId;
        private PscProducer<String, String> pscProducer;

        public PscProducerThread(PscConfiguration pscConfiguration, String topicUriStr) {
            this.pscConfiguration = pscConfiguration;
            this.topicUriStr = topicUriStr;

            this.pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
            this.pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        }

        public PscProducerThread(PscConfiguration pscConfiguration, String topicUriStr, int partition) {
            this(pscConfiguration, topicUriStr);
            this.partition = partition;
        }

        public void halt() {
            active = false;
        }

        public long getThreadId() {
            return threadId;
        }

        public PscConfiguration getPscConfiguration() {
            return pscConfiguration;
        }

        public PscProducer getPscProducer() {
            return pscProducer;
        }

        public boolean produced() {
            return count.get() > 0;
        }

        @Override
        public void run() {
            this.threadId = Thread.currentThread().getId();
            count.set(0);
            try {
                pscProducer = new PscProducer<>(pscConfiguration);
                logger.info(
                        "Producer with id {} started in thread {}.",
                        pscConfiguration.getString(PscConfiguration.PSC_PRODUCER_CLIENT_ID), threadId
                );
                while (active) {
                    PscProducerMessage<String, String> pscProducerMessage = new PscProducerMessage<>(
                            topicUriStr, partition, null, String.format("%d-%d", threadId, count.get())
                    );
                    pscProducer.send(pscProducerMessage, (messageId, exception) -> count.incrementAndGet());
                    Thread.sleep(50);
                }
            } catch (ProducerException | ConfigurationException | InterruptException | InterruptedException e) {
                logger.warn("Producer loop stopped for thread id {}.", threadId);
            } finally {
                try {
                    pscProducer.close();
                } catch (ProducerException ignored) {
                }
                logger.info("Thread with id {} completed after producing {} messages.", threadId, count);
            }
        }
    }
}
