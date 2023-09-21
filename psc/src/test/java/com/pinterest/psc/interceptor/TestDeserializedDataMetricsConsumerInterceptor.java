package com.pinterest.psc.interceptor;

import com.codahale.metrics.Snapshot;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.MetricsReporterConfiguration;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerUtils;
import com.pinterest.psc.consumer.StringPscConsumerMessageTestUtil;
import com.pinterest.psc.consumer.creation.PscBackendConsumerCreator;
import com.pinterest.psc.consumer.creation.PscConsumerCreatorManager;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetricTagManager;
import com.pinterest.psc.metrics.PscMetrics;
import com.pinterest.psc.serde.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestDeserializedDataMetricsConsumerInterceptor {
    @Mock
    protected PscConsumerCreatorManager creatorManager;

    @Mock
    protected PscBackendConsumerCreator<String, String> creator;

    @Mock
    protected PscMetricRegistryManager pscMetricRegistryManager;

    @Mock
    protected PscMetricTagManager pscMetricTagManager;

    @Mock
    protected Environment environment = new Environment();

    @Mock
    protected PscConfigurationInternal pscConfigurationInternal;

    protected PscConsumer<String, String> pscConsumer;
    protected String keyDeserializerClass = StringDeserializer.class.getName();
    protected String valueDeserializerClass = StringDeserializer.class.getName();


    @BeforeEach
    void setUp() throws Exception {
        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "client-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "group-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, keyDeserializerClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, valueDeserializerClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        when(pscConfigurationInternal.getEnvironment()).thenReturn(environment);
        when(pscConfigurationInternal.getConfiguration()).thenReturn(pscConfiguration);
        MetricsReporterConfiguration metricsReporterConfiguration = new MetricsReporterConfiguration(
                true, TestUtils.DEFAULT_METRICS_REPORTER, 10, "host001", 9999, 30000
        );
        when(pscConfigurationInternal.getMetricsReporterConfiguration()).thenReturn(metricsReporterConfiguration);
        pscConsumer = new PscConsumer<>(pscConfiguration);

        PscConsumerUtils.setCreatorManager(pscConsumer, creatorManager);
        pscMetricRegistryManager = PscMetricRegistryManager.getInstance();
        pscMetricTagManager = PscMetricTagManager.getInstance();
        pscMetricRegistryManager.setPscMetricTagManager(pscMetricTagManager);
        PscConsumerUtils.setPscMetricRegistryManager(pscConsumer, pscMetricRegistryManager);
        pscMetricTagManager.initializePscMetricTagManager(pscConfigurationInternal);
        when(pscConfigurationInternal.getClientType()).thenReturn(PscConfiguration.PSC_CLIENT_TYPE_CONSUMER);
    }

    @AfterEach
    void tearDown() throws ConsumerException {
        if (pscConsumer != null)
            pscConsumer.close();
    }

    @Test
    void onConsumeDoesNotModifyMessages() throws TopicUriSyntaxException {
        StringPscConsumerMessageTestUtil stringConsumerTestUtil = new StringPscConsumerMessageTestUtil();

        TypedDataMetricsInterceptor<String, String> typedDataMetricsInterceptor =
                new TypedDataMetricsInterceptor<>();
        typedDataMetricsInterceptor.setPscConfigurationInternal(pscConfigurationInternal);

        List<PscConsumerMessage<String, String>> pscConsumerMessageList =
                stringConsumerTestUtil.getRandomPscConsumerMessages(100);

        Map<TopicUri, Integer> totalMessages = new HashMap<>();
        Map<TopicUri, Integer> nonNullKeyCounts = new HashMap<>();
        Map<TopicUri, Integer> nullValueCounts = new HashMap<>();
        Map<TopicUri, Long> keySizes = new HashMap<>();
        Map<TopicUri, Long> valueSizes = new HashMap<>();

        for (PscConsumerMessage<String, String> pscConsumerMessage : pscConsumerMessageList) {
            MessageId messageId = pscConsumerMessage.getMessageId();
            TopicUri topicUri = messageId.getTopicUriPartition().getTopicUri();
            totalMessages.compute(topicUri, (key, val) -> (val == null) ? 1 : val + 1);
            if (pscConsumerMessage.getKey() != null)
                nonNullKeyCounts.compute(topicUri, (key, val) -> (val == null) ? 1 : val + 1);
            if (pscConsumerMessage.getValue() == null)
                nullValueCounts.compute(topicUri, (key, val) -> (val == null) ? 1 : val + 1);
            long keySize = messageId.getSerializedKeySizeBytes();
            keySizes.compute(topicUri, (key, val) -> (val == null) ? keySize : val + keySize);
            long valueSize = messageId.getSerializedValueSizeBytes();
            valueSizes.compute(topicUri, (key, val) -> (val == null) ? valueSize : val + valueSize);
        }

        // verify messages are not modified
        List<PscConsumerMessage<String, String>> pscConsumerMessageListCopy = new ArrayList<>(pscConsumerMessageList);
        pscConsumerMessageList.forEach(typedDataMetricsInterceptor::onConsume);
        stringConsumerTestUtil.verifyIdenticalLists(pscConsumerMessageList, pscConsumerMessageListCopy);

        // verify metrics are as expected
        Set<TopicUri> topicUris = totalMessages.keySet();
        topicUris.forEach(topicUri -> {
            assertEquals(
                    totalMessages.get(topicUri).longValue(),
                    pscMetricRegistryManager.getCounterMetric(topicUri, PscMetrics.PSC_CONSUMER_POLL_MESSAGES_METRIC, pscConfigurationInternal)
            );

            if (nonNullKeyCounts.containsKey(topicUri)) {
                assertEquals(
                        nonNullKeyCounts.get(topicUri).longValue(),
                        pscMetricRegistryManager.getCounterMetric(
                                topicUri,
                                PscMetrics.PSC_CONSUMER_POLL_KEYED_MESSAGES_METRIC, pscConfigurationInternal
                        )
                );
            } else {
                assertEquals(
                        0,
                        pscMetricRegistryManager.getCounterMetric(
                                topicUri,
                                PscMetrics.PSC_CONSUMER_POLL_KEYED_MESSAGES_METRIC, pscConfigurationInternal
                        )
                );
            }

            if (nullValueCounts.containsKey(topicUri)) {
                assertEquals(
                        nullValueCounts.get(topicUri).longValue(),
                        pscMetricRegistryManager.getCounterMetric(
                                topicUri,
                                PscMetrics.PSC_CONSUMER_POLL_NULL_MESSAGE_VALUES_METRIC, pscConfigurationInternal
                        )
                );
            } else {
                assertEquals(
                        0,
                        pscMetricRegistryManager.getCounterMetric(
                                topicUri,
                                PscMetrics.PSC_CONSUMER_POLL_NULL_MESSAGE_VALUES_METRIC, pscConfigurationInternal
                        )
                );
            }

            Snapshot messageKeySizeBytesSnapshot = pscMetricRegistryManager.getHistogramMetric(
                    topicUri,
                    PscMetrics.PSC_CONSUMER_POLL_MESSAGE_KEY_SIZE_BYTES_METRIC, pscConfigurationInternal
            );
            assertNotNull(messageKeySizeBytesSnapshot);
            assertEquals(keySizes.get(topicUri).longValue(), messageKeySizeBytesSnapshot.getMax());

            Snapshot messageValueSizeBytesSnapshot = pscMetricRegistryManager.getHistogramMetric(
                    topicUri,
                    PscMetrics.PSC_CONSUMER_POLL_MESSAGE_VALUE_SIZE_BYTES_METRIC, pscConfigurationInternal
            );
            assertNotNull(messageValueSizeBytesSnapshot);
            assertEquals(valueSizes.get(topicUri).longValue(), messageValueSizeBytesSnapshot.getMax());
        });
    }
}