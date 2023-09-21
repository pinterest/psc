package com.pinterest.psc.producer;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.metrics.NullMetricsReporter;
import com.pinterest.psc.metrics.PscMetricTagManager;
import com.pinterest.psc.producer.creation.PscBackendProducerCreator;
import com.pinterest.psc.producer.creation.PscProducerCreatorManager;
import com.pinterest.psc.serde.StringSerializer;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;

@ExtendWith(MockitoExtension.class)
public class TestPscProducerBase {

    @Mock
    protected
    PscBackendProducerCreator<String, String> creator;

    @Mock
    protected
    PscProducerCreatorManager creatorManager;

    protected
    PscProducer<String, String> pscProducer;

    @Mock
    protected
    PscMetricTagManager pscMetricTagManager;

    @Mock
    protected Environment environment;

    protected String keySerializerClass = StringSerializer.class.getName();
    protected String valueSerializerClass = StringSerializer.class.getName();
    protected String metricsReporterClass = TestUtils.DEFAULT_METRICS_REPORTER;

    protected static final String testTopic1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic1";
    protected static final String testTopic2 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic2";
    protected static final String testTopic3 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic3";

    protected static final String kafkaTopic1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:topic1";
    protected static final String kafkaTopic2 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:topic2";
    protected static final String kafkaTopic3 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:topic3";

    protected static final List<String> keysList = Arrays.asList("k1", "k2", "k3");
    protected static final List<String> valuesList = Arrays.asList("v1", "v2", "v3");

    protected static final List<String> testTopics = Arrays.asList(testTopic1, testTopic2, testTopic3);

    protected PscProducerMessage<String, String> getTestMessage(String key, String val, String topicUriString) {
        return getTestMessage(key, val, topicUriString, PscUtils.NO_PARTITION);
    }

    protected PscProducerMessage<String, String> getTestMessage(String key, String val, String topicUriString, int partition) {
        return new PscProducerMessage<>(topicUriString, partition, key, val);
    }
}
