package com.pinterest.psc.consumer;

import com.google.common.collect.Lists;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.creation.PscBackendConsumerCreator;
import com.pinterest.psc.consumer.creation.PscConsumerCreatorManager;
import com.pinterest.psc.consumer.kafka.KafkaToPscMessageIteratorConverter;
import com.pinterest.psc.consumer.listener.MessageCounterListener;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.interceptor.Interceptors;
import com.pinterest.psc.interceptor.ConsumerInterceptors;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetricTagManager;
import com.pinterest.psc.serde.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class TestPscConsumerBase {
    protected static final String testTopic1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic1";
    protected static final String testTopic2 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic2";
    protected static final String testTopic3 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic3";
    protected static final String testKafkaTopic1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:topic1";
    protected static final long defaultPollTimeoutMs = 500;

    protected static final List<String[]> keysList = Arrays.asList(
            new String[]{"k11", "k12", "k13"},
            new String[]{"k21", "k22", "k23"},
            new String[]{"k31", "k32", "k33"});

    protected static final List<String[]> valuesList = Arrays.asList(
            new String[]{"v11", "v12", "v13"},
            new String[]{"v21", "v22", "v23"},
            new String[]{"v31", "v32", "v33"});

    protected static final List<String[]> uriList = Arrays.asList(
            new String[]{testTopic1, testTopic1, testTopic1},
            new String[]{testTopic2, testTopic2, testTopic2},
            new String[]{testTopic3, testTopic3, testTopic3});

    @Mock
    protected
    PscConsumerCreatorManager creatorManager;

    @Mock
    protected
    PscBackendConsumerCreator<String, String> creator;

    @Mock
    protected
    PscMetricRegistryManager pscMetricRegistryManager;

    @Mock
    protected
    PscMetricTagManager pscMetricTagManager;

    protected PscConsumer<String, String> pscConsumer;

    @Mock
    MessageCounterListener<String, String> messageListener;

    @Mock
    protected Environment environment;

    @Mock
    protected PscConfigurationInternal pscConfigurationInternal;

    protected String keyDeserializerClass = StringDeserializer.class.getName();
    protected String valueDeserializerClass = StringDeserializer.class.getName();
    protected String messageListenerClass = MessageCounterListener.class.getName();
    protected String metricsReporterClass = TestUtils.DEFAULT_METRICS_REPORTER;

    public static class TestMessageId extends MessageId {

        public TestMessageId(TopicUriPartition topicUriPartition, long offset) {
            super(topicUriPartition, offset);
        }
    }

    protected PscConsumerPollMessageIterator<String, String> getTestMessages(String[] keys, String[] values, String[] uris) throws TopicUriSyntaxException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getTestMessages(keys, values, uris, 0, getNoOpInterceptors(), null);
    }

    protected PscConsumerPollMessageIterator<String, String> getTestMessages(
            String[] keys, String[] values, String[] uris, Interceptors<String, String> interceptors) throws TopicUriSyntaxException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getTestMessages(keys, values, uris, 0, interceptors, null);
    }

    protected PscConsumerPollMessageIterator<String, String> getTestMessages(
            String[] keys, String[] values, String[] uris, Supplier<Header[]> headerSupplier) throws TopicUriSyntaxException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getTestMessages(keys, values, uris, 0, getNoOpInterceptors(), headerSupplier);
    }

    protected PscConsumerPollMessageIterator<String, String> getTestMessages(
            String[] keys, String[] values, String[] uris, long publishTimestamp, Supplier<Header[]> headerSupplier) throws TopicUriSyntaxException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getTestMessages(keys, values, uris, publishTimestamp, getNoOpInterceptors(), headerSupplier);
    }

    PscConsumerPollMessageIterator<String, String> getTestMessages(
            String[] keys,
            String[] values,
            String[] uris,
            long publishTimestamp,
            Interceptors<String, String> interceptors,
            Supplier<Header[]> headerSupplier)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, TopicUriSyntaxException {
        List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
        Map<String, TopicUri> topicToTopicUri = new HashMap<>();
        for (int i = 0; i < keys.length; i++) {
            ConsumerRecord<byte[], byte[]> consumerRecord = headerSupplier != null ?
                    new ConsumerRecord<>(uris[i], 0, i, publishTimestamp, TimestampType.NO_TIMESTAMP_TYPE, 0L,
                            -1, -1, keys[i].getBytes(), values[i].getBytes(),
                            new RecordHeaders(headerSupplier.get())) :
                    new ConsumerRecord<>(uris[i], 0, i, publishTimestamp, TimestampType.NO_TIMESTAMP_TYPE, 0L,
                            -1, -1, keys[i].getBytes(), values[i].getBytes());
            recordList.add(consumerRecord);
            TopicUri topicUri = TopicUri.validate(uris[i]);
            topicToTopicUri.put(uris[i], new KafkaTopicUri(topicUri));
        }

        ConsumerInterceptors<String, String> consumerInterceptors = new ConsumerInterceptors<>(
                interceptors,
                pscConfigurationInternal,
                (StringDeserializer) Class.forName(keyDeserializerClass).newInstance(),
                (StringDeserializer) Class.forName(valueDeserializerClass).newInstance()
        );

        return new SimplePscConsumerPollMessages<>(
                new KafkaToPscMessageIteratorConverter<>(
                        recordList.iterator(),
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        topicToTopicUri,
                        consumerInterceptors
                )
        );
    }

    @SafeVarargs
    protected final void verifyConsumerPollResult(
            PscConsumerPollMessageIterator<String, String> polledMessages,
            PscConsumerPollMessageIterator<String, String>... expectedMessages
    ) {
        List<PscConsumerMessage<String, String>> polledMessagesList = Lists.newArrayList(polledMessages);

        for (PscConsumerPollMessageIterator<String, String> expectedMessagesPart : expectedMessages) {
            while (expectedMessagesPart.hasNext()) {
                assertTrue(findElementAndRemove(polledMessagesList, expectedMessagesPart.next()));
            }
        }

        assertTrue(polledMessagesList.isEmpty());
    }

    protected <T> boolean findElementAndRemove(List<T> elementList, T elementToFind) {
        for (T element : elementList) {
            if (element.equals(elementToFind)) {
                elementList.remove(element);
                return true;
            }
        }
        return false;
    }

    private Interceptors<String, String> getNoOpInterceptors() {
        return new Interceptors<>(null, null, pscConfigurationInternal);
    }
}
