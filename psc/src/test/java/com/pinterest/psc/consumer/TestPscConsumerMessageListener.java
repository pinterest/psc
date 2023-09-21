package com.pinterest.psc.consumer;

import com.google.common.collect.Sets;
import com.pinterest.psc.common.TestTopicUri;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.kafka.PscKafkaConsumer;
import com.pinterest.psc.consumer.listener.MessageCounterListener;
import com.pinterest.psc.exception.ExceptionMessage;
import com.pinterest.psc.exception.consumer.ConsumerException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPscConsumerMessageListener extends TestPscConsumerBase {

    @SuppressWarnings("unchecked")
    @BeforeEach
    private void init() throws Exception {
        when(creatorManager.getBackendCreators()).thenReturn(Collections.singletonMap("test", creator));

        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "client-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "group-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, keyDeserializerClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, valueDeserializerClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_MESSAGE_LISTENER, messageListenerClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, metricsReporterClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        pscConsumer = new PscConsumer<>(pscConfiguration);
        messageListener = (MessageCounterListener) PscConsumerUtils.getMessageListener(pscConsumer);

        PscConsumerUtils.setCreatorManager(pscConsumer, creatorManager);
        pscMetricRegistryManager.setPscMetricTagManager(pscMetricTagManager);
        pscConsumer.setPscMetricRegistryManager(pscMetricRegistryManager);
        pscMetricTagManager.initializePscMetricTagManager(pscConfigurationInternal);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMessageListenerMultiConsumerPollOnce() throws Exception {
        PscConsumerPollMessageIterator<String, String> messages1 = getTestMessages(keysList.get(0), valuesList.get(0), uriList.get(0));
        PscConsumerPollMessageIterator<String, String> messages2 = getTestMessages(keysList.get(1), valuesList.get(1), uriList.get(1));

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri topicUri2 = TopicUri.validate(testTopic2);

        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);
        TopicUri testTopicUri2 = TestTopicUri.validate(topicUri2);

        PscKafkaConsumer<String, String> backendConsumer1 = mock(PscKafkaConsumer.class);
        when(backendConsumer1.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic1))));
        when(backendConsumer1.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages1).thenReturn(null);

        PscKafkaConsumer<String, String> backendConsumer2 = mock(PscKafkaConsumer.class);
        when(backendConsumer2.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic2))));
        when(backendConsumer2.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages2).thenReturn(null);

        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.validateBackendTopicUri(topicUri2)).thenReturn(testTopicUri2);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean())).thenReturn(
                Sets.newHashSet(backendConsumer1, backendConsumer2));

        pscConsumer.subscribe(Sets.newHashSet(testTopic1, testTopic2));
        Thread.sleep(1000);
        pscConsumer.close();
        Assertions.assertEquals(6, messageListener.getHandleCallCounter());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMessageListenerMultiConsumerPollMultipleTimes() throws Exception {
        PscConsumerPollMessageIterator<String, String> messages1 = getTestMessages(keysList.get(0), valuesList.get(0), uriList.get(0));
        PscConsumerPollMessageIterator<String, String> messages2 = getTestMessages(keysList.get(1), valuesList.get(1), uriList.get(1));

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri topicUri2 = TopicUri.validate(testTopic2);

        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);
        TopicUri testTopicUri2 = TestTopicUri.validate(topicUri2);

        PscKafkaConsumer<String, String> backendConsumer1 = mock(PscKafkaConsumer.class);
        when(backendConsumer1.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic1))));
        when(backendConsumer1.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages1).thenReturn(null);

        PscKafkaConsumer<String, String> backendConsumer2 = mock(PscKafkaConsumer.class);
        when(backendConsumer2.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic2))));
        // null at first poll, messages at second poll, then null for the rest
        when(backendConsumer2.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(null).thenReturn(messages2).thenReturn(null);

        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.validateBackendTopicUri(topicUri2)).thenReturn(testTopicUri2);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean())).thenReturn(
                Sets.newHashSet(backendConsumer1, backendConsumer2));

        pscConsumer.subscribe(Sets.newHashSet(testTopic1, testTopic2));
        Thread.sleep(1000);
        pscConsumer.close();
        Assertions.assertEquals(6, messageListener.getHandleCallCounter());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMessageListenerPollException() throws Exception {
        PscKafkaConsumer<String, String> backendConsumer1 = mock(PscKafkaConsumer.class);
        when(backendConsumer1.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic1))));

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri topicUri2 = TopicUri.validate(testTopic2);

        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);
        TopicUri testTopicUri2 = TestTopicUri.validate(topicUri2);

        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.validateBackendTopicUri(topicUri2)).thenReturn(testTopicUri2);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean())).thenReturn(
                Collections.singleton(backendConsumer1));

        // poll() should fail once consumer is initialized
        Exception e = assertThrows(ConsumerException.class, pscConsumer::poll);
        assertEquals(ExceptionMessage.MUTUALLY_EXCLUSIVE_APIS("poll()", "MessageListener"), e.getMessage());

        pscConsumer.subscribe(Sets.newHashSet(testTopic1, testTopic2));
        // poll() should still fail after subscribe
        e = assertThrows(ConsumerException.class, pscConsumer::poll);
        assertEquals(ExceptionMessage.MUTUALLY_EXCLUSIVE_APIS("poll()", "MessageListener"), e.getMessage());

        pscConsumer.unsubscribe();
        // poll() should still fail after unsubscribe
        e = assertThrows(ConsumerException.class, pscConsumer::poll);
        assertEquals(ExceptionMessage.MUTUALLY_EXCLUSIVE_APIS("poll()", "MessageListener"), e.getMessage());

        pscConsumer.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMessageListenerUnsubscribe() throws Exception {
        PscConsumerPollMessageIterator<String, String> messages1 = getTestMessages(keysList.get(0), valuesList.get(0), uriList.get(0));
        PscConsumerPollMessageIterator<String, String> messages2 = getTestMessages(keysList.get(1), valuesList.get(1), uriList.get(1));

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri topicUri2 = TopicUri.validate(testTopic2);

        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);
        TopicUri testTopicUri2 = TestTopicUri.validate(topicUri2);

        PscKafkaConsumer<String, String> backendConsumer1 = mock(PscKafkaConsumer.class);
        when(backendConsumer1.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic1))));
        when(backendConsumer1.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages1);

        PscKafkaConsumer<String, String> backendConsumer2 = mock(PscKafkaConsumer.class);
        when(backendConsumer2.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic2))));
        // will always have messages to poll
        when(backendConsumer2.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages2);

        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.validateBackendTopicUri(topicUri2)).thenReturn(testTopicUri2);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean())).thenReturn(
                Sets.newHashSet(backendConsumer1, backendConsumer2));

        pscConsumer.subscribe(Sets.newHashSet(testTopic1, testTopic2));
        Thread.sleep(1000);
        pscConsumer.unsubscribe();
        Thread.sleep(1000);
        Assertions.assertEquals(6, messageListener.getHandleCallCounter());
        Thread.sleep(1000);
        Assertions.assertEquals(6, messageListener.getHandleCallCounter());

        pscConsumer.close();
    }
}
