package com.pinterest.psc.producer;

import com.google.common.collect.ImmutableMap;
import com.pinterest.psc.common.TestTopicUri;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.ExceptionMessage;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metrics.NullMetricsReporter;
import com.pinterest.psc.producer.creation.PscBackendProducerCreator;
import com.pinterest.psc.producer.kafka.PscKafkaProducer;
import com.pinterest.psc.serde.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestPscProducer extends TestPscProducerBase {

    @BeforeEach
    void init() throws ProducerException, ConfigurationException {
        when(creatorManager.getBackendCreators()).thenReturn(Collections.singletonMap("test", creator));
        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "client-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscProducer = new PscProducer<>(pscConfiguration);
        PscProducerUtils.setCreatorManager(pscProducer, creatorManager);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSingleTopicSend() throws ProducerException, ConfigurationException, TopicUriSyntaxException, IOException {
        String topicUriString = testTopics.get(0);
        TopicUri topicUri1 = TopicUri.validate(topicUriString);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);

        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);

        PscKafkaProducer<String, String> backendProducer = mock(PscKafkaProducer.class);
        when(creator.getProducer(any(), any(), any(), any())).thenReturn(backendProducer);

        PscProducerMessage<String, String> message0 = getTestMessage(keysList.get(0), valuesList.get(0), topicUriString);
        PscProducerMessage<String, String> message1 = getTestMessage(keysList.get(1), valuesList.get(1), topicUriString);
        PscProducerMessage<String, String> message2 = getTestMessage(keysList.get(2), valuesList.get(2), topicUriString);

        pscProducer.send(message0);
        pscProducer.send(message1);
        pscProducer.send(message2);

        verify(backendProducer, times(1)).send(message0, null);
        verify(backendProducer, times(1)).send(message1, null);
        verify(backendProducer, times(1)).send(message2, null);

        pscProducer.close();

        verify(backendProducer, times(1)).close(any());
        verify(creatorManager, times(1)).reset();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMultipleTopicsSend() throws ProducerException, ConfigurationException, TopicUriSyntaxException, IOException {
        TopicUri topicUri0 = TopicUri.validate(testTopics.get(0));
        TopicUri testTopicUri0 = TestTopicUri.validate(topicUri0);
        TopicUri topicUri1 = TopicUri.validate(testTopics.get(1));
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);
        TopicUri topicUri2 = TopicUri.validate(testTopics.get(2));
        TopicUri testTopicUri2 = TestTopicUri.validate(topicUri2);

        when(creator.validateBackendTopicUri(topicUri0)).thenReturn(testTopicUri0);
        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.validateBackendTopicUri(topicUri2)).thenReturn(testTopicUri2);

        PscKafkaProducer<String, String> backendProducer = mock(PscKafkaProducer.class);
        when(creator.getProducer(any(), any(), any(), any())).thenReturn(backendProducer);

        PscProducerMessage<String, String> message0 = getTestMessage(keysList.get(0), valuesList.get(0), testTopics
                .get(0));
        PscProducerMessage<String, String> message1 = getTestMessage(keysList.get(1), valuesList.get(1), testTopics
                .get(1));
        PscProducerMessage<String, String> message2 = getTestMessage(keysList.get(2), valuesList.get(2), testTopics
                .get(2));

        pscProducer.send(message0);
        pscProducer.send(message1);
        pscProducer.send(message2);

        verify(backendProducer, times(1)).send(message0, null);
        verify(backendProducer, times(1)).send(message1, null);
        verify(backendProducer, times(1)).send(message2, null);

        pscProducer.close();

        verify(backendProducer, times(1)).close(any());
        verify(creatorManager, times(1)).reset();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testBadBackend() throws TopicUriSyntaxException, ProducerException, IOException {
        String topicUriAsString = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":badbackend:env:cloud_region::cluster:topic";
        TopicUri topicUri = TopicUri.validate(topicUriAsString);
        Exception e = assertThrows(ProducerException.class,
                () -> pscProducer.send(
                        getTestMessage(keysList.get(0), valuesList.get(0), topicUriAsString))
        );
        pscProducer.close();
        assertEquals(ExceptionMessage.TOPIC_URI_UNSUPPORTED_BACKEND(topicUri.getBackend()), e.getMessage());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMultipleBackendProducers() throws TopicUriSyntaxException, ProducerException, ConfigurationException, IOException {
        TopicUri testBaseUri = TopicUri.validate(testTopic1);
        TopicUri testTopicUri = TestTopicUri.validate(testBaseUri);
        TopicUri kafkaBaseUri = TopicUri.validate(kafkaTopic1);
        TopicUri kafkaTopicUri = KafkaTopicUri.validate(kafkaBaseUri);

        PscBackendProducerCreator<String, String> testCreator = creator;
        PscBackendProducerCreator<String, String> kafkaCreator = mock(PscBackendProducerCreator.class);

        when(testCreator.validateBackendTopicUri(testBaseUri)).thenReturn(testTopicUri);
        when(kafkaCreator.validateBackendTopicUri(kafkaBaseUri)).thenReturn(kafkaTopicUri);

        PscKafkaProducer<String, String> backendProducer = mock(PscKafkaProducer.class);
        PscKafkaProducer<String, String> backendProducer2 = mock(PscKafkaProducer.class);

        when(creatorManager.getBackendCreators()).thenReturn(ImmutableMap.of("test", testCreator, "kafka", kafkaCreator));

        when(testCreator.getProducer(any(), any(), any(), any())).thenReturn(backendProducer);
        when(kafkaCreator.getProducer(any(), any(), any(), any())).thenReturn(backendProducer2);

        PscProducerMessage<String, String> message = getTestMessage(keysList.get(0), valuesList.get(0), testTopic1);
        PscProducerMessage<String, String> message2 = getTestMessage(keysList.get(1), valuesList.get(1), kafkaTopic1);

        pscProducer.send(message);
        pscProducer.send(message2);

        verify(backendProducer, times(1)).send(message, null);
        verify(backendProducer2, times(1)).send(message2, null);

        pscProducer.close();

        verify(backendProducer, times(1)).close(any());
        verify(creatorManager, times(1)).reset();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testClosedProducerErrorScenario() throws TopicUriSyntaxException, ProducerException, ConfigurationException, IOException {
        String topicUriString = testTopics.get(0);
        TopicUri topicUri1 = TopicUri.validate(topicUriString);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);

        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);

        PscKafkaProducer<String, String> backendProducer = mock(PscKafkaProducer.class);
        when(creator.getProducer(any(), any(), any(), any())).thenReturn(backendProducer);

        PscProducerMessage<String, String> message0 = getTestMessage(keysList.get(0), valuesList.get(0), topicUriString);
        pscProducer.send(message0);

        verify(backendProducer, times(1)).send(message0, null);

        pscProducer.close();

        PscProducerMessage<String, String> message1 = getTestMessage(keysList.get(1), valuesList.get(1), topicUriString);

        Exception e = assertThrows(ProducerException.class, () -> pscProducer.send(message1));
        assertEquals(ExceptionMessage.ALREADY_CLOSED_EXCEPTION, e.getMessage());

        verify(backendProducer, times(0)).send(message1, null);
        verify(backendProducer, times(1)).close(any());
        verify(creatorManager, times(1)).reset();
    }


    @Test
    @SuppressWarnings("unchecked")
    void testGetPartitionsNonExistingTopic() throws Exception {
        String nonExistingTopicUriString = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:not_there";
        TopicUri nonExistingTopicUri = TopicUri.validate(nonExistingTopicUriString);
        TopicUri nonExistingTestTopicUri = TestTopicUri.validate(nonExistingTopicUri);

        PscKafkaProducer<String, String> backendProducer = mock(PscKafkaProducer.class);
        when(creator.validateBackendTopicUri(nonExistingTopicUri)).thenReturn(nonExistingTestTopicUri);
        when(creator.getProducer(any(), any(), any(), any())).thenReturn(backendProducer);
        when(backendProducer.getPartitions(nonExistingTestTopicUri)).thenReturn(null);

        assertNull(pscProducer.getPartitions(nonExistingTopicUriString));
        pscProducer.close();
    }
}
