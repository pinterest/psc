package com.pinterest.psc.consumer;

import com.google.common.collect.Sets;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TestTopicUri;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.kafka.PscKafkaConsumer;
import com.pinterest.psc.exception.ExceptionMessage;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestPscConsumer extends TestPscConsumerBase {

    private static final String testTopic1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic1";
    private static final String testTopic2 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic2";

    private static final List<String[]> keysList = Arrays.asList(new String[]{"k11", "k12", "k13"}, new String[]{"k21", "k22", "k23"});
    private static final List<String[]> valuesList = Arrays.asList(new String[]{"v11", "v12", "v13"}, new String[]{"v21", "v22", "v23"});
    private static final List<String[]> uriList = Arrays.asList(new String[]{testTopic1, testTopic1, testTopic1}, new String[]{testTopic2, testTopic1, testTopic2});

    @BeforeEach
    protected void init() throws Exception {
        when(creatorManager.getBackendCreators()).thenReturn(Collections.singletonMap("test", creator));

        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "client-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "group-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, keyDeserializerClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, valueDeserializerClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, metricsReporterClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        pscConsumer = new PscConsumer<>(pscConfiguration);

        PscConsumerUtils.setCreatorManager(pscConsumer, creatorManager);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSingleTopicSingleConsumer() throws Exception {
        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(0), valuesList.get(0), uriList.get(0));
        PscConsumerPollMessageIterator<String, String> messagesCp = getTestMessages(keysList.get(0), valuesList.get(0), uriList.get(0));

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);

        PscKafkaConsumer<String, String> backendConsumer = mock(PscKafkaConsumer.class);
        when(backendConsumer.subscription()).thenReturn(Collections.singleton(testTopicUri1));
        when(backendConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages);

        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean())).thenReturn(Collections.singleton(backendConsumer));

        Set<String> topics = Collections.singleton(testTopic1);
        pscConsumer.subscribe(topics);

        assertEquals(topics, pscConsumer.subscription());

        verifyConsumerPollResult(pscConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)), messagesCp);
        pscConsumer.unsubscribe();
        verify(creatorManager, times(1)).reset();
        verify(backendConsumer, times(1)).unsubscribe();
        verify(backendConsumer, times(1)).close();

        pscConsumer.close();
        verify(creatorManager, times(2)).reset();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMultiTopicSingleConsumer() throws Exception {
        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(1), valuesList.get(1), uriList.get(1));
        PscConsumerPollMessageIterator<String, String> messagesCopy = getTestMessages(keysList.get(1), valuesList.get(1), uriList.get(1));

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri topicUri2 = TopicUri.validate(testTopic2);

        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);
        TopicUri testTopicUri2 = TestTopicUri.validate(topicUri2);

        PscKafkaConsumer<String, String> backendConsumer = mock(PscKafkaConsumer.class);
        when(backendConsumer.subscription()).thenReturn(Sets.newHashSet(testTopicUri1, testTopicUri2));
        when(backendConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages);

        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.validateBackendTopicUri(topicUri2)).thenReturn(testTopicUri2);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean())).thenReturn(Collections.singleton(backendConsumer));

        Set<String> topics = Sets.newHashSet(testTopic1, testTopic2);
        pscConsumer.subscribe(topics);

        assertEquals(topics, pscConsumer.subscription());

        verifyConsumerPollResult(pscConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)), messagesCopy);
        verify(backendConsumer, times(1)).poll(Duration.ofMillis(defaultPollTimeoutMs));

        pscConsumer.unsubscribe();
        verify(creatorManager, times(1)).reset();
        verify(backendConsumer, times(1)).unsubscribe();
        verify(backendConsumer, times(1)).close();

        pscConsumer.close();
        verify(creatorManager, times(2)).reset();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMultiTopicMultiConsumer() throws Exception {
        PscConsumerPollMessageIterator<String, String> messages1 = getTestMessages(keysList.get(0), valuesList.get(0), uriList.get(0));
        PscConsumerPollMessageIterator<String, String> messages1Cp = getTestMessages(keysList.get(0), valuesList.get(0), uriList.get(0));
        PscConsumerPollMessageIterator<String, String> messages2 = getTestMessages(keysList.get(1), valuesList.get(1), uriList.get(1));
        PscConsumerPollMessageIterator<String, String> messages2Cp = getTestMessages(keysList.get(1), valuesList.get(1), uriList.get(1));

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri topicUri2 = TopicUri.validate(testTopic2);

        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);
        TopicUri testTopicUri2 = TestTopicUri.validate(topicUri2);

        PscKafkaConsumer<String, String> backendConsumer1 = mock(PscKafkaConsumer.class);
        when(backendConsumer1.subscription()).thenReturn(Collections.singleton(testTopicUri1));
        when(backendConsumer1.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages1);

        PscKafkaConsumer<String, String> backendConsumer2 = mock(PscKafkaConsumer.class);
        when(backendConsumer2.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic2))));
        when(backendConsumer2.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages2);

        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.validateBackendTopicUri(topicUri2)).thenReturn(testTopicUri2);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean())).thenReturn(Sets.newHashSet(backendConsumer1, backendConsumer2));

        Set<String> topics = Sets.newHashSet(testTopic1, testTopic2);
        pscConsumer.subscribe(Sets.newHashSet(testTopic1, testTopic2));

        assertEquals(topics, pscConsumer.subscription());

        verifyConsumerPollResult(pscConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)), messages1Cp, messages2Cp);

        pscConsumer.unsubscribe();
        verify(creatorManager, times(1)).reset();
        verify(backendConsumer1, times(1)).unsubscribe();
        verify(backendConsumer2, times(1)).unsubscribe();
        verify(backendConsumer1, times(1)).close();
        verify(backendConsumer2, times(1)).close();

        pscConsumer.close();
        verify(creatorManager, times(2)).reset();
    }

    @Test
    @MockitoSettings(strictness = Strictness.LENIENT)
    @SuppressWarnings("unchecked")
    void testResubscribe() throws Exception {
        PscConsumerPollMessageIterator<String, String> messages1 = getTestMessages(keysList.get(0), valuesList.get(0), uriList.get(0));
        PscConsumerPollMessageIterator<String, String> messages1Cp = getTestMessages(keysList.get(0), valuesList.get(0), uriList.get(0));
        PscConsumerPollMessageIterator<String, String> messages2 = getTestMessages(keysList.get(1), valuesList.get(1), uriList.get(1));

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri topicUri2 = TopicUri.validate(testTopic2);

        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);
        TopicUri testTopicUri2 = TestTopicUri.validate(topicUri2);

        PscKafkaConsumer<String, String> backendConsumer1 = mock(PscKafkaConsumer.class);
        when(backendConsumer1.subscription()).thenReturn(Collections.singleton(testTopicUri1));
        when(backendConsumer1.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages1);

        PscKafkaConsumer<String, String> backendConsumer2 = mock(PscKafkaConsumer.class);
        when(backendConsumer2.subscription()).thenReturn(Collections.singleton(testTopicUri2));
        when(backendConsumer2.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages2);

        Set<String> topics = Sets.newHashSet(testTopic1, testTopic2);

        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.validateBackendTopicUri(topicUri2)).thenReturn(testTopicUri2);

        when(creator.getConsumers(any(), any(), any(), eq(Sets.newHashSet(testTopicUri1, testTopicUri2)), any(), anyBoolean(), anyBoolean())).thenReturn(
                Sets.newHashSet(backendConsumer1, backendConsumer2));
        when(creator.getConsumers(any(), any(), any(), eq(Collections.singleton(testTopicUri1)), any(), anyBoolean(), anyBoolean())).thenReturn(
                Collections.singleton(backendConsumer1));
        pscConsumer.subscribe(Sets.newHashSet(testTopic1, testTopic2));

        assertEquals(topics, pscConsumer.subscription());

        // resubscribe
        pscConsumer.subscribe(Collections.singleton(testTopic1));
        assertEquals(Collections.singleton(testTopic1), pscConsumer.subscription());

        verifyConsumerPollResult(pscConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)), messages1Cp);

        // equivalent to unsubscribe
        pscConsumer.subscribe(Collections.emptySet());
        verify(creatorManager, times(1)).reset();
        verify(backendConsumer1, times(1)).unsubscribe();
        verify(backendConsumer2, times(1)).unsubscribe();
        verify(backendConsumer1, times(1)).close();
        verify(backendConsumer2, times(1)).close();
        assertEquals(Collections.emptySet(), pscConsumer.subscription());

        pscConsumer.close();
        verify(creatorManager, times(2)).reset();
    }

    @Test
    void testBadBackend() throws TopicUriSyntaxException, ConsumerException {
        String topicUriAsString = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":badbackend:env:cloud_region::cluster:topic";
        TopicUri topicUri = TopicUri.validate(topicUriAsString);
        Exception e = assertThrows(ConsumerException.class,
                () -> pscConsumer.subscribe(Collections.singleton(topicUriAsString))
        );
        pscConsumer.close();
        assertEquals(ExceptionMessage.TOPIC_URI_UNSUPPORTED_BACKEND(topicUri.getBackend()), e.getMessage());
    }

    @Test
    @MockitoSettings(strictness = Strictness.LENIENT)
    void testPollBeforeSubscribe() throws ConsumerException {
        Exception e = assertThrows(ConsumerException.class, pscConsumer::poll);
        pscConsumer.close();
        assertEquals(ExceptionMessage.NO_SUBSCRIPTION_ASSIGNMENT("poll()"), e.getMessage());
    }

    @Test
    void testSeekErrorScenarios() throws Exception {
        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);
        TopicUriPartition topicUriPartition1 = new TopicUriPartition(testTopic1, 0);
        BaseTopicUri.finalizeTopicUriPartition(topicUriPartition1, topicUri1);
        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);

        pscConsumer.subscribe(Collections.singleton(testTopic1));
        Set<MessageId> messageIds = new HashSet<>();
        messageIds.add(new MessageId(TestUtils.getFinalizedTopicUriPartition(topicUri1, 0), 10));
        messageIds.add(new MessageId(TestUtils.getFinalizedTopicUriPartition(topicUri1, 1), 100));
        messageIds.add(new MessageId(TestUtils.getFinalizedTopicUriPartition(topicUri1, 0), 1000));
        Exception e = assertThrows(ConsumerException.class, () -> pscConsumer.seek(messageIds));
        pscConsumer.close();
        assertEquals(
                ExceptionMessage.DUPLICATE_PARTITIONS_IN_MESSAGE_IDS(Collections.singleton(topicUriPartition1)),
                e.getMessage()
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void testGetPartitionsNonExistingTopic() throws Exception {
        String nonExistingTopicUriString = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:not_there";
        TopicUri nonExistingTopicUri = TopicUri.validate(nonExistingTopicUriString);
        TopicUri nonExistingTestTopicUri = TestTopicUri.validate(nonExistingTopicUri);

        PscKafkaConsumer<String, String> backendConsumer = mock(PscKafkaConsumer.class);
        when(creator.validateBackendTopicUri(nonExistingTopicUri)).thenReturn(nonExistingTestTopicUri);
        when(creator.getConsumer(any(), any(), any(), any(), anyBoolean(), anyBoolean())).thenReturn(backendConsumer);
        when(backendConsumer.getPartitions(nonExistingTestTopicUri)).thenReturn(null);

        assertNull(pscConsumer.getPartitions(nonExistingTopicUriString));
        pscConsumer.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAssignmentAndSubscribeErrorScenarios() throws TopicUriSyntaxException, ConfigurationException, ConsumerException {
        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);

        PscKafkaConsumer<String, String> backendConsumer = mock(PscKafkaConsumer.class);
        when(backendConsumer.subscription()).thenReturn(Collections.singleton(testTopicUri1));

        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean())).thenReturn(Collections.singleton(backendConsumer));

        Set<String> topics = Collections.singleton(testTopic1);
        Set<TopicUriPartition> topicUriPartitions = Collections.singleton(new TopicUriPartition(testTopic1));

        // test that psc consumer cannot be both subscribed and assigned at the same time
        pscConsumer.assign(topicUriPartitions); // no error
        assertThrows(ConsumerException.class, () -> pscConsumer.subscribe(topics));
        pscConsumer.unassign();
        pscConsumer.subscribe(topics);  // no error
        assertThrows(ConsumerException.class, () -> pscConsumer.assign(topicUriPartitions));
        pscConsumer.unsubscribe();
        pscConsumer.assign(topicUriPartitions); // no error
        pscConsumer.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testGetAssignment() throws ConsumerException, ConfigurationException, TopicUriSyntaxException {
        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);
        TopicUri topicUri2 = TopicUri.validate(testTopic2);
        TopicUri testTopicUri2 = TestTopicUri.validate(topicUri2);
        TopicUri topicUri3 = TopicUri.validate(testTopic3);
        TopicUri testTopicUri3 = TestTopicUri.validate(topicUri3);

        TopicUriPartition[] topicUriPartitionsList = new TopicUriPartition[]{
                new TopicUriPartition(testTopic1),
                new TopicUriPartition(testTopic2),
                new TopicUriPartition(testTopic3)
        };
        Set<TopicUriPartition> topicUriPartitions = new HashSet<>(
                Arrays.asList(topicUriPartitionsList));

        PscKafkaConsumer<String, String> backendConsumer0 = mock(PscKafkaConsumer.class);
        when(backendConsumer0.assignment()).thenReturn(Collections.singleton(topicUriPartitionsList[0]));
        PscKafkaConsumer<String, String> backendConsumer1 = mock(PscKafkaConsumer.class);
        when(backendConsumer1.assignment()).thenReturn(Collections.singleton(topicUriPartitionsList[1]));
        PscKafkaConsumer<String, String> backendConsumer2 = mock(PscKafkaConsumer.class);
        when(backendConsumer2.assignment()).thenReturn(Collections.singleton(topicUriPartitionsList[2]));

        when(creator.getAssignmentConsumers(any(), any(), any(), any(), anyBoolean(), anyBoolean())).thenReturn(
                new HashSet<>(Arrays.asList(
                        backendConsumer0, backendConsumer1, backendConsumer2
                )));
        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.validateBackendTopicUri(topicUri2)).thenReturn(testTopicUri2);
        when(creator.validateBackendTopicUri(topicUri3)).thenReturn(testTopicUri3);

        pscConsumer.assign(topicUriPartitions);

        assertTrue(pscConsumer.assignment().equals(topicUriPartitions));
        pscConsumer.close();
    }
}