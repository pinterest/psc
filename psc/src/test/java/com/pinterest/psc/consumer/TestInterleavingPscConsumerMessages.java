package com.pinterest.psc.consumer;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.kafka.KafkaToPscMessageIteratorConverter;
import com.pinterest.psc.exception.ExceptionMessage;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.interceptor.ConsumerInterceptors;
import com.pinterest.psc.serde.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class TestInterleavingPscConsumerMessages {
    private final StringDeserializer keyDeserializer = new StringDeserializer();
    private final StringDeserializer valueDeserializer = new StringDeserializer();

    @BeforeEach
    private void init() {
    }

    @Test
    void testEmptyMessages() {
        PscConsumerPollMessageIterator<String, String> zeroSize = new InterleavingPscConsumerPollMessages<>(new LinkedList<>());
        assertFalse(zeroSize.hasNext());
    }

    @Test
    void testInterleavingMessages() throws TopicUriSyntaxException, ConfigurationException {
        LinkedList<PscConsumerPollMessageIterator<String, String>> listOfMessages = new LinkedList<>();
        String[][] listOfValues = new String[][]{
                new String[]{"a", "b", "c", "d"},
                new String[]{"e", "f", "g"},
                new String[]{"h", "i"}
        };

        String[] interleavedResult = new String[]{"a", "e", "h", "b", "f", "i", "c", "g", "d"};

        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "client-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "group-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        pscConfiguration.setProperty(PscConfiguration.PSC_METRIC_REPORTING_ENABLED, "false");
        PscConfigurationInternal pscConfigurationInternal = new PscConfigurationInternal(pscConfiguration, PscConfiguration.PSC_CLIENT_TYPE_CONSUMER);
        ConsumerInterceptors<String, String> consumerInterceptors = new ConsumerInterceptors<>(
                null, pscConfigurationInternal, keyDeserializer, valueDeserializer
        );

        for (String[] values : listOfValues) {
            List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
            Map<String, TopicUri> topicToTopicUri = new HashMap<>();

            for (String value : values) {
                recordList.add(new ConsumerRecord<>(TestPscConsumerBase.testKafkaTopic1, 0, 0, null, value.getBytes()));
                topicToTopicUri.put(
                        TestPscConsumerBase.testKafkaTopic1,
                        KafkaTopicUri.validate(TopicUri.validate(TestPscConsumerBase.testKafkaTopic1))
                );
            }

            listOfMessages.addLast(new SimplePscConsumerPollMessages<>(
                    new KafkaToPscMessageIteratorConverter<>(
                            recordList.iterator(),
                            Collections.emptyMap(),
                            Collections.emptySet(),
                            topicToTopicUri,
                            consumerInterceptors
                    )
            ));
        }

        InterleavingPscConsumerPollMessages<String, String> itr = new InterleavingPscConsumerPollMessages<>(listOfMessages);
        for (String c : interleavedResult) {
            assertTrue(itr.hasNext());
            assertEquals(c, itr.next().getValue());
        }
        assertFalse(itr.hasNext());
        Exception e = assertThrows(NoSuchElementException.class, itr::next);
        assertEquals(ExceptionMessage.ITERATOR_OUT_OF_ELEMENTS, e.getMessage());
    }

    @Test
    public void testCloseMessage() {
        List<PscConsumerPollMessageIterator<byte[], byte[]>> listOfMessages = new ArrayList<>();

        TestIterator tmp = new TestIterator();
        listOfMessages.add(tmp);
        InterleavingPscConsumerPollMessages<byte[], byte[]> itr = new InterleavingPscConsumerPollMessages<>(listOfMessages);

        try {
            assertFalse(tmp.isClosed());
            itr.close();
            assertTrue(tmp.isClosed());
        } catch (Exception e) {
            fail(e);
        }

        TestIterator failIterator = new TestIterator(true);
        listOfMessages.clear();
        listOfMessages.add(failIterator);

        itr = new InterleavingPscConsumerPollMessages<>(listOfMessages);

        try {
            itr.close();
        } catch (Exception e) {
            fail(e);
        }

    }

    private static class TestIterator extends PscConsumerPollMessageIterator<byte[], byte[]> {
        private boolean closed = false;
        private boolean failClosed = false;

        public boolean isClosed() {
            return closed;
        }

        TestIterator() {
        }

        TestIterator(boolean failClosed) {
            this.failClosed = failClosed;
        }

        @Override
        public PscConsumerPollMessageIterator<byte[], byte[]> iteratorFor(
                TopicUriPartition topicUriPartition) {
            return null;
        }

        @Override
        public Set<TopicUriPartition> getTopicUriPartitions() {
            return Collections.emptySet();
        }

        @Override
        public void close() throws IOException {
            if (failClosed) {
                throw new IOException();
            }
            closed = true;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public PscConsumerMessage<byte[], byte[]> next() {
            return null;
        }
    }
}