package com.pinterest.psc.interceptor;

import com.codahale.metrics.Snapshot;
import com.pinterest.psc.common.PscMessage;
import com.pinterest.psc.common.TestTopicUri;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.MetricsReporterConfiguration;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.consumer.PscConsumerUtils;
import com.pinterest.psc.consumer.TestPscConsumerBase;
import com.pinterest.psc.consumer.kafka.PscKafkaConsumer;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.consumer.DeserializerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metrics.MetricsUtils;
import com.pinterest.psc.metrics.NullMetricsReporter;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetricTagManager;
import com.pinterest.psc.metrics.PscMetrics;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@MockitoSettings(strictness = Strictness.LENIENT)
public class TestPscConsumerInterceptor extends TestPscConsumerBase {
    private Interceptors<String, String> interceptors;

    @BeforeEach
    void init() {
        when(pscConfigurationInternal.getClientType()).thenReturn(PscConfiguration.PSC_CLIENT_TYPE_CONSUMER);
        when(pscConfigurationInternal.getMetricsReporterConfiguration()).thenReturn(new MetricsReporterConfiguration(
                true, NullMetricsReporter.class.getName(), 1, "localhost",-1, 10));
    }

    @AfterEach
    void cleanup() {
        MetricsUtils.resetMetrics(pscMetricRegistryManager);
    }

    /**
     * Tests whether a single interceptor gets executed
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("unchecked")
    void testSimpleInterceptors() throws Exception {
        initializePscConsumerWithInterceptors(
                Collections.EMPTY_LIST,
                Collections.singletonList(IdentityInterceptor.class.getName())
        );
        IdentityInterceptor<String, String> interceptor = (IdentityInterceptor) interceptors.getTypedDataInterceptors()
                .get(0);

        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0), interceptors);
        PscConsumerPollMessageIterator<String, String> messagesCp = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0));
        PscConsumerPollMessageIterator<String, String> messagesCp2 = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0), interceptors);

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);

        PscKafkaConsumer<String, String> backendConsumer = mock(PscKafkaConsumer.class);
        when(backendConsumer.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic1))));
        when(backendConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages);
        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn(Collections.singleton(backendConsumer));

        pscConsumer.subscribe(Collections.singleton(testTopic1));
        verifyConsumerPollResult(pscConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)), messagesCp);
        assertEquals(valuesList.get(0).length, interceptor.onConsumeCounter);

        when(creator.getAssignmentConsumer(any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn(backendConsumer);
        pscConsumer.commitSync(Collections.singleton(messagesCp2.next().getMessageId()));
        assertEquals(1, interceptor.onCommitCounter);

        pscConsumer.close();
        //assertEquals(1, interceptor.closeCounter);
    }

    /**
     * Tests whether multiple interceptors get executed
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("unchecked")
    void testInterceptorChain() throws Exception {
        initializePscConsumerWithInterceptors(
                Collections.singletonList(IdentityInterceptor.class.getName()),
                Collections.singletonList(IdentityInterceptor.class.getName())
        );
        IdentityInterceptor<byte[], byte[]> interceptor1 = (IdentityInterceptor) interceptors.getRawDataInterceptors()
                .get(0);
        IdentityInterceptor<String, String> interceptor2 = (IdentityInterceptor) interceptors.getTypedDataInterceptors()
                .get(0);

        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0), interceptors);
        PscConsumerPollMessageIterator<String, String> messagesCp = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0));
        PscConsumerPollMessageIterator<String, String> messagesCp2 = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0));

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);

        PscKafkaConsumer<String, String> backendConsumer = mock(PscKafkaConsumer.class);
        when(backendConsumer.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic1))));
        when(backendConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages);
        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn(Collections.singleton(backendConsumer));

        //assertEquals(1, interceptor1.configureCounter);
        //assertEquals(1, interceptor2.configureCounter);

        pscConsumer.subscribe(Collections.singleton(testTopic1));
        verifyConsumerPollResult(pscConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)), messagesCp);
        assertEquals(valuesList.get(0).length, interceptor1.onConsumeCounter);
        assertEquals(valuesList.get(0).length, interceptor2.onConsumeCounter);

        when(creator.getAssignmentConsumer(any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn(backendConsumer);
        pscConsumer.commitSync(Collections.singleton(messagesCp2.next().getMessageId()));
        assertEquals(1, interceptor1.onCommitCounter);
        assertEquals(1, interceptor2.onCommitCounter);

        pscConsumer.close();
        //assertEquals(1, interceptor1.closeCounter);
        //assertEquals(1, interceptor2.closeCounter);
    }

    /**
     * Test if a single interceptor failure are transparent
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("unchecked")
    void testSingleInterceptorError() throws Exception {
        initializePscConsumerWithInterceptors(
                Collections.EMPTY_LIST,
                Collections.singletonList(ExceptionalInterceptor.class.getName())
        );
        IdentityInterceptor<String, String> interceptor = (IdentityInterceptor) interceptors.getTypedDataInterceptors()
                .get(0);

        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0), interceptors);
        PscConsumerPollMessageIterator<String, String> messagesCp = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0));
        PscConsumerPollMessageIterator<String, String> messagesCp2 = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0));

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);

        PscKafkaConsumer<String, String> backendConsumer = mock(PscKafkaConsumer.class);
        when(backendConsumer.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic1))));
        when(backendConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages);
        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn(Collections.singleton(backendConsumer));

        //assertEquals(1, interceptor.configureCounter);

        pscConsumer.subscribe(Collections.singleton(testTopic1));
        verifyConsumerPollResult(pscConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)), messagesCp);

        when(creator.getAssignmentConsumer(any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn(backendConsumer);
        pscConsumer.commitSync(Collections.singleton(messagesCp2.next().getMessageId()));
        assertEquals(1, interceptor.onCommitCounter);

        pscConsumer.close();
        //assertEquals(1, interceptor.closeCounter);
    }

    /**
     * Test whether errors are handler properly when there are multiple interceptors
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("unchecked")
    void testInterceptorChainError() throws Exception {
        initializePscConsumerWithInterceptors(
                Collections.singletonList(ExceptionalInterceptor.class.getName()),
                Collections.singletonList(IdentityInterceptor.class.getName())
        );
        IdentityInterceptor<byte[], byte[]> badInterceptor = (IdentityInterceptor) interceptors.getRawDataInterceptors()
                .get(0);
        IdentityInterceptor<String, String> goodInterceptor = (IdentityInterceptor) interceptors
                .getTypedDataInterceptors().get(0);

        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0), interceptors);
        PscConsumerPollMessageIterator<String, String> messagesCp = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0));
        PscConsumerPollMessageIterator<String, String> messagesCp2 = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0));

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);

        PscKafkaConsumer<String, String> backendConsumer = mock(PscKafkaConsumer.class);
        when(backendConsumer.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic1))));
        when(backendConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages);
        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn(Collections.singleton(backendConsumer));

        //assertEquals(1, badInterceptor.configureCounter);
        //assertEquals(1, goodInterceptor.configureCounter);

        pscConsumer.subscribe(Collections.singleton(testTopic1));
        verifyConsumerPollResult(pscConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)), messagesCp);
        assertEquals(valuesList.get(0).length,
                badInterceptor.onConsumeCounter); // two list are being traversed for comparison
        assertEquals(valuesList.get(0).length,
                goodInterceptor.onConsumeCounter); // two list are being traversed for comparison

        when(creator.getAssignmentConsumer(any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn(backendConsumer);
        pscConsumer.commitSync(Collections.singleton(messagesCp2.next().getMessageId()));
        assertEquals(1, badInterceptor.onCommitCounter);
        assertEquals(1, goodInterceptor.onCommitCounter);

        pscConsumer.close();
        //assertEquals(1, badInterceptor.closeCounter);
        //assertEquals(1, goodInterceptor.closeCounter);
    }

    /**
     * Test if records are modified and returned properly for a single interceptor (3 messages -> 1 message)
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("unchecked")
    void testSingleInterceptorModifyRecords() throws Exception {
        initializePscConsumerWithInterceptors(
                Collections.EMPTY_LIST,
                Collections.singletonList(ModifierInterceptor.class.getName())
        );

        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0), interceptors);
        PscConsumerPollMessageIterator<String, String> messagesCp = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0));

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);

        PscKafkaConsumer<String, String> backendConsumer = mock(PscKafkaConsumer.class);
        when(backendConsumer.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic1))));
        when(backendConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages);
        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn(Collections.singleton(backendConsumer));

        pscConsumer.subscribe(Collections.singleton(testTopic1));
        verifyConsumerPollResult(pscConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)), messagesCp);
        pscConsumer.close();
    }

    /**
     * Test if records are modified and returned properly for multiple interceptors (3 messages -> 1 message)
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("unchecked")
    void testInterceptorChainModifyRecordsWithErrors() throws Exception {
        initializePscConsumerWithInterceptors(
                Collections.singletonList(IdentityInterceptor.class.getName()),
                Arrays.asList(ModifierInterceptor.class.getName(), ExceptionalInterceptor.class.getName())
        );
        ModifierInterceptor<String, String> modifyInterceptor = (ModifierInterceptor) interceptors
                .getTypedDataInterceptors().get(0);
        ExceptionalInterceptor<String, String> badInterceptor = (ExceptionalInterceptor) interceptors
                .getTypedDataInterceptors().get(1);
        IdentityInterceptor<byte[], byte[]> goodInterceptor = (IdentityInterceptor) interceptors
                .getRawDataInterceptors().get(0);

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);

        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0), interceptors);
        PscConsumerPollMessageIterator<String, String> messagesCp = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0));

        PscKafkaConsumer<String, String> backendConsumer = mock(PscKafkaConsumer.class);
        when(backendConsumer.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic1))));
        when(backendConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages);
        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn(Collections.singleton(backendConsumer));

        pscConsumer.subscribe(Collections.singleton(testTopic1));
        verifyConsumerPollResult(pscConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)), messagesCp);

        assertEquals(valuesList.get(0).length, modifyInterceptor.onConsumeCounter);
        assertEquals(valuesList.get(0).length, badInterceptor.onConsumeCounter);
        assertEquals(valuesList.get(0).length, goodInterceptor.onConsumeCounter);

        pscConsumer.close();
    }

    /**
     * Test TimeLagInterceptor if records are not modified and reported lag metrics is within a range.
     * This interceptor is built-in.
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("unchecked")
    void testTimeLagConsumerInterceptor() throws Exception {
        initializePscConsumerWithInterceptors(Collections.EMPTY_LIST, Collections.EMPTY_LIST);

        long lag = 10000;
        long currentTs = System.currentTimeMillis();
        Supplier<Header[]> headerSupplier = () -> {
            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
            buf.putLong(currentTs - lag);
            Header header = new RecordHeader(PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP, buf.array());
            return new Header[]{header};
        };

        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0), headerSupplier);
        PscConsumerPollMessageIterator<String, String> messagesCp = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0), headerSupplier);

        validateTimeLagInterceptorMetrics(testTopic1, messages, messagesCp, lag, lag * 2, 2 * valuesList.get(0).length);
    }

    /**
     * Test TimeLagInterceptor if record headers are corrupt, shouldn't throw exception but shouldn't publish metrics.
     * This interceptor is built-in.
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("unchecked")
    void testTimeLagConsumerInterceptorCorruptedHeader() throws Exception {
        initializePscConsumerWithInterceptors(Collections.EMPTY_LIST, Collections.EMPTY_LIST);

        Supplier<Header[]> headerSupplier = () -> {
            ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
            buf.putInt(1);
            Header header = new RecordHeader(PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP, buf.array());
            return new Header[]{header};
        };

        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(1), valuesList.get(1),
                uriList.get(1), headerSupplier);
        PscConsumerPollMessageIterator<String, String> messagesCp = getTestMessages(keysList.get(1), valuesList.get(1),
                uriList.get(1), headerSupplier);

        validateTimeLagInterceptorMetrics(testTopic2, messages, messagesCp, 0, 0, 0);
    }

    /**
     * Test TimeLagInterceptor if only some headers are corrupt, shouldn't throw exception but should publish metrics
     * for messages with non-corrupt headers. This interceptor is built-in.
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("unchecked")
    void testTimeLagConsumerInterceptorSomeCorruptedHeaders() throws Exception {
        initializePscConsumerWithInterceptors(Collections.EMPTY_LIST, Collections.EMPTY_LIST);

        long lag = 10000;
        AtomicInteger i = new AtomicInteger(0);
        long currentTs = System.currentTimeMillis();
        Supplier<Header[]> headerSupplier = () -> {
            if (i.getAndIncrement() == 0) {
                ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
                buf.putLong(currentTs - lag);
                Header header = new RecordHeader(PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP, buf.array());
                return new Header[]{header};
            } else {
                ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
                buf.putInt(1);  // put int instead of long to simulate corruption for the rest of the messages
                Header header = new RecordHeader(PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP, buf.array());
                return new Header[]{header};
            }
        };

        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(2), valuesList.get(2),
                uriList.get(2), headerSupplier);
        i.set(0);
        PscConsumerPollMessageIterator<String, String> messagesCp = getTestMessages(keysList.get(2), valuesList.get(2),
                uriList.get(2), headerSupplier);

        validateTimeLagInterceptorMetrics(testTopic3, messages, messagesCp, lag, lag * 2, 2 * 1);
    }

    /**
     * Test TimeLagInterceptor with no PSC-injected headers. TimeLagInterceptor should use publishTimestamp field
     * instead. This interceptor is built-in.
     *
     * @throws Exception
     */
    @Test
    void testTimeLagConsumerInterceptorWithNoHeadersAndPublishTimestampInMillis() throws Exception {
        testTimeLagConsumerInterceptorWithNoHeaders(MILLISECONDS);
    }

    /**
     * Test TimeLagInterceptor with no PSC-injected headers where publishTimestamp is in microseconds.
     * TimeLagInterceptor should use publishTimestamp field instead and do proper unit conversion. This interceptor is
     * built-in.
     *
     * @throws Exception
     */
    @Test
    void testTimeLagConsumerInterceptorWithNoHeadersAndPublishTimestampInMicros() throws Exception {
        testTimeLagConsumerInterceptorWithNoHeaders(MICROSECONDS);
    }

    /**
     * Test TimeLagInterceptor with no PSC-injected headers where publishTimestamp is in nanoseconds. TimeLagInterceptor
     * should use publishTimestamp field instead and do proper unit conversion. This interceptor is built-in.
     *
     * @throws Exception
     */
    @Test
    void testTimeLagConsumerInterceptorWithNoHeadersAndPublishTimestampInNanos() throws Exception {
        testTimeLagConsumerInterceptorWithNoHeaders(NANOSECONDS);
    }

    private void testTimeLagConsumerInterceptorWithNoHeaders(TimeUnit timeUnit) throws Exception {
        initializePscConsumerWithInterceptors(Collections.EMPTY_LIST, Collections.EMPTY_LIST);

        long lagMs = 10_000;
        Instant instant = Instant.now();
        long coefficient = -1;
        long currentTsInTimeUnit = -1;
        switch (timeUnit) {
            case NANOSECONDS:
                coefficient = 1;
                currentTsInTimeUnit = SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano() / coefficient;
                break;
            case MICROSECONDS:
                coefficient = 1_000;
                currentTsInTimeUnit = SECONDS.toMicros(instant.getEpochSecond()) + instant.getNano() / coefficient;
                break;
            case MILLISECONDS:
                coefficient = 1_000_000;
                currentTsInTimeUnit = SECONDS.toMillis(instant.getEpochSecond()) + instant.getNano() / coefficient;
                break;
            default:
                fail();
        }

        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0), currentTsInTimeUnit - lagMs * (1_000_000 / coefficient),
                null);
        PscConsumerPollMessageIterator<String, String> messagesCp = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0), currentTsInTimeUnit - lagMs * (1_000_000 / coefficient),
                null);

        validateTimeLagInterceptorMetrics(testTopic1, messages, messagesCp, lagMs, lagMs * 2, 2 * valuesList.get(0).length);
    }

    /**
     * Test TimeLagInterceptor with some PSC-injected headers. TimeLagInterceptor should use publishTimestamp field
     * instead for those without an injected timestamp header. This interceptor is built-in.
     *
     * @throws Exception
     */
    @Test
    void testTimeLagConsumerInterceptorWithSomeTimestampHeaders() throws Exception {
        initializePscConsumerWithInterceptors(Collections.EMPTY_LIST, Collections.EMPTY_LIST);

        long lag = 10000;
        AtomicInteger i = new AtomicInteger(0);
        long currentTs = System.currentTimeMillis();
        Supplier<Header[]> headerSupplier = () -> {
            if (i.getAndIncrement() == 0) {
                ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
                buf.putLong(currentTs - lag);
                Header header = new RecordHeader(PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP, buf.array());
                return new Header[]{header};
            } else {
                ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
                buf.putInt(1);  // put int instead of long to simulate corruption for the rest of the messages
                Header header = new RecordHeader("asdf", buf.array());  // header does not have the timestamp key
                return new Header[]{header};
            }
        };

        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(2), valuesList.get(2),
                uriList.get(2), currentTs - lag,
                headerSupplier);
        i.set(0);
        PscConsumerPollMessageIterator<String, String> messagesCp = getTestMessages(keysList.get(2), valuesList.get(2),
                uriList.get(2), currentTs - lag,
                headerSupplier);

        validateTimeLagInterceptorMetrics(testTopic3, messages, messagesCp, lag, lag * 2, 2 * 3);
    }

    @Test
    void testConsumerInterceptorWithErrorMetrics() throws Exception {
        initializePscConsumerWithInterceptors(Collections.EMPTY_LIST, Collections.singletonList(
                ExceptionalMetricsReportingInterceptor.class.getName()));

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);

        PscConsumerPollMessageIterator<String, String> messages = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0), interceptors);
        PscConsumerPollMessageIterator<String, String> messagesCp = getTestMessages(keysList.get(0), valuesList.get(0),
                uriList.get(0));

        PscKafkaConsumer<String, String> backendConsumer = mock(PscKafkaConsumer.class);
        when(backendConsumer.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(testTopic1))));
        when(backendConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages);
        when(creator.validateBackendTopicUri(topicUri1)).thenReturn(testTopicUri1);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn(Collections.singleton(backendConsumer));

        pscConsumer.subscribe(Collections.singleton(testTopic1));
        verifyConsumerPollResult(pscConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)), messagesCp);

        long counter = pscMetricRegistryManager.getCounterMetric(testTopicUri1, new DeserializerException("").getMetricName(), pscConfigurationInternal);
        assertEquals(keysList.size(), counter);
    }

    @Deprecated
    @Test
    void testMultivaluedInterceptorConfigsDeprecated() throws ConfigurationException, ConsumerException {
        Configuration configuration = new PropertiesConfiguration();
        configuration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "client-id");
        configuration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "group-id");
        configuration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, keyDeserializerClass);
        configuration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, valueDeserializerClass);
        configuration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, metricsReporterClass);
        configuration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");

        IdentityInterceptor rawIdentityInterceptor = new IdentityInterceptor();
        ModifierInterceptor typedModifierInterceptor = new ModifierInterceptor();

        configuration.setProperty(
                PscConfiguration.PSC_CONSUMER_INTERCEPTORS_RAW_CLASSES,
                Arrays.asList(rawIdentityInterceptor, ModifierInterceptor.class.getName())
        );
        configuration.setProperty(
                PscConfiguration.PSC_CONSUMER_INTERCEPTORS_TYPED_CLASSES,
                Arrays.asList(IdentityInterceptor.class.getName(), typedModifierInterceptor)
        );
        pscConsumer = new PscConsumer<>(configuration);

        assertEquals(2, pscConsumer.getRawInterceptors().size());
        assertEquals(rawIdentityInterceptor, pscConsumer.getRawInterceptors().get(0));
        assertEquals(ModifierInterceptor.class, pscConsumer.getRawInterceptors().get(1).getClass());
        assertEquals(2, pscConsumer.getTypedInterceptors().size());
        assertEquals(IdentityInterceptor.class, pscConsumer.getTypedInterceptors().get(0).getClass());
        assertEquals(typedModifierInterceptor, pscConsumer.getTypedInterceptors().get(1));
    }

    @Test
    void testMultivaluedInterceptorConfigs() throws ConfigurationException, ConsumerException {
        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "client-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "group-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, keyDeserializerClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, valueDeserializerClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, metricsReporterClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");

        IdentityInterceptor rawIdentityInterceptor = new IdentityInterceptor();
        ModifierInterceptor typedModifierInterceptor = new ModifierInterceptor();

        pscConfiguration.setProperty(
                PscConfiguration.PSC_CONSUMER_INTERCEPTORS_RAW_CLASSES,
                Arrays.asList(rawIdentityInterceptor, ModifierInterceptor.class.getName())
        );
        pscConfiguration.setProperty(
                PscConfiguration.PSC_CONSUMER_INTERCEPTORS_TYPED_CLASSES,
                Arrays.asList(IdentityInterceptor.class.getName(), typedModifierInterceptor)
        );
        pscConsumer = new PscConsumer<>(pscConfiguration);

        assertEquals(2, pscConsumer.getRawInterceptors().size());
        assertEquals(rawIdentityInterceptor, pscConsumer.getRawInterceptors().get(0));
        assertEquals(ModifierInterceptor.class, pscConsumer.getRawInterceptors().get(1).getClass());
        assertEquals(2, pscConsumer.getTypedInterceptors().size());
        assertEquals(IdentityInterceptor.class, pscConsumer.getTypedInterceptors().get(0).getClass());
        assertEquals(typedModifierInterceptor, pscConsumer.getTypedInterceptors().get(1));
    }

    private void validateTimeLagInterceptorMetrics(String topic,
                                                   PscConsumerPollMessageIterator<String, String> messages,
                                                   PscConsumerPollMessageIterator<String, String> messagesCp,
                                                   long computedLagLowerBound,
                                                   long computedLagUpperBound,
                                                   int numExpectedMetrics)
            throws ConsumerException, ConfigurationException, TopicUriSyntaxException {
        TopicUri topicUri = TopicUri.validate(topic);
        TopicUri testTopicUri = TestTopicUri.validate(topicUri);

        PscKafkaConsumer<String, String> backendConsumer = mock(PscKafkaConsumer.class);
        when(backendConsumer.subscription()).thenReturn(
                Collections.singleton(TestTopicUri.validate(TopicUri.validate(topic))));
        when(backendConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs))).thenReturn(messages);
        when(creator.validateBackendTopicUri(topicUri)).thenReturn(testTopicUri);
        when(creator.getConsumers(any(), any(), any(), any(), any(), anyBoolean(), anyBoolean())).thenReturn(
                Collections.singleton(backendConsumer));

        pscConsumer.subscribe(Collections.singleton(topic));
        verifyConsumerPollResult(pscConsumer.poll(Duration.ofMillis(defaultPollTimeoutMs)), messagesCp);

        validateTimeLagMetrics(topic, computedLagLowerBound, computedLagUpperBound, numExpectedMetrics);
        pscConsumer.close();
    }

    private void validateTimeLagMetrics(String topic, long lowerBound, long upperBound, int numExpectedMetrics)
            throws TopicUriSyntaxException {
        Snapshot metricSnapshot = pscMetricRegistryManager.getHistogramMetric(
                TopicUri.validate(topic),
                0,
                PscMetrics.PSC_CONSUMER_TIME_LAG_MS_METRIC,
                pscConfigurationInternal
        );

        assertNotNull(metricSnapshot);
        double meanComputedLag = metricSnapshot.getMean();
        assertTrue(meanComputedLag >= lowerBound);
        assertTrue(meanComputedLag <= upperBound);
        assertEquals(numExpectedMetrics, metricSnapshot.getValues().length);
    }

    @SuppressWarnings("unchecked")
    private void initializePscConsumerWithInterceptors(
            List<String> rawInterceptorClasses, List<String> typedInterceptorClasses) throws Exception {
        when(creatorManager.getBackendCreators()).thenReturn(Collections.singletonMap("test", creator));
        when(pscConfigurationInternal.getEnvironment()).thenReturn(environment);
        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "client-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "group-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, keyDeserializerClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, valueDeserializerClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, metricsReporterClass);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        pscConfiguration.setProperty(
                PscConfiguration.PSC_CONSUMER_INTERCEPTORS_RAW_CLASSES,
                String.join(",", rawInterceptorClasses)
        );
        pscConfiguration.setProperty(
                PscConfiguration.PSC_CONSUMER_INTERCEPTORS_TYPED_CLASSES,
                String.join(",", typedInterceptorClasses)
        );

        when(pscConfigurationInternal.getConfiguration()).thenReturn(pscConfiguration);
        pscConsumer = new PscConsumer<>(pscConfiguration);

        PscConsumerUtils.setCreatorManager(pscConsumer, creatorManager);
        pscMetricRegistryManager = PscMetricRegistryManager.getInstance();
        pscMetricTagManager = PscMetricTagManager.getInstance();
        pscMetricRegistryManager.setPscMetricTagManager(pscMetricTagManager);
        PscConsumerUtils.setPscMetricRegistryManager(pscConsumer, pscMetricRegistryManager);
        pscMetricTagManager.initializePscMetricTagManager(pscConfigurationInternal);

        interceptors = PscConsumerUtils.getInterceptors(pscConsumer);

        List<TypePreservingInterceptor<byte[], byte[]>> rawInterceptors = interceptors.getRawDataInterceptors();
        assertEquals(rawInterceptorClasses.size(), rawInterceptors.size());
        for (int i = 0; i < rawInterceptors.size(); ++i)
            assertEquals(rawInterceptorClasses.get(i), rawInterceptors.get(i).getClass().getName());

        List<TypePreservingInterceptor<String, String>> typedInterceptors = interceptors.getTypedDataInterceptors();
        assertEquals(typedInterceptorClasses.size(), typedInterceptors.size());
        for (int i = 0; i < typedInterceptors.size(); ++i)
            assertEquals(typedInterceptorClasses.get(i), typedInterceptors.get(i).getClass().getName());
    }

    @AfterEach
    void tearDown() {
        //MetricsUtils.resetMetrics(pscMetricRegistryManager);
    }
}
