package com.pinterest.psc.exception;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TestTopicUri;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.MetricsReporterConfiguration;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.consumer.BackendConsumerException;
import com.pinterest.psc.exception.consumer.DeserializerException;
import com.pinterest.psc.exception.consumer.WakeupException;
import com.pinterest.psc.exception.handler.PscErrorHandler;
import com.pinterest.psc.exception.producer.SerializerException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metrics.MetricsUtils;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetricTagManager;
import com.pinterest.psc.metrics.PscMetricType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestPscErrorHandler {

    protected static final String testTopic1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic1";
    protected static final String testTopic2 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic2";
    protected static final String testTopic3 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic3";

    @Mock
    protected
    PscMetricRegistryManager pscMetricRegistryManager;

    @Mock
    protected
    PscMetricTagManager pscMetricTagManager;

    @Mock
    protected Environment environment;

    @Mock
    protected PscConfigurationInternal pscConfigurationInternal;

    protected String metricsReporterClass = TestUtils.DEFAULT_METRICS_REPORTER;

    @BeforeEach
    void init() {
        when(pscConfigurationInternal.getClientType()).thenReturn(PscConfiguration.PSC_CLIENT_TYPE_CONSUMER);
        pscMetricRegistryManager = PscMetricRegistryManager.getInstance();
        pscMetricTagManager = PscMetricTagManager.getInstance();
        pscMetricRegistryManager.setPscMetricTagManager(pscMetricTagManager);
        pscMetricTagManager.initializePscMetricTagManager(pscConfigurationInternal);
        when(pscConfigurationInternal.getClientType()).thenReturn(PscConfiguration.PSC_CLIENT_TYPE_CONSUMER);
        when(pscConfigurationInternal.getConfiguration()).thenReturn(new PscConfiguration());
        when(pscConfigurationInternal.getEnvironment()).thenReturn(environment);
        MetricsReporterConfiguration metricsReporterConfiguration = new MetricsReporterConfiguration(
                true, metricsReporterClass, 10, "host001", 9999, 30000
        );
        when(pscConfigurationInternal.getMetricsReporterConfiguration()).thenReturn(metricsReporterConfiguration);
        pscMetricRegistryManager.initialize(pscConfigurationInternal);
    }

    @AfterEach
    void cleanup() {
        MetricsUtils.resetMetrics(pscMetricRegistryManager);
        MetricsUtils.shutdownMetrics(pscMetricRegistryManager, pscConfigurationInternal);
    }

    @Test
    void testGeneralErrorMetricsHandling() throws TopicUriSyntaxException {
        PscException e1 = new DeserializerException("test exception 1");
        PscException e2 = new DeserializerException("test exception 2");
        PscException e3 = new DeserializerException("test exception 3");
        PscException e4 = new DeserializerException("test exception 4");
        PscException e5 = new SerializerException("test exception 5");
        PscException e6 = new WakeupException("test exception 6");
        PscException e7 = new DeserializerException("test exception 7");
        PscException e8 = new BackendConsumerException(new org.apache.kafka.common.errors.WakeupException(), PscUtils.BACKEND_TYPE_KAFKA);

        String deserExceptionMetricName = e1.getMetricName();
        String serializerExceptionMetricName = e5.getMetricName();

        TopicUri topicUri1 = TopicUri.validate(testTopic1);
        TopicUri testTopicUri1 = TestTopicUri.validate(topicUri1);

        PscErrorHandler.handle(e1, testTopicUri1, true, pscConfigurationInternal); // increments counter by 1
        long metric = pscMetricRegistryManager.getCounterMetric(testTopicUri1, deserExceptionMetricName, pscConfigurationInternal);
        assertEquals(1, metric);

        PscErrorHandler.handle(e3, testTopicUri1, true, pscConfigurationInternal);  // e3 should append to counter metrics for Deser exception
        metric = pscMetricRegistryManager.getCounterMetric(testTopicUri1, deserExceptionMetricName, pscConfigurationInternal);
        assertEquals(2, metric);

        PscErrorHandler.handle(e4, testTopicUri1, false, pscConfigurationInternal); // don't emit metrics
        metric = pscMetricRegistryManager.getCounterMetric(testTopicUri1, deserExceptionMetricName, pscConfigurationInternal);
        assertEquals(2, metric);

        PscErrorHandler.handle(e5, testTopicUri1, true, pscConfigurationInternal);   // emit metrics for SerializerException now
        metric = pscMetricRegistryManager.getCounterMetric(testTopicUri1, serializerExceptionMetricName, pscConfigurationInternal);
        assertEquals(1, metric);

        PscErrorHandler.handle(e6, testTopicUri1, 1, true, pscConfigurationInternal);  // test partition metrics
        metric = pscMetricRegistryManager
                .getCounterMetric(testTopicUri1, 1, e6.getMetricName(), pscConfigurationInternal);
        assertEquals(1, metric);

        PscErrorHandler.handle(
                e7,
                testTopicUri1,
                PscUtils.NO_PARTITION,
                e7.getMetricName(),
                PscMetricType.COUNTER,
                12L,
                true,
                pscConfigurationInternal,
                null
        );
        metric = pscMetricRegistryManager.getCounterMetric(testTopicUri1, deserExceptionMetricName, pscConfigurationInternal);
        assertEquals(14, metric); // add onto previous deser metrics

        PscErrorHandler.handle(null,
                null,
                -1,
                null,
                null,
                null,
                true,
                               pscConfigurationInternal,
                null); // sad path, should not error

        metric = pscMetricRegistryManager
                .getBackendCounterMetric(testTopicUri1, e8.getMetricName(), pscConfigurationInternal);
        assertEquals(0, metric);
        PscErrorHandler.handle(e8, testTopicUri1, true, pscConfigurationInternal);   // emit a backend consumer exception metric
        metric = pscMetricRegistryManager
                .getBackendCounterMetric(testTopicUri1, e8.getMetricName(), pscConfigurationInternal);
        assertEquals(1, metric);
    }
}
