package com.pinterest.psc.producer.kafka;

import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.config.PscProducerToBackendProducerConfigConverter;
import com.pinterest.psc.config.PscProducerToKafkaProducerConfigConverter;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metrics.NullMetricsReporter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestPscProducerToKafkaProducerConfigConverter {

    @Test
    void testSslConfigInjection() throws ConfigurationException, TopicUriSyntaxException {
        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "client-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        PscConfigurationInternal pscConfigurationInternal = new PscConfigurationInternal(pscConfiguration, PscConfiguration.PSC_CLIENT_TYPE_PRODUCER);
        TopicUri secureUri = TopicUri.validate("secure:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:topic");
        TopicUri plaintextUri = TopicUri.validate("plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:topic");
        PscProducerToBackendProducerConfigConverter converter = new PscProducerToKafkaProducerConfigConverter();

        // inject SSL security protocol for Kafka config
        Properties properties = converter.convert(pscConfigurationInternal, secureUri);
        assertEquals("SSL", properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertNull(properties.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));  // should not be injected

        // inject PLAINTEXT security protocol for Kafka config
        properties = converter.convert(pscConfigurationInternal, plaintextUri);
        assertEquals(CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL, properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }
}
