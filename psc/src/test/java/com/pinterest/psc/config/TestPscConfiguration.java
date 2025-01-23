package com.pinterest.psc.config;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.interceptor.TypePreservingInterceptor;
import com.pinterest.psc.metrics.NullMetricsReporter;
import com.pinterest.psc.metrics.OpenTSDBReporter;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.serde.StringSerializer;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPscConfiguration {

    @Test
    void testConfigValidation() throws ConfigurationException {
        // good configs, should not throw exception
        PscConfiguration configuration1 = new PscConfiguration();
        configuration1.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "client-id");
        configuration1.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "group-id");
        configuration1.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        configuration1.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        configuration1.setProperty(PscConfiguration.PSC_METRICS_REPORTER_PARALLELISM, "5");
        PscConfigurationInternal consumerConfig = new PscConfigurationInternal(configuration1, PscConfigurationInternal.PSC_CLIENT_TYPE_CONSUMER);
        PscConfiguration configuration2 = new PscConfiguration();
        configuration2.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "client-id");
        configuration2.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        configuration2.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        PscConfigurationInternal producerConfig = new PscConfigurationInternal(configuration2, PscConfigurationInternal.PSC_CLIENT_TYPE_PRODUCER);

        // bad client type
        assertThrows(ConfigurationException.class, () -> new PscConfigurationInternal(configuration1, "bad_client_type"));

        Set<String> additionalRequiredConfigs = PscConfigurationInternal.getAdditionalRequiredConfigs();
        additionalRequiredConfigs.forEach(c -> setConfigValue(consumerConfig.getConfiguration(), c, ""));
        if (!additionalRequiredConfigs.isEmpty()) {
            // remove additionalRequiredConfigs, should throw exception upon validate
            assertThrows(ConfigurationException.class, consumerConfig::validate);
        } else {
            // should not throw exception if there are no additional required configs
            consumerConfig.validate();
        }

        assertEquals(5, consumerConfig.getConfiguration().getInt(PscConfiguration.PSC_METRICS_REPORTER_PARALLELISM));
        assertEquals(10, producerConfig.getConfiguration().getInt(PscConfiguration.PSC_METRICS_REPORTER_PARALLELISM));

        // clear required property for producer
        clearProperty(producerConfig.getConfiguration(), PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER);
        assertThrows(ConfigurationException.class, producerConfig::validate);
    }

    /**
     * Verifies that environment dependent configurations are initialized properly. These configurations include metric
     * reporter class, and config logging enabled.
     *
     * @throws ConfigurationException
     */
    @Test
    void testEnvironmentBasedConfigurationDefaults() throws ConfigurationException {
        // check when they're set (1)
        PscConfiguration configuration = new PscConfiguration();
        configuration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "client-id1");
        configuration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "group-id");
        configuration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, OpenTSDBReporter.class.getName());
        configuration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        PscConfigurationInternal consumerConfig = new PscConfigurationInternal(configuration, PscConfigurationInternal.PSC_CLIENT_TYPE_CONSUMER);
        consumerConfig.validate();
        assertEquals(OpenTSDBReporter.class.getName(), consumerConfig.getPscMetricsReporterClass());
        assertFalse(consumerConfig.isConfigLoggingEnabled());

        // check when they're set (2)
        // This would cause `ERROR Failed to emit PSC configs to config_topic_uri.` due to topic not being
        // valid nor actually exist anywhere, but shouldn't fail the test.
        configuration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "client-id2");
        configuration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        configuration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "true");
        configuration.setProperty(PscConfiguration.PSC_CONFIG_TOPIC_URI, "config_topic_uri");
        consumerConfig = new PscConfigurationInternal(configuration, PscConfigurationInternal.PSC_CLIENT_TYPE_CONSUMER);
        consumerConfig.validate();
        assertEquals(TestUtils.DEFAULT_METRICS_REPORTER, consumerConfig.getPscMetricsReporterClass());
        assertTrue(consumerConfig.isConfigLoggingEnabled());
        assertEquals("config_topic_uri", consumerConfig.getConfigLoggingTopicUri());

        // check when they're not set
        // On ec2 hosts this would cause `ERROR Failed to emit PSC configs to config_topic_uri.` due to topic not being
        // valid nor actually exist anywhere, but shouldn't fail the test.
        configuration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "client-id3");
        clearProperty(configuration, PscConfiguration.PSC_METRICS_REPORTER_CLASS);
        clearProperty(configuration, PscConfiguration.PSC_CONFIG_LOGGING_ENABLED);
        if (!PscUtils.isEc2Host())
            clearProperty(configuration, PscConfiguration.PSC_CONFIG_TOPIC_URI);
        consumerConfig = new PscConfigurationInternal(configuration, PscConfigurationInternal.PSC_CLIENT_TYPE_CONSUMER);
        consumerConfig.validate();
        assertEquals(
                PscUtils.isEc2Host() ? OpenTSDBReporter.class.getName() : TestUtils.DEFAULT_METRICS_REPORTER,
                consumerConfig.getPscMetricsReporterClass()
        );
        assertEquals(PscUtils.isEc2Host(), consumerConfig.isConfigLoggingEnabled());
        assertEquals(PscUtils.isEc2Host() ? "config_topic_uri" : null, consumerConfig.getConfigLoggingTopicUri());
    }

    @Test
    @Deprecated
    void testMultiValuesAndSpecialCharactersInConfigValuesDeprecated() throws ConfigurationException {
        String valueWithSpecialCharacters = "Source: kafka-flink-test-counter-source -> (kafka-flink-test-counter-mapper, Sink: kafka-flink-test-counter-sink)-4";
        Configuration configuration = new PropertiesConfiguration();
        configuration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, valueWithSpecialCharacters);
        configuration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        configuration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        configuration.setProperty(
                PscConfiguration.PSC_PRODUCER_INTERCEPTORS_TYPED_CLASSES,
                String.join(",",
                        Arrays.asList(ProducerTypedInterceptor1.class.getName(), ProducerTypedInterceptor2.class.getName())
                )
        );
        PscConfigurationInternal pscConfiguration = new PscConfigurationInternal(configuration, PscConfigurationInternal.PSC_CLIENT_TYPE_PRODUCER);

        assertEquals(valueWithSpecialCharacters, pscConfiguration.getPscProducerClientId());
        assertEquals(2, pscConfiguration.getTypedPscProducerInterceptors().size());
    }

    @Test
    void testMultiValuesAndSpecialCharactersInConfigValues() throws ConfigurationException {
        String valueWithSpecialCharacters = "Source: kafka-flink-test-counter-source -> (kafka-flink-test-counter-mapper, Sink: kafka-flink-test-counter-sink)-4";
        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, valueWithSpecialCharacters);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscConfiguration.setProperty(
                PscConfiguration.PSC_PRODUCER_INTERCEPTORS_TYPED_CLASSES,
                String.join(",",
                        Arrays.asList(ProducerTypedInterceptor1.class.getName(), ProducerTypedInterceptor2.class.getName())
                )
        );
        PscConfigurationInternal pscConfigurationInternal = new PscConfigurationInternal(pscConfiguration, PscConfigurationInternal.PSC_CLIENT_TYPE_PRODUCER);

        assertEquals(valueWithSpecialCharacters, pscConfigurationInternal.getPscProducerClientId());
        assertEquals(2, pscConfigurationInternal.getTypedPscProducerInterceptors().size());
    }

    @Test
    void testConvertedConfigNamesDontHonorOldNames() throws ConfigurationException {
        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "psc-consumer");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "psc-consumer-group");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);

        // PSC default for PSC_CONSUMER_BUFFER_RECEIVE_BYTES is 1 MB
        pscConfiguration.setProperty("psc.consumer.receive.buffer.bytes", "1024");
        // PSC default for PSC_CONSUMER_OFFSET_AUTO_RESET is null (to enforce the default value from backend)
        pscConfiguration.setProperty("psc.consumer.auto.offset.reset", "earliest");

        PscConfigurationInternal pscConfigurationInternal = new PscConfigurationInternal(pscConfiguration, PscConfigurationInternal.PSC_CLIENT_TYPE_CONSUMER);

        assertEquals(1048576, pscConfigurationInternal.getConfiguration().getLong(PscConfiguration.PSC_CONSUMER_BUFFER_RECEIVE_BYTES));
        assertEquals(null, pscConfigurationInternal.getConfiguration().getString(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET));
    }

    @Test
    void testValidateSerializerConfig() throws ConfigurationException {
        PscConfiguration pscProducerConfiguration = new PscConfiguration();
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "psc-producer");
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, new StringSerializer());

        PscConfiguration pscConsumerConfiguration = new PscConfiguration();
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "psc-consumer");
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "psc-consumer-group-id");
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, new StringDeserializer());
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConfigurationInternal pscProducerConfigurationInternal = new PscConfigurationInternal(pscProducerConfiguration, PscConfigurationInternal.PSC_CLIENT_TYPE_PRODUCER);
        PscConfigurationInternal pscConsumerConfigurationInternal = new PscConfigurationInternal(pscConsumerConfiguration, PscConfigurationInternal.PSC_CLIENT_TYPE_CONSUMER);

        // Test producer serializers
        assertTrue(pscProducerConfigurationInternal.getPscProducerKeySerializer() instanceof StringSerializer);
        assertTrue(pscProducerConfigurationInternal.getPscProducerValueSerializer() instanceof StringSerializer);

        // Test consumer deserializers
        assertTrue(pscConsumerConfigurationInternal.getPscConsumerKeyDeserializer() instanceof StringDeserializer);
        assertTrue(pscConsumerConfigurationInternal.getPscConsumerValueDeserializer() instanceof StringDeserializer);
    }

    private static void setConfigValue(PscConfiguration config, String key, String val) {
        config.setProperty(key, val);
    }

    private static void clearProperty(PscConfiguration config, String key) {
        config.clearProperty(key);
    }

    static class ProducerTypedInterceptor1 extends TypePreservingInterceptor<String, String> {
    }

    static class ProducerTypedInterceptor2 extends TypePreservingInterceptor<String, String> {
    }
}
