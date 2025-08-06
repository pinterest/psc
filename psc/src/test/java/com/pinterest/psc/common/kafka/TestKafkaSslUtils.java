package com.pinterest.psc.common.kafka;

import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetrics;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestKafkaSslUtils {

    private static final String TEST_KEYSTORE_PATH = "src/test/resources/kafka.keystore.jks";
    private static final String TEST_TRUSTSTORE_PATH = "src/test/resources/kafka.truststore.jks";
    private static final String TEST_NONEXISTENT_PATH = "src/test/resources/kafka.nonexistent.jks";
    private static final String TEST_PASSWORD = "testpass";
    private static String topicUriSecure1 = "secure:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic1";
    private static String topicUriSecure2 = "secure:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic2";
    private static String topicUriSecure3 = "secure:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":test:env:cloud_region::cluster:topic3";
    private static final long EXPECTED_EXPIRY_TIMESTAMP = 1715793882000L;

    @Test
    public void testGetSslCertificateExpiryTimeFromFile() {
        long keystoreExpiry = KafkaSslUtils.getSslCertificateExpiryTimeFromFile(TEST_KEYSTORE_PATH, TEST_PASSWORD);
        long truststoreExpiry = KafkaSslUtils.getSslCertificateExpiryTimeFromFile(TEST_TRUSTSTORE_PATH, TEST_PASSWORD);
        assertEquals(EXPECTED_EXPIRY_TIMESTAMP, keystoreExpiry);
        assertEquals(EXPECTED_EXPIRY_TIMESTAMP, truststoreExpiry);
        long expiryNonexistent = KafkaSslUtils.getSslCertificateExpiryTimeFromFile(TEST_NONEXISTENT_PATH, TEST_PASSWORD);
        assertEquals(Long.MAX_VALUE, expiryNonexistent);
    }

    @Test
    public void testCalculateSslCertExpiryTime() throws ConfigurationException, TopicUriSyntaxException {
        Properties kafkaClientProperties = new Properties();
        kafkaClientProperties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, TEST_KEYSTORE_PATH);
        kafkaClientProperties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TEST_TRUSTSTORE_PATH);
        kafkaClientProperties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, TEST_PASSWORD);
        kafkaClientProperties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TEST_PASSWORD);

        // nonexistent keystore
        Properties kafkaClientPropertiesNonexistentKeystore = new Properties();
        kafkaClientPropertiesNonexistentKeystore.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, TEST_NONEXISTENT_PATH);
        kafkaClientPropertiesNonexistentKeystore.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TEST_TRUSTSTORE_PATH);
        kafkaClientPropertiesNonexistentKeystore.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, TEST_PASSWORD);
        kafkaClientPropertiesNonexistentKeystore.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TEST_PASSWORD);

        // nonexistent both keystore and truststore
        Properties kafkaClientPropertiesNonexistent = new Properties();
        kafkaClientPropertiesNonexistent.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, TEST_NONEXISTENT_PATH);
        kafkaClientPropertiesNonexistent.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TEST_NONEXISTENT_PATH);
        kafkaClientPropertiesNonexistent.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, TEST_PASSWORD);
        kafkaClientPropertiesNonexistent.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TEST_PASSWORD);

        PscConfiguration configuration1 = new PscConfiguration();
        configuration1.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "client-id");
        configuration1.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "group-id");
        configuration1.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        configuration1.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        configuration1.setProperty(PscConfiguration.PSC_METRICS_REPORTER_PARALLELISM, "5");
        PscConfigurationInternal pscConfigurationInternal = new PscConfigurationInternal(configuration1, PscConfigurationInternal.PSC_CLIENT_TYPE_CONSUMER);

        PscMetricRegistryManager.getInstance().initialize(pscConfigurationInternal);
        TopicUri topicUri1 = KafkaTopicUri.validate(topicUriSecure1);
        TopicUri topicUri2 = KafkaTopicUri.validate(topicUriSecure2);
        TopicUri topicUri3 = KafkaTopicUri.validate(topicUriSecure3);
        TopicUriPartition topicUriPartition1 = new TopicUriPartition(topicUriSecure1, 0);
        TopicUriPartition topicUriPartition2 = new TopicUriPartition(topicUriSecure1, 1);

        // happy path testing
        long expirySuccess = KafkaSslUtils.calculateSslCertExpiryTime(kafkaClientProperties, pscConfigurationInternal, Collections.singleton(topicUri1));
        assertEquals(EXPECTED_EXPIRY_TIMESTAMP, expirySuccess);
        long keystoreReadSuccessCount1 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUri1, PscMetrics.PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_SUCCESS, pscConfigurationInternal);
        long truststoreReadSuccessCount1 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUri1, PscMetrics.PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_SUCCESS, pscConfigurationInternal);
        long keystoreReadFailureCount1 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUri1, PscMetrics.PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_FAILURE, pscConfigurationInternal);
        long truststoreReadFailureCount1 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUri1, PscMetrics.PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_FAILURE, pscConfigurationInternal);
        long expiryTimeMetric = PscMetricRegistryManager.getInstance().getBackendHistogramMetric(
                topicUri1, PscMetrics.PSC_BACKEND_SECURE_SSL_CERTIFICATE_EXPIRY_TIME_METRIC, pscConfigurationInternal).getValues()[0];
        assertEquals(1, keystoreReadSuccessCount1);
        assertEquals(1, truststoreReadSuccessCount1);
        assertEquals(0, keystoreReadFailureCount1);
        assertEquals(0, truststoreReadFailureCount1);
        assertEquals(EXPECTED_EXPIRY_TIMESTAMP, expiryTimeMetric);

        // should still use min(truststore, keystore) if one is nonexistent
        long expiryNonexistentKeystore = KafkaSslUtils.calculateSslCertExpiryTime(
                kafkaClientPropertiesNonexistentKeystore, pscConfigurationInternal, Collections.singleton(topicUri2));
        assertEquals(1715793882000L, expiryNonexistentKeystore);
        long keystoreReadSuccessCount2 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUri2, PscMetrics.PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_SUCCESS, pscConfigurationInternal);
        long truststoreReadSuccessCount2 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUri2, PscMetrics.PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_SUCCESS, pscConfigurationInternal);
        long keystoreReadFailureCount2 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUri2, PscMetrics.PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_FAILURE, pscConfigurationInternal);
        long truststoreReadFailureCount2 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUri2, PscMetrics.PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_FAILURE, pscConfigurationInternal);
        long expiryTimeMetricNonexistentKeystore = PscMetricRegistryManager.getInstance().getBackendHistogramMetric(
                topicUri2, PscMetrics.PSC_BACKEND_SECURE_SSL_CERTIFICATE_EXPIRY_TIME_METRIC, pscConfigurationInternal).getValues()[0];
        assertEquals(0, keystoreReadSuccessCount2);
        assertEquals(1, truststoreReadSuccessCount2);
        assertEquals(1, keystoreReadFailureCount2);
        assertEquals(0, truststoreReadFailureCount2);
        assertEquals(EXPECTED_EXPIRY_TIMESTAMP, expiryTimeMetricNonexistentKeystore);

        // if both nonexistent, use current time + default countdown
        long expiryNonexistent = KafkaSslUtils.calculateSslCertExpiryTime(
                kafkaClientPropertiesNonexistent, pscConfigurationInternal, Collections.singleton(topicUri3));
        // half-second buffer for test execution
        long buffer = 500L;
        assertTrue(expiryNonexistent < System.currentTimeMillis() + KafkaSslUtils.DEFAULT_SSL_EXPIRY_COUNTDOWN_SECS * 1000 + buffer);
        assertTrue(expiryNonexistent > System.currentTimeMillis());
        long keystoreReadSuccessCount3 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUri3, PscMetrics.PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_SUCCESS, pscConfigurationInternal);
        long truststoreReadSuccessCount3 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUri3, PscMetrics.PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_SUCCESS, pscConfigurationInternal);
        long keystoreReadFailureCount3 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUri3, PscMetrics.PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_FAILURE, pscConfigurationInternal);
        long truststoreReadFailureCount3 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUri3, PscMetrics.PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_FAILURE, pscConfigurationInternal);
        long expiryTimeMetricNonexistent = PscMetricRegistryManager.getInstance().getBackendHistogramMetric(
                topicUri3, PscMetrics.PSC_BACKEND_SECURE_SSL_CERTIFICATE_EXPIRY_TIME_METRIC, pscConfigurationInternal).getValues()[0];
        assertEquals(0, keystoreReadSuccessCount3);
        assertEquals(0, truststoreReadSuccessCount3);
        assertEquals(1, keystoreReadFailureCount3);
        assertEquals(1, truststoreReadFailureCount3);
        assertTrue(expiryTimeMetricNonexistent < System.currentTimeMillis() + KafkaSslUtils.DEFAULT_SSL_EXPIRY_COUNTDOWN_SECS * 1000 + buffer);
        assertTrue(expiryTimeMetricNonexistent > System.currentTimeMillis());

        // test using collection of topicUriPartitions
        long expirySuccessTup = KafkaSslUtils.calculateSslCertExpiryTime(kafkaClientProperties, pscConfigurationInternal,
                new HashSet<>(Arrays.asList(topicUriPartition1, topicUriPartition2)));
        assertEquals(1715793882000L, expirySuccessTup);
        long keystoreReadSuccessCountTup1 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUriPartition1.getTopicUri(), topicUriPartition1.getPartition(),
                PscMetrics.PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_SUCCESS, pscConfigurationInternal);
        long truststoreReadSuccessCountTup1 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUriPartition1.getTopicUri(), topicUriPartition1.getPartition(),
                PscMetrics.PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_SUCCESS, pscConfigurationInternal);
        long keystoreReadSuccessCountTup2 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUriPartition2.getTopicUri(), topicUriPartition2.getPartition(),
                PscMetrics.PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_SUCCESS, pscConfigurationInternal);
        long truststoreReadSuccessCountTup2 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUriPartition2.getTopicUri(), topicUriPartition2.getPartition(),
                PscMetrics.PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_SUCCESS, pscConfigurationInternal);
        long keystoreReadFailureCountTup1 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUriPartition1.getTopicUri(), topicUriPartition1.getPartition(),
                PscMetrics.PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_FAILURE, pscConfigurationInternal);
        long truststoreReadFailureCountTup1 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUriPartition1.getTopicUri(), topicUriPartition1.getPartition(),
                PscMetrics.PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_FAILURE, pscConfigurationInternal);
        long keystoreReadFailureCountTup2 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUriPartition2.getTopicUri(), topicUriPartition2.getPartition(),
                PscMetrics.PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_FAILURE, pscConfigurationInternal);
        long truststoreReadFailureCountTup2 = PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                topicUriPartition2.getTopicUri(), topicUriPartition2.getPartition(),
                PscMetrics.PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_FAILURE, pscConfigurationInternal);
        long expiryTimeMetricTup1 = PscMetricRegistryManager.getInstance().getBackendHistogramMetric(
                topicUriPartition1.getTopicUri(), topicUriPartition1.getPartition(),
                PscMetrics.PSC_BACKEND_SECURE_SSL_CERTIFICATE_EXPIRY_TIME_METRIC, pscConfigurationInternal).getValues()[0];
        long expiryTimeMetricTup2 = PscMetricRegistryManager.getInstance().getBackendHistogramMetric(
                topicUriPartition2.getTopicUri(), topicUriPartition2.getPartition(),
                PscMetrics.PSC_BACKEND_SECURE_SSL_CERTIFICATE_EXPIRY_TIME_METRIC, pscConfigurationInternal).getValues()[0];
        assertEquals(1, keystoreReadSuccessCountTup1);
        assertEquals(1, truststoreReadSuccessCountTup1);
        assertEquals(1, keystoreReadSuccessCountTup2);
        assertEquals(1, truststoreReadSuccessCountTup2);
        assertEquals(0, keystoreReadFailureCountTup1);
        assertEquals(0, truststoreReadFailureCountTup1);
        assertEquals(0, keystoreReadFailureCountTup2);
        assertEquals(0, truststoreReadFailureCountTup2);
        assertEquals(EXPECTED_EXPIRY_TIMESTAMP, expiryTimeMetricTup1);
        assertEquals(EXPECTED_EXPIRY_TIMESTAMP, expiryTimeMetricTup2);
    }

    @Test
    public void testKeyStoresExist() {
        Properties kafkaClientProperties = new Properties();
        kafkaClientProperties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, TEST_KEYSTORE_PATH);
        kafkaClientProperties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TEST_TRUSTSTORE_PATH);
        kafkaClientProperties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, TEST_PASSWORD);
        kafkaClientProperties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TEST_PASSWORD);

        // nonexistent keystore
        Properties kafkaClientPropertiesNonexistentKeystore = new Properties();
        kafkaClientPropertiesNonexistentKeystore.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, TEST_NONEXISTENT_PATH);
        kafkaClientPropertiesNonexistentKeystore.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TEST_TRUSTSTORE_PATH);
        kafkaClientPropertiesNonexistentKeystore.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, TEST_PASSWORD);
        kafkaClientPropertiesNonexistentKeystore.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TEST_PASSWORD);

        // nonexistent both keystore and truststore
        Properties kafkaClientPropertiesNonexistent = new Properties();
        kafkaClientPropertiesNonexistent.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, TEST_NONEXISTENT_PATH);
        kafkaClientPropertiesNonexistent.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TEST_NONEXISTENT_PATH);
        kafkaClientPropertiesNonexistent.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, TEST_PASSWORD);
        kafkaClientPropertiesNonexistent.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TEST_PASSWORD);

        boolean bothExists = KafkaSslUtils.keyStoresExist(kafkaClientProperties);
        boolean nonexistentKeystore = KafkaSslUtils.keyStoresExist(kafkaClientPropertiesNonexistentKeystore);
        boolean bothNotExists = KafkaSslUtils.keyStoresExist(kafkaClientPropertiesNonexistent);

        assertTrue(bothExists);
        assertFalse(nonexistentKeystore);
        assertFalse(bothNotExists);
    }
}