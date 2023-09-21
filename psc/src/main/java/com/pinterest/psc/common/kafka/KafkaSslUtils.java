package com.pinterest.psc.common.kafka;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetrics;
import org.apache.kafka.common.config.SslConfigs;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;

public class KafkaSslUtils {

    private static final PscLogger logger = PscLogger.getLogger(KafkaSslUtils.class);

    public static final int DEFAULT_SSL_EXPIRY_COUNTDOWN_SECS = 60 * 60 * 2;    // 2 hours

    protected static long getSslCertificateExpiryTimeFromFile(String path, String pass) {
        long expiryTime = Long.MAX_VALUE;
        try (FileInputStream keyStoreFile = new FileInputStream(path)) {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(keyStoreFile, pass.toCharArray());
            Enumeration<String> aliases = keyStore.aliases();
            if (aliases == null || !aliases.hasMoreElements()) {
                logger.warn("KeyStore for " + path + "doesn't have any alias");
                return expiryTime;
            }
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                Certificate cert = keyStore.getCertificate(alias);
                if (cert instanceof X509Certificate) {
                    long certExp = ((X509Certificate) cert).getNotAfter().getTime();
                    expiryTime = Math.min(certExp, expiryTime);
                    logger.debug(path + " " + alias + " got time " + certExp + " new expiry is " + expiryTime);
                } else {
                    logger.debug("Keystore=" + path + " alias=" + alias + " certificate is not X.509, "
                            + "skipping expiry time calculation. Type is " + cert.getType());
                }
            }
        } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | IOException e) {
            logger.error("Cannot initiate KeyStore for " + path, e);
        }
        return expiryTime;
    }

    public static long calculateSslCertExpiryTime(Properties kafkaClientProperties,
                                                  PscConfigurationInternal pscConfigurationInternal,
                                                  Set<Object> topicUriOrPartitions) {
        String keyStorePath = kafkaClientProperties.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        String keyStorePassword = kafkaClientProperties.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        long keyStoreExpiryTime = getSslCertificateExpiryTimeFromFile(keyStorePath, keyStorePassword);
        if (keyStoreExpiryTime == Long.MAX_VALUE) {
            logger.warn("Cannot fetch expiry time for key store " + keyStorePath);
            emitCounterMetric(PscMetrics.PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_FAILURE,
                    pscConfigurationInternal, topicUriOrPartitions);
        } else {
            emitCounterMetric(PscMetrics.PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_SUCCESS,
                    pscConfigurationInternal, topicUriOrPartitions);
        }

        String trustStorePath = kafkaClientProperties.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        String trustStorePassword = kafkaClientProperties.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        long trustStoreExpiryTime = getSslCertificateExpiryTimeFromFile(trustStorePath, trustStorePassword);
        if (trustStoreExpiryTime == Long.MAX_VALUE) {
            logger.warn("Cannot fetch expiry time for trust store" + trustStorePath);
            emitCounterMetric(PscMetrics.PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_FAILURE,
                    pscConfigurationInternal, topicUriOrPartitions);
        } else {
            emitCounterMetric(PscMetrics.PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_SUCCESS,
                    pscConfigurationInternal, topicUriOrPartitions);
        }

        long expiryTime = Math.min(keyStoreExpiryTime, trustStoreExpiryTime);
        if (expiryTime == Long.MAX_VALUE) {
            logger.warn("Could not fetch a valid expiry time from Key Store "
                    + "and Trust Store. Setting Expiry to " + DEFAULT_SSL_EXPIRY_COUNTDOWN_SECS
                    + " seconds from now");
            expiryTime = System.currentTimeMillis() + (DEFAULT_SSL_EXPIRY_COUNTDOWN_SECS * 1000);
        } else {
            logger.info("Successfully calculated cert expiry time: " + expiryTime);
        }
        emitHistogramMetric(PscMetrics.PSC_BACKEND_SECURE_SSL_CERTIFICATE_EXPIRY_TIME_METRIC,
                pscConfigurationInternal,
                topicUriOrPartitions,
                expiryTime);

        return expiryTime;
    }

    public static boolean keyStoresExist(Properties kafkaClientProperties) {
        for (String path : Arrays.asList(kafkaClientProperties.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                kafkaClientProperties.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG))) {
            Path p = Paths.get(path);
            if (!Files.exists(p)) {
                return false;
            }
        }
        return true;
    }

    private static void emitCounterMetric(String metricKey,
                                          PscConfigurationInternal pscConfigurationInternal,
                                          Set<Object> topicUriOrPartitions) {
        for (Object topicUriOrPartition: topicUriOrPartitions) {
            if (topicUriOrPartition instanceof TopicUri)
                PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                        (TopicUri) topicUriOrPartition, metricKey, pscConfigurationInternal);
            else if (topicUriOrPartition instanceof TopicUriPartition)
                PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                        ((TopicUriPartition) topicUriOrPartition).getTopicUri(),
                        ((TopicUriPartition) topicUriOrPartition).getPartition(),
                        metricKey, pscConfigurationInternal);

        }
    }

    private static void emitHistogramMetric(String metricKey,
                                            PscConfigurationInternal pscConfigurationInternal,
                                            Set<Object> topicUriOrPartitions,
                                            long value) {
        for (Object topicUriOrPartition: topicUriOrPartitions) {
            if (topicUriOrPartition instanceof TopicUri)
                PscMetricRegistryManager.getInstance().updateBackendHistogramMetric(
                        (TopicUri) topicUriOrPartition, metricKey, value, pscConfigurationInternal);
            else if (topicUriOrPartition instanceof TopicUriPartition)
                PscMetricRegistryManager.getInstance().updateBackendHistogramMetric(
                        ((TopicUriPartition) topicUriOrPartition).getTopicUri(),
                        ((TopicUriPartition) topicUriOrPartition).getPartition(),
                        metricKey, value, pscConfigurationInternal);

        }
    }
}