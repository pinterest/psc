package com.pinterest.psc.integration.consumer;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.integration.KafkaCluster;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.utils.PscTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.kafka.test.junit5.SharedZookeeperTestResource;
import com.salesforce.kafka.test.listeners.SslListener;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestOneKafkaBackendSsl {
    private static final PscLogger logger = PscLogger.getLogger(TestOneKafkaBackendSsl.class);

    private static final int TEST_TIMEOUT_SECONDS = 60;
    private static final PscConfiguration consumerConfig = new PscConfiguration();
    private static final PscConfiguration producerConfig = new PscConfiguration();
    private static String baseClientId;
    private static String baseGroupId;

    private static final String CERT_TMP_DIRECTORY = "/tmp/pscTestCerts";
    private static final String CLIENT_CERT_TEMP_DIRECTORY = "/tmp/pscTestClientCerts";
    private static final String GENERATE_CERTS_SCRIPT = "src/test/resources/generateCertificates.sh";
    private static final String KEYSTORE_PATH = CERT_TMP_DIRECTORY + "/" + "keystore/kafka.keystore.jks";
    private static final String TRUSTSTORE_PATH = CERT_TMP_DIRECTORY + "/" + "truststore/kafka.truststore.jks";
    private static final String CLIENT_KEYSTORE_PATH = CLIENT_CERT_TEMP_DIRECTORY + "/kafka.keystore.jks";
    private static final String CLIENT_TRUSTSTORE_PATH = CLIENT_CERT_TEMP_DIRECTORY + "/kafka.truststore.jks";
    private static final String CA_CERT_PATH = CERT_TMP_DIRECTORY + "/ca-cert";
    private static final String CA_KEY_PATH = CERT_TMP_DIRECTORY + "/ca-key";
    private static final String CERT_FILE_PATH = CERT_TMP_DIRECTORY + "/cert-file";
    private static final String CERT_FILE_SIGNED_PATH = CERT_TMP_DIRECTORY + "/cert-file-signed";
    private static final String PASSWORD = "password";
    private static final String topicWithSsl = "topic";

    private KafkaCluster kafkaSSLCluster;
    private String topicUriStrWithSsl;

    static {
        // generate certs for SSL tests
        try {
            Process proc = Runtime.getRuntime().exec(String.format("sh " + GENERATE_CERTS_SCRIPT + " " +
                                                            "%s %s %s %s %s %s %s %s %s %s",
                                                    KEYSTORE_PATH, TRUSTSTORE_PATH,
                                                    CLIENT_KEYSTORE_PATH, CLIENT_TRUSTSTORE_PATH, CA_CERT_PATH,
                                                    CA_KEY_PATH, CERT_FILE_PATH,
                                                    CERT_FILE_SIGNED_PATH,
                                                    CERT_TMP_DIRECTORY, PASSWORD));
            printProcessOutput(proc);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @RegisterExtension
    public static SharedKafkaTestResource sharedKafkaTestResourceWithSSL = new SharedKafkaTestResource()
            .withBrokers(1)
            .withBrokerProperty("auto.create.topics.enable", "false")
            .withBrokerProperty("socket.request.max.bytes", "400000000")
            .withBrokerProperty("listener.name.ssl.ssl.keystore.location", KEYSTORE_PATH)
            .withBrokerProperty("listener.name.ssl.ssl.truststore.location", TRUSTSTORE_PATH)
            .registerListener(
                    new SslListener()
                            .withClientAuthRequested()
                            .withKeyStoreLocation(KEYSTORE_PATH)
                            .withKeyStorePassword(PASSWORD)
                            .withTrustStoreLocation(TRUSTSTORE_PATH)
                            .withTrustStorePassword(PASSWORD)
                            .withKeyPassword(PASSWORD)
            );

    @RegisterExtension
    public static final SharedZookeeperTestResource sharedZookeeperTestResource = new SharedZookeeperTestResource();

    @BeforeEach
    public void setup() throws IOException, InterruptedException {
        baseClientId = this.getClass().getSimpleName() + "-psc-consumer-client";
        baseGroupId = this.getClass().getSimpleName() + "-psc-consumer-group";
        consumerConfig.clear();
        consumerConfig.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        consumerConfig.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseClientId + "-" + UUID.randomUUID());
        consumerConfig.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, baseGroupId + "-" + UUID.randomUUID());

        kafkaSSLCluster = new KafkaCluster(
                "secure",
                "region",
                "sslCluster",
                Integer.parseInt(
                        sharedKafkaTestResourceWithSSL.getKafkaBrokers().stream().iterator().next().getConnectString().split(":")[2]
                )
        );
        kafkaSSLCluster.createTlsServersetFile();
        int partitionsWithSsl = 6;
        topicUriStrWithSsl = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaSSLCluster.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaSSLCluster.getRegion(), kafkaSSLCluster.getCluster(), topicWithSsl);
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResourceWithSSL, topicWithSsl, partitionsWithSsl);
    }

    @AfterEach
    public void teardown() throws ExecutionException, InterruptedException {
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResourceWithSSL, topicWithSsl);
    }

    @AfterAll
    public static void shutdown() throws IOException, InterruptedException {
        FileUtils.deleteDirectory(new File(CERT_TMP_DIRECTORY));
        FileUtils.deleteDirectory(new File(CLIENT_CERT_TEMP_DIRECTORY));
    }

    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    @Disabled // ignore for now since this reproduces the SSL handshake exception
    public void testSSLErrorRecovery() throws ConsumerException, ConfigurationException, ProducerException {
        consumerConfig.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        consumerConfig.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        consumerConfig.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        consumerConfig.setProperty(PscConfiguration.PSC_CONSUMER_POLL_MESSAGES_MAX, 1);

        // SSL configs
        consumerConfig.setProperty("psc.consumer." + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, CLIENT_TRUSTSTORE_PATH);
        consumerConfig.setProperty("psc.consumer." + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, PASSWORD);
        consumerConfig.setProperty("psc.consumer." + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, CLIENT_KEYSTORE_PATH);
        consumerConfig.setProperty("psc.consumer." + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, PASSWORD);
        consumerConfig.setProperty("psc.consumer." + SslConfigs.SSL_KEY_PASSWORD_CONFIG, PASSWORD);
        consumerConfig.setProperty("psc.consumer." + SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2,TLSv1.1,TLSv1");
        consumerConfig.setProperty("psc.consumer." + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        consumerConfig.setProperty("psc.consumer." + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");

        producerConfig.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, com.pinterest.psc.serde.StringSerializer.class.getName());
        producerConfig.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, com.pinterest.psc.serde.StringSerializer.class.getName());
        producerConfig.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "testProducer");

        producerConfig.setProperty("psc.producer." + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, CLIENT_TRUSTSTORE_PATH);
        producerConfig.setProperty("psc.producer." + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, PASSWORD);
        producerConfig.setProperty("psc.producer." + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, CLIENT_KEYSTORE_PATH);
        producerConfig.setProperty("psc.producer." + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, PASSWORD);
        producerConfig.setProperty("psc.producer." + SslConfigs.SSL_KEY_PASSWORD_CONFIG, PASSWORD);
        producerConfig.setProperty("psc.producer." + SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2,TLSv1.1,TLSv1");
        producerConfig.setProperty("psc.producer." + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        producerConfig.setProperty("psc.producer." + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");

        PscConsumer<String, String> pscConsumer = new PscConsumer<>(consumerConfig);
        pscConsumer.subscribe(Collections.singleton(topicUriStrWithSsl));

        PscProducer<String, String> pscProducer = new PscProducer<>(producerConfig);

        long duration = 30000;
        long end = System.currentTimeMillis() + duration;
        boolean certRotated = false;
        while (System.currentTimeMillis() < end) {
            int messageCount = 1;
            for (int i = 0; i < messageCount; i++) {
                try {
                    Future<MessageId> messageFuture = pscProducer.send(new PscProducerMessage<>(topicUriStrWithSsl, "test"));
                    messageFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    // SSL handshake error should be thrown here after CA is rotated
                    throw new RuntimeException(e);
                }

            }
            if (end - System.currentTimeMillis() < duration/2 && !certRotated) {
                // perform CA rotation
                try {
                    Process proc = Runtime.getRuntime()
                           .exec(String.format("sh " + GENERATE_CERTS_SCRIPT + " " +
                                                       "%s %s %s %s %s %s %s %s %s %s rotate",
                                               KEYSTORE_PATH, TRUSTSTORE_PATH,
                                               CLIENT_KEYSTORE_PATH, CLIENT_TRUSTSTORE_PATH, CA_CERT_PATH,
                                               CA_KEY_PATH, CERT_FILE_PATH,
                                               CERT_FILE_SIGNED_PATH,
                                               CERT_TMP_DIRECTORY, PASSWORD));
                    printProcessOutput(proc);
                    Thread.sleep(2000);

                    // we can either re-create the consumer or producer after client cert rotation. whichever one is
                    // recreated will fail with SSL handshake exception after rotation. If neither are re-created,
                    // production and consumption will continue to succeed after cert rotation
//                    pscProducer = new PscProducer<>(producerConfig);
                    pscConsumer = new PscConsumer<>(consumerConfig);
                    pscConsumer.subscribe(Collections.singleton(topicUriStrWithSsl));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                logger.info("Certs rotated");
                certRotated = true;
            }
            pscConsumer.poll(Duration.ofSeconds(3));
        }
    }

    private static void printProcessOutput(Process proc) {
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));

        BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

        // Read the output from the command
        logger.info("Here is the standard output of the command:\n");
        String s = null;
        try {
            while ((s = stdInput.readLine()) != null) {
                logger.info("\t" + s);
            }

            // Read any errors from the attempted command
            logger.info("Here is the standard error of the command (if any):\n");
            while ((s = stdError.readLine()) != null) {
                logger.info("\t" + s);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
