/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.streaming.connectors.psc.internals.metrics;

import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.serde.ByteArrayDeserializer;
import com.pinterest.psc.serde.ByteArraySerializer;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.util.TestLoggerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.pinterest.flink.connector.psc.testutils.DockerImageVersions.KAFKA;
import static com.pinterest.flink.connector.psc.testutils.PscTestUtils.putDiscoveryProperties;
import static com.pinterest.flink.connector.psc.testutils.PscUtil.createKafkaContainer;

@Testcontainers
@ExtendWith(TestLoggerExtension.class)
class PscMetricMutableWrapperTest {

    private static final Logger LOG = LoggerFactory.getLogger(PscMetricMutableWrapperTest.class);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();

    @Container
    public static final KafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(KAFKA, LOG)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @Test
    void testOnlyMeasurableMetricsAreRegisteredWithMutableWrapper() throws ConfigurationException, ClientException, IOException {
        testOnlyMeasurableMetricsAreRegistered(PscMetricMutableWrapper::new);
    }

    @Test
    void testOnlyMeasurableMetricsAreRegistered() throws ConfigurationException, ClientException, IOException {
        testOnlyMeasurableMetricsAreRegistered(PscMetricWrapper::new);
    }

    private static void testOnlyMeasurableMetricsAreRegistered(
            Function<Metric, Gauge<Double>> wrapperFactory) throws ConfigurationException, ClientException, IOException {
        final Collection<Gauge<Double>> metricWrappers = new ArrayList<>();
        try (final PscConsumer<?, ?> consumer =
                        new PscConsumer<>(getPscClientConfiguration());
             final PscProducer<?, ?> producer =
                        new PscProducer<>(getPscClientConfiguration())) {
            Stream.concat(
                            consumer.metrics().values().stream(),
                            producer.metrics().values().stream())
                    .map(wrapperFactory::apply)
                    .forEach(metricWrappers::add);

            // Ensure that all values are accessible and return valid double values
            metricWrappers.forEach(Gauge::getValue);
        }
    }

    private static PscConfiguration getPscClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put(PscConfiguration.PSC_CONSUMER_GROUP_ID, UUID.randomUUID().toString());
        standardProps.put(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, false);
        standardProps.put(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, ByteArrayDeserializer.class.getName());
        standardProps.put(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, ByteArrayDeserializer.class.getName());
        standardProps.put(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, ByteArraySerializer.class.getName());
        standardProps.put(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, ByteArraySerializer.class.getName());
        standardProps.put(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "psc-metric-mutable-wrapper-test");
        standardProps.put(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "psc-metric-mutable-wrapper-test");
        standardProps.put(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, "earliest");
        standardProps.put(PscConfiguration.PSC_CONSUMER_PARTITION_FETCH_MAX_BYTES, 256);
        putDiscoveryProperties(standardProps, KAFKA_CONTAINER.getBootstrapServers(), PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        return PscConfigurationUtils.propertiesToPscConfiguration(standardProps);
    }
}
