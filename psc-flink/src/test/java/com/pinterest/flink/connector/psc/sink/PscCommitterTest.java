/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.connector.psc.sink;

import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.serde.StringSerializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.util.TestLoggerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PscCommitter}. */
@ExtendWith({TestLoggerExtension.class})
public class PscCommitterTest {

    private static final int PRODUCER_ID = 0;
    private static final short EPOCH = 0;
    private static final String TRANSACTIONAL_ID = "transactionalId";
    private static final String CLUSTER_URI = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region1::cluster1:";

    /** Causes a network error by inactive broker and tests that a retry will happen. */
    @Test
    public void testRetryCommittableOnRetriableError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final PscCommitter committer = new PscCommitter(properties);
                FlinkPscInternalProducer<Object, Object> producer =
                        new FlinkPscInternalProducer<>(properties, TRANSACTIONAL_ID);
                Recyclable<FlinkPscInternalProducer<Object, Object>> recyclable =
                        new Recyclable<>(producer, p -> {})) {
            final MockCommitRequest<PscCommittable> request =
                    new MockCommitRequest<>(
                            new PscCommittable(PRODUCER_ID, EPOCH, TRANSACTIONAL_ID, recyclable));

            producer.resumeTransaction(PRODUCER_ID, EPOCH);
            committer.commit(Collections.singletonList(request));

            assertThat(request.getNumberOfRetries()).isEqualTo(1);
            assertThat(recyclable.isRecycled()).isFalse();
            // FLINK-25531: force the producer to close immediately, else it would take 1 hour
            producer.close(Duration.ZERO);
        } catch (ConfigurationException | ProducerException | TopicUriSyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testFailJobOnUnknownFatalError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final PscCommitter committer = new PscCommitter(properties);
                FlinkPscInternalProducer<Object, Object> producer =
                        new FlinkPscInternalProducer<>(properties, TRANSACTIONAL_ID);
                Recyclable<FlinkPscInternalProducer<Object, Object>> recyclable =
                        new Recyclable<>(producer, p -> {})) {
            // will fail because transaction not started
            final MockCommitRequest<PscCommittable> request =
                    new MockCommitRequest<>(
                            new PscCommittable(PRODUCER_ID, EPOCH, TRANSACTIONAL_ID, recyclable));
            committer.commit(Collections.singletonList(request));
            assertThat(request.getFailedWithUnknownReason())
                    .isInstanceOf(IllegalStateException.class);
            assertThat(request.getFailedWithUnknownReason().getMessage())
                    .contains("Transaction was not started");
            assertThat(recyclable.isRecycled()).isTrue();
        } catch (ConfigurationException | ProducerException | TopicUriSyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testKafkaCommitterClosesProducer() throws IOException, InterruptedException, ConfigurationException, ProducerException, TopicUriSyntaxException {
        Properties properties = getProperties();
        FlinkPscInternalProducer<Object, Object> producer =
                new FlinkPscInternalProducer(properties, TRANSACTIONAL_ID) {
                    @Override
                    public void commitTransaction() {}

                    @Override
                    public void flush() {}

                    @Override
                    public void close() throws IOException {
                        super.close();
                    }
                };
        Recyclable<FlinkPscInternalProducer<Object, Object>> recyclable =
                new Recyclable<>(producer, p -> {
                    try {
                        p.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        try (final PscCommitter committer = new PscCommitter(properties)) {
            final MockCommitRequest<PscCommittable> request =
                    new MockCommitRequest<>(
                            new PscCommittable(PRODUCER_ID, EPOCH, TRANSACTIONAL_ID, recyclable));

            committer.commit(Collections.singletonList(request));
            assertThat(recyclable.isRecycled()).isTrue();
            assertThat(producer.isClosed()).isTrue();
            producer.close();
        }
    }

    Properties getProperties() {
        Properties properties = new Properties();
        properties.put(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        properties.put(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        properties.put(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "PscCommitterTest");
        properties.put(PscFlinkConfiguration.CLUSTER_URI_CONFIG, CLUSTER_URI);
        properties.setProperty("psc.discovery.topic.uri.prefixes",
                StringUtils.repeat(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX, ",", 1));
        properties.setProperty("psc.discovery.connection.urls", "http://localhost:1");
        properties.setProperty("psc.discovery.security.protocols",
                StringUtils.repeat("plaintext", ",", 1));
        properties.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        return properties;
    }
}
