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

package com.pinterest.flink.connector.psc.sink.testutils;

import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.apache.flink.connector.testframe.external.ExternalContextFactory;
import org.testcontainers.containers.KafkaContainer;

import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/** Kafka sink external context factory. */
public class PscSinkExternalContextFactory
        implements ExternalContextFactory<PscSinkExternalContext> {

    private final KafkaContainer kafkaContainer;
    private final List<URL> connectorJars;

    public PscSinkExternalContextFactory(KafkaContainer kafkaContainer, List<URL> connectorJars) {
        this.kafkaContainer = kafkaContainer;
        this.connectorJars = connectorJars;
    }

    private String getBootstrapServer() {
        final String internalEndpoints =
                kafkaContainer.getNetworkAliases().stream()
                        .map(host -> String.join(":", host, Integer.toString(9092)))
                        .collect(Collectors.joining(","));
        return String.join(",", kafkaContainer.getBootstrapServers(), internalEndpoints);
    }

    @Override
    public PscSinkExternalContext createExternalContext(String testName) {
        return new PscSinkExternalContext(getBootstrapServer(), PscTestUtils.getClusterUri(), connectorJars);
    }
}
