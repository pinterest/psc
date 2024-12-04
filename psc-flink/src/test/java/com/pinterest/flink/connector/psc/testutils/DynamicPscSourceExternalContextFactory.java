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

package com.pinterest.flink.connector.psc.testutils;

import com.google.common.collect.ImmutableList;
import org.apache.flink.connector.testframe.external.ExternalContextFactory;
import org.testcontainers.containers.KafkaContainer;

import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

/** A external context factory for {@link com.pinterest.flink.connector.psc.dynamic.source.DynamicPscSource}
 * connector testing framework. */
public class DynamicPscSourceExternalContextFactory
        implements ExternalContextFactory<DynamicPscSourceExternalContext> {

    private final KafkaContainer kafkaContainer0;
    private final KafkaContainer kafkaContainer1;
    private final List<URL> connectorJars;

    public DynamicPscSourceExternalContextFactory(
            KafkaContainer kafkaContainer0,
            KafkaContainer kafkaContainer1,
            List<URL> connectorJars) {
        this.kafkaContainer0 = kafkaContainer0;
        this.kafkaContainer1 = kafkaContainer1;
        this.connectorJars = connectorJars;
    }

    @Override
    public DynamicPscSourceExternalContext createExternalContext(String testName) {
        return new DynamicPscSourceExternalContext(
                ImmutableList.of(
                        getBootstrapServers(kafkaContainer0), getBootstrapServers(kafkaContainer1)),
                connectorJars);
    }

    private static String getBootstrapServers(KafkaContainer kafkaContainer) {
        final String internalEndpoints =
                kafkaContainer.getNetworkAliases().stream()
                        .map(host -> String.join(":", host, Integer.toString(9092)))
                        .collect(Collectors.joining(","));
        return String.join(",", kafkaContainer.getBootstrapServers(), internalEndpoints);
    }
}
