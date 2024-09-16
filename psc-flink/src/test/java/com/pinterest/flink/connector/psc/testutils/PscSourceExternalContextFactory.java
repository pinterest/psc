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

import org.apache.flink.connector.testframe.external.ExternalContextFactory;

import org.testcontainers.containers.KafkaContainer;

import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

/** Factory of {@link PscSourceExternalContext}. */
public class PscSourceExternalContextFactory
        implements ExternalContextFactory<PscSourceExternalContext> {

    private final KafkaContainer kafkaContainer;
    private final List<URL> connectorJars;
    private final PscSourceExternalContext.SplitMappingMode splitMappingMode;

    public PscSourceExternalContextFactory(
            KafkaContainer kafkaContainer,
            List<URL> connectorJars,
            PscSourceExternalContext.SplitMappingMode splitMappingMode) {
        this.kafkaContainer = kafkaContainer;
        this.connectorJars = connectorJars;
        this.splitMappingMode = splitMappingMode;
    }

    protected String getBootstrapServer() {
        final String internalEndpoints =
                kafkaContainer.getNetworkAliases().stream()
                        .map(host -> String.join(":", host, Integer.toString(9092)))
                        .collect(Collectors.joining(","));
        return String.join(",", kafkaContainer.getBootstrapServers(), internalEndpoints);
    }

    @Override
    public PscSourceExternalContext createExternalContext(String testName) {
        return new PscSourceExternalContext(
                getBootstrapServer(), this.splitMappingMode, this.connectorJars);
    }
}
