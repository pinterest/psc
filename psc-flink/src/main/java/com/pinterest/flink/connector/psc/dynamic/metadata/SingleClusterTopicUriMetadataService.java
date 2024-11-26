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

package com.pinterest.flink.connector.psc.dynamic.metadata;

import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.connector.psc.source.PscPropertiesUtil;
import com.pinterest.flink.connector.psc.source.PscSourceOptions;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import org.apache.flink.annotation.Experimental;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * A {@link PscMetadataService} that delegates metadata fetching to a single {@link PscMetadataClient},
 * which is scoped to a single cluster. The stream ids are equivalent to topics.
 */
@Experimental
public class SingleClusterTopicUriMetadataService implements PscMetadataService {

    private final String clusterId;
    private final Properties properties;
    private final TopicUri clusterUri;
    private transient PscMetadataClient metadataClient;

    /**
     * Create a {@link SingleClusterTopicUriMetadataService}.
     *
     * @param clusterId the id of the PubSub cluster.
     * @param properties the properties of the PubSub cluster.
     */
    public SingleClusterTopicUriMetadataService(String clusterId, Properties properties) throws TopicUriSyntaxException {
        this.clusterId = clusterId;
        this.properties = properties;
        this.clusterUri = PscFlinkConfiguration.validateAndGetBaseClusterUri(properties);
    }

    /** {@inheritDoc} */
    @Override
    public Set<PscStream> getAllStreams() {
        try {
            return getMetadataClient().listTopicRns(clusterUri, Duration.ofMillis(Long.MAX_VALUE)).stream()
                    .map(TopicRn::getTopic)
                    .map(this::createPscStream)
                    .collect(Collectors.toSet());
        } catch (InterruptedException | ExecutionException |
                 ConfigurationException | TopicUriSyntaxException | TimeoutException e) {
            throw new RuntimeException("Fetching all streams failed", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, PscStream> describeStreams(Collection<String> streamIds) {
        List<TopicUri> streamIdsConvertedToTopicUriList = streamIds.stream()
                .map(streamId -> clusterUri.getTopicUriAsString() + "/" + streamId)
                .map(topicUriStr -> {
                    try {
                        return BaseTopicUri.validate(topicUriStr);
                    } catch (TopicUriSyntaxException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
        try {
            return getMetadataClient().describeTopicUris(clusterUri, streamIdsConvertedToTopicUriList, Duration.ofMillis(Long.MAX_VALUE))
                    .keySet()
                    .stream()
                    .map(TopicUri::getTopic)
                    .collect(Collectors.toMap(topic -> topic, this::createPscStream));
        } catch (InterruptedException | ExecutionException | ConfigurationException | TopicUriSyntaxException | TimeoutException e) {
            throw new RuntimeException("Fetching all streams failed", e);
        }
    }

    private PscStream createPscStream(String topic) {
        ClusterMetadata clusterMetadata =
                new ClusterMetadata(Collections.singleton(topic), properties);

        return new PscStream(topic, Collections.singletonMap(clusterId, clusterMetadata));
    }

    private PscMetadataClient getMetadataClient() throws ConfigurationException {
        if (metadataClient == null) {
            Properties metadataClientProps = new Properties();
            PscPropertiesUtil.copyProperties(properties, metadataClientProps);
            String clientIdPrefix =
                    metadataClientProps.getProperty(PscSourceOptions.CLIENT_ID_PREFIX.key());
            metadataClientProps.setProperty(
                    PscConfiguration.PSC_METADATA_CLIENT_ID,
                    clientIdPrefix + "-single-cluster-topic-metadata-service");
            metadataClient = new PscMetadataClient(PscConfigurationUtils.propertiesToPscConfiguration(metadataClientProps));
        }

        return metadataClient;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClusterActive(String clusterId) {
        return this.clusterId.equals(clusterId);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        if (metadataClient != null) {
            metadataClient.close();
        }
    }
}
