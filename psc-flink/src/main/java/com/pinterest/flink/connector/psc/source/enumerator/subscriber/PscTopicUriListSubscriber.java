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

package com.pinterest.flink.connector.psc.source.enumerator.subscriber;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metadata.TopicUriMetadata;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.pinterest.flink.connector.psc.source.enumerator.subscriber.PscSubscriberUtils.getTopicUriMetadata;

/**
 * A subscriber to a fixed list of topics. The subscribed topics must have existed in the PSC
 * cluster, otherwise an exception will be thrown.
 */
class PscTopicUriListSubscriber implements PscSubscriber {
    private static final long serialVersionUID = -6917603843104947866L;
    private static final Logger LOG = LoggerFactory.getLogger(PscTopicUriListSubscriber.class);
    private final List<TopicUri> topicUris;

    PscTopicUriListSubscriber(List<String> topicUris) {
        this.topicUris = topicUris.stream().map(topicUri -> {
            try {
                return BaseTopicUri.validate(topicUri);
            } catch (TopicUriSyntaxException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }

    /**
     * Get a set of subscribed {@link TopicUriPartition}s. This method will preserve the protocol of the
     * supplied topicUris.
     *
     * @param metadataClient The admin client used to retrieve subscribed topic partitions.
     * @param clusterUri The cluster URI to subscribe to.
     * @return A set of subscribed {@link TopicUriPartition}s
     */
    @Override
    public Set<TopicUriPartition> getSubscribedTopicUriPartitions(PscMetadataClient metadataClient, TopicUri clusterUri) {
        LOG.debug("Fetching descriptions for topicUris: {}", topicUris);
        final Map<TopicUri, TopicUriMetadata> topicMetadata =
                getTopicUriMetadata(metadataClient, clusterUri, topicUris);

        Set<TopicUriPartition> subscribedPartitions = new HashSet<>();
        for (TopicUriMetadata topic : topicMetadata.values()) {
            subscribedPartitions.addAll(topic.getTopicUriPartitions());
        }

        return subscribedPartitions;
    }
}
