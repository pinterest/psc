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

import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.metadata.TopicUriMetadata;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.pinterest.flink.connector.psc.source.enumerator.subscriber.PscSubscriberUtils.getAllTopicUriMetadata;

/**
 * A subscriber to a topic name pattern. Note that this pattern should match only the topic name itself. The pattern
 * does not care about the entire TopicUri.
 */
class TopicNamePatternSubscriber implements PscSubscriber {
    private static final long serialVersionUID = -7471048577725467797L;
    private static final Logger LOG = LoggerFactory.getLogger(TopicNamePatternSubscriber.class);
    private final Pattern topicNamePattern;

    TopicNamePatternSubscriber(Pattern topicNamePattern) {
        this.topicNamePattern = topicNamePattern;
    }

    /**
     * Get a set of subscribed {@link TopicUriPartition}s. This method will return a set of TopicUriPartitions whose
     * protocols match the clusterUri's protocol.
     *
     * @param metadataClient The admin client used to retrieve subscribed topic partitions.
     * @param clusterUri The cluster URI to subscribe to.
     * @return A set of subscribed {@link TopicUriPartition}s
     */
    @Override
    public Set<TopicUriPartition> getSubscribedTopicUriPartitions(PscMetadataClient metadataClient, TopicUri clusterUri) {
        LOG.debug("Fetching descriptions for all topics on PubSub cluster");
        final Map<TopicUri, TopicUriMetadata> allTopicRnMetadata = getAllTopicUriMetadata(metadataClient, clusterUri);

        Set<TopicUriPartition> subscribedTopicUriPartitions = new HashSet<>();

        allTopicRnMetadata.forEach(
                (topicRn, topicRnMetadata) -> {
                    if (topicNamePattern.matcher(topicRn.getTopic()).matches()) {
                        subscribedTopicUriPartitions.addAll(topicRnMetadata.getTopicUriPartitions());
                    }
                });

        return subscribedTopicUriPartitions;
    }
}
