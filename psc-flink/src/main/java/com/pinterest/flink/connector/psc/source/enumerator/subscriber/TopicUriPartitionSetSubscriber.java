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
import com.pinterest.psc.metadata.TopicRnMetadata;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.pinterest.flink.connector.psc.source.enumerator.subscriber.PscSubscriberUtils.getTopicRnMetadata;

/** A subscriber for a partition set. */
class TopicUriPartitionSetSubscriber implements PscSubscriber {
    private static final long serialVersionUID = 390970375272146036L;
    private static final Logger LOG = LoggerFactory.getLogger(TopicUriPartitionSetSubscriber.class);
    private final Set<TopicUriPartition> subscribedPartitions;

    TopicUriPartitionSetSubscriber(Set<TopicUriPartition> partitions) {
        this.subscribedPartitions = partitions;
    }

    @Override
    public Set<TopicUriPartition> getSubscribedTopicUriPartitions(PscMetadataClient metadataClient, TopicUri clusterUri) {
        final List<TopicRn> topicRns =
                subscribedPartitions.stream()
                        .map(TopicUriPartition::getTopicUri)
                        .map(TopicUri::getTopicRn)
                        .collect(Collectors.toList());

        LOG.debug("Fetching descriptions for topics: {}", topicRns);
        final Map<TopicRn, TopicRnMetadata> topicRnMetadata =
                getTopicRnMetadata(metadataClient, clusterUri, topicRns);

        Set<TopicUriPartition> existingSubscribedPartitions = new HashSet<>();

        for (TopicUriPartition subscribedPartition : this.subscribedPartitions) {
            if (topicRnMetadata.containsKey(subscribedPartition.getTopicUri().getTopicRn())
                    && partitionExistsInTopic(
                            subscribedPartition, topicRnMetadata.get(subscribedPartition.getTopicUri().getTopicRn()))) {
                existingSubscribedPartitions.add(subscribedPartition);
            } else {
                throw new RuntimeException(
                        String.format(
                                "Partition '%s' does not exist on PubSub brokers",
                                subscribedPartition));
            }
        }

        return existingSubscribedPartitions;
    }

    private boolean partitionExistsInTopic(TopicUriPartition partition, TopicRnMetadata topicRnMetadata) {
        return topicRnMetadata.getTopicUriPartitions().size() > partition.getPartition();
    }
}
