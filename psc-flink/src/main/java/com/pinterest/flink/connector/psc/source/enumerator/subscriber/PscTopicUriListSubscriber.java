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

/**
 * A subscriber to a fixed list of topics. The subscribed topics must have existed in the Kafka
 * cluster, otherwise an exception will be thrown.
 */
class PscTopicUriListSubscriber implements PscSubscriber {
    private static final long serialVersionUID = -6917603843104947866L;
    private static final Logger LOG = LoggerFactory.getLogger(PscTopicUriListSubscriber.class);
    private final List<TopicRn> topicRns;

    PscTopicUriListSubscriber(List<String> topicUris) {
        this.topicRns = topicUris.stream().map(topicUri -> {
            try {
                return BaseTopicUri.validate(topicUri).getTopicRn();
            } catch (TopicUriSyntaxException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }

    @Override
    public Set<TopicUriPartition> getSubscribedTopicUriPartitions(PscMetadataClient metadataClient, TopicUri clusterUri) {
        LOG.debug("Fetching descriptions for topicRns: {}", topicRns);
        final Map<TopicRn, TopicRnMetadata> topicMetadata =
                getTopicRnMetadata(metadataClient, clusterUri, topicRns);

        Set<TopicUriPartition> subscribedPartitions = new HashSet<>();
        for (TopicRnMetadata topic : topicMetadata.values()) {
            subscribedPartitions.addAll(topic.getTopicUriPartitions());
        }

        return subscribedPartitions;
    }
}
