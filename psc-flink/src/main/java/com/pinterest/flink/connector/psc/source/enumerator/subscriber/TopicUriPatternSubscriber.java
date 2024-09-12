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
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.pinterest.flink.connector.psc.source.enumerator.subscriber.PscSubscriberUtils.getAllTopicRnMetadata;

/** A subscriber to a topic pattern. */
class TopicUriPatternSubscriber implements PscSubscriber {
    private static final long serialVersionUID = -7471048577725467797L;
    private static final Logger LOG = LoggerFactory.getLogger(TopicUriPatternSubscriber.class);
    private final Pattern topicRnPattern;

    TopicUriPatternSubscriber(Pattern topicPattern) {
        this.topicRnPattern = topicPattern;
    }

    @Override
    public Set<TopicUriPartition> getSubscribedTopicUriPartitions(PscMetadataClient metadataClient, TopicUri clusterUri) {
        LOG.debug("Fetching descriptions for all topics on Kafka cluster");
        final Map<TopicRn, TopicRnMetadata> allTopicRnMetadata = getAllTopicRnMetadata(metadataClient, clusterUri);

        Set<TopicUriPartition> subscribedTopicUriPartitions = new HashSet<>();

        allTopicRnMetadata.forEach(
                (topicRn, topicRnMetadata) -> {
                    if (topicRnPattern.matcher(topicRn.toString()).matches()) {
                        subscribedTopicUriPartitions.addAll(topicRnMetadata.getTopicUriPartitions());
                    }
                });

        return subscribedTopicUriPartitions;
    }
}
