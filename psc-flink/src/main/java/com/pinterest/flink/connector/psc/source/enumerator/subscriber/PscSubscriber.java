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

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Kafka consumer allows a few different ways to consume from the topics, including:
 *
 * <ol>
 *   <li>Subscribe from a collection of topics.
 *   <li>Subscribe to a topic pattern using Java {@code Regex}.
 *   <li>Assign specific partitions.
 * </ol>
 *
 * <p>The KafkaSubscriber provides a unified interface for the Kafka source to support all these
 * three types of subscribing mode.
 */
@Internal
public interface PscSubscriber extends Serializable {

    /**
     * Get a set of subscribed {@link TopicUriPartition}s.
     *
     * @param metadataClient The admin client used to retrieve subscribed topic partitions.
     * @return A set of subscribed {@link TopicUriPartition}s
     */
    Set<TopicUriPartition> getSubscribedTopicUriPartitions(PscMetadataClient metadataClient, TopicUri clusterUri);

    // ----------------- factory methods --------------

    static PscSubscriber getTopicUriListSubscriber(List<String> topicUris) {
        return new PscTopicUriListSubscriber(topicUris);
    }

    static PscSubscriber getTopicPatternSubscriber(Pattern topicUriPattern) {
        // TODO: should this be topic pattern or topic uri pattern?
        return new TopicUriPatternSubscriber(topicUriPattern);
    }

    static PscSubscriber getPartitionSetSubscriber(Set<TopicUriPartition> partitions) {
        return new TopicUriPartitionSetSubscriber(partitions);
    }
}
