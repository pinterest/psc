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
import com.pinterest.psc.metadata.TopicUriMetadata;
import com.pinterest.psc.metadata.client.PscMetadataClient;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** The base implementations of {@link PscSubscriber}. */
class PscSubscriberUtils {

    private PscSubscriberUtils() {}

    static Map<TopicUri, TopicUriMetadata> getAllTopicUriMetadata(PscMetadataClient metadataClient, TopicUri clusterUri) {
        try {
            List<TopicRn> allTopicRns = metadataClient.listTopicRns(clusterUri, Duration.ofMillis(Long.MAX_VALUE));
            return getTopicUriMetadata(metadataClient, clusterUri, allTopicRns.stream().map(rn -> new BaseTopicUri(clusterUri.getProtocol(), rn)).collect(Collectors.toList()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get metadata for all topics.", e);
        }
    }

    static Map<TopicUri, TopicUriMetadata> getTopicUriMetadata(
            PscMetadataClient metadataClient, TopicUri clusterUri, List<TopicUri> topicUris) {
        try {
            return metadataClient.describeTopicUris(clusterUri, new HashSet<>(topicUris), Duration.ofMillis(Long.MAX_VALUE));
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to get metadata for topicUris %s.", topicUris), e);
        }
    }
}
