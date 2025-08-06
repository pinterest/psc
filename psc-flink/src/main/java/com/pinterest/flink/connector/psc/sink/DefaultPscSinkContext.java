/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.connector.psc.sink;

import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.producer.PscProducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Context providing information to assist constructing a {@link
 * com.pinterest.psc.producer.PscProducerMessage}.
 */
public class DefaultPscSinkContext implements PscRecordSerializationSchema.PscSinkContext {

    private final int subtaskId;
    private final int numberOfParallelInstances;
    private final PscConfiguration pscProducerConfig;

    private final Map<String, int[]> cachedPartitions = new HashMap<>();

    public DefaultPscSinkContext(
            int subtaskId, int numberOfParallelInstances, PscConfiguration pscProducerConfig) {
        this.subtaskId = subtaskId;
        this.numberOfParallelInstances = numberOfParallelInstances;
        this.pscProducerConfig = pscProducerConfig;
    }

    @Override
    public int getParallelInstanceId() {
        return subtaskId;
    }

    @Override
    public int getNumberOfParallelInstances() {
        return numberOfParallelInstances;
    }

    @Override
    public int[] getPartitionsForTopicUri(String topicUri) {
        return cachedPartitions.computeIfAbsent(topicUri, k -> {
            try {
                return fetchPartitionsForTopic(topicUri);
            } catch (ConfigurationException | ProducerException | IOException e) {
                throw new RuntimeException("Failed to get partitions for topicUri " + topicUri, e);
            }
        });
    }

    private int[] fetchPartitionsForTopic(String topicUri) throws ConfigurationException, ProducerException, IOException {
        try (final PscProducer<?, ?> producer = new PscProducer<>(pscProducerConfig)) {
            // the fetched list is immutable, so we're creating a mutable copy in order to sort
            // it
            final List<TopicUriPartition> partitionsList =
                    new ArrayList<>(producer.getPartitions(topicUri));

            return partitionsList.stream()
                    .sorted(Comparator.comparing(TopicUriPartition::getPartition))
                    .map(TopicUriPartition::getPartition)
                    .mapToInt(Integer::intValue)
                    .toArray();
        }
    }
}
