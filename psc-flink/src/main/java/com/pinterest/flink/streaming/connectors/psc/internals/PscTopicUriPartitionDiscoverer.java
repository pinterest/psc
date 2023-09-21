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

package com.pinterest.flink.streaming.connectors.psc.internals;

import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import org.apache.flink.annotation.Internal;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A topic URI partition discoverer that can be used to discover topic URIs and partitions metadata
 * from backend pubsub via the PSC consumer API.
 *
 * Note: getAllTopicUris() is currently unsupported, see details in method docs
 */
@Internal
public class PscTopicUriPartitionDiscoverer extends AbstractTopicUriPartitionDiscoverer {

    private final Properties pscConsumerProperties;

    private PscConsumer<?, ?> pscConsumer;

    public PscTopicUriPartitionDiscoverer(
            PscTopicUrisDescriptor topicUrisDescriptor,
            int indexOfThisSubtask,
            int numParallelSubtasks,
            Properties properties) {

        super(topicUrisDescriptor, indexOfThisSubtask, numParallelSubtasks);
        this.pscConsumerProperties = checkNotNull(properties);
    }

    @Override
    protected void initializeConnections() throws ConfigurationException, ConsumerException {
        this.pscConsumer = PscConsumerThread.getConsumer(pscConsumerProperties);
    }

    /**
     * Note: Topic discovery is currently unsupported
     *
     * @return empty ArrayList due to currently unsupported operation
     * @throws WakeupException
     */
    @Override
    protected List<String> getAllTopicUris() throws WakeupException {
        try {
            return new ArrayList<>();
        } catch (Exception e) {
            //} catch (com.pinterest.psc.exception.consumer.WakeupException e) {
            // rethrow our own wakeup exception
            throw new WakeupException();
        }
    }

    @Override
    protected List<PscTopicUriPartition> getAllPartitionsForTopicUris(List<String> topicUris) throws WakeupException, RuntimeException {
        final List<PscTopicUriPartition> pscTopicUriPartitions = new LinkedList<>();

        try {
            for (String topicUri : topicUris) {
                final Set<TopicUriPartition> topicUriPartitions = pscConsumer.getPartitions(topicUri);

                if (topicUriPartitions == null) {
                    throw new RuntimeException(String.format("Could not fetch partitions for %s. Make sure that the topic exists.", topicUri));
                }

                for (TopicUriPartition topicUriPartition : topicUriPartitions) {
                    pscTopicUriPartitions.add(new PscTopicUriPartition(topicUriPartition.getTopicUriAsString(), topicUriPartition.getPartition()));
                }
            }
        } catch (com.pinterest.psc.exception.consumer.WakeupException e) {
            throw new WakeupException();
        } catch (ConsumerException | ConfigurationException e) {
            throw new RuntimeException(e);
        }

        return pscTopicUriPartitions;
    }

    @Override
    protected void wakeupConnections() {
        if (this.pscConsumer != null) {
            this.pscConsumer.wakeup();
        }
    }

    @Override
    protected void closeConnections() throws Exception {
        if (this.pscConsumer != null) {
            this.pscConsumer.close();

            // de-reference the consumer to avoid closing multiple times
            this.pscConsumer = null;
        }
    }

}
