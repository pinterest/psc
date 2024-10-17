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

package com.pinterest.flink.connector.psc.sink.testutils;

import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/** Kafka dataStream data reader. */
public class PscDataReader implements ExternalSystemDataReader<String> {
    private final PscConsumer<String, String> consumer;

    public PscDataReader(Properties properties, Collection<TopicUriPartition> partitions) throws ConfigurationException, ConsumerException {
        this.consumer = new PscConsumer<>(PscConfigurationUtils.propertiesToPscConfiguration(properties));
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
    }

    @Override
    public List<String> poll(Duration timeout) {
        List<String> result = new LinkedList<>();
        PscConsumerPollMessageIterator<String, String> consumerRecords;
        try {
            consumerRecords = consumer.poll(timeout);
        } catch (ConsumerException we) {
            return Collections.emptyList();
        }
        while (consumerRecords.hasNext()) {
            result.add(consumerRecords.next().getValue());
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
    }
}
