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

package com.pinterest.flink.connector.psc.testutils;

import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

/** Source split data writer for writing test data into Kafka topic partitions. */
public class PscTopicUriPartitionDataWriter implements ExternalSystemSplitDataWriter<String> {

    private final PscProducer<byte[], byte[]> pscProducer;
    private final TopicUriPartition topicPartition;

    public PscTopicUriPartitionDataWriter(Properties producerProperties, TopicUriPartition topicPartition) throws ConfigurationException, ProducerException {
        this.pscProducer = new PscProducer<>(PscConfigurationUtils.propertiesToPscConfiguration(producerProperties));
        this.topicPartition = topicPartition;
    }

    @Override
    public void writeRecords(List<String> records) {
        for (String record : records) {
            PscProducerMessage<byte[], byte[]> producerRecord =
                    new PscProducerMessage<>(
                            topicPartition.getTopicUriAsString(),
                            topicPartition.getPartition(),
                            null,
                            record.getBytes(StandardCharsets.UTF_8));
            try {
                pscProducer.send(producerRecord);
            } catch (ProducerException | ConfigurationException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            pscProducer.flush();
        } catch (ProducerException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        pscProducer.close();
    }

    public TopicUriPartition getTopicUriPartition() {
        return topicPartition;
    }
}
