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
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.serde.ByteArrayDeserializer;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/** Collection of methods to interact with a Kafka cluster. */
public class PscUtil {

    private static final Logger LOG = LoggerFactory.getLogger(PscUtil.class);
    private static final Duration CONSUMER_POLL_DURATION = Duration.ofSeconds(1);

    private PscUtil() {}

    /**
     * This method helps to set commonly used Kafka configurations and aligns the internal Kafka log
     * levels with the ones used by the capturing logger.
     *
     * @param dockerImageVersion describing the Kafka image
     * @param logger to derive the log level from
     * @return configured Kafka container
     */
    public static KafkaContainer createKafkaContainer(String dockerImageVersion, Logger logger) {
        return createKafkaContainer(dockerImageVersion, logger, null);
    }

    /**
     * This method helps to set commonly used Kafka configurations and aligns the internal Kafka log
     * levels with the ones used by the capturing logger, and set the prefix of logger.
     */
    public static KafkaContainer createKafkaContainer(
            String dockerImageVersion, Logger logger, String loggerPrefix) {
        String logLevel;
        if (logger.isTraceEnabled()) {
            logLevel = "TRACE";
        } else if (logger.isDebugEnabled()) {
            logLevel = "DEBUG";
        } else if (logger.isInfoEnabled()) {
            logLevel = "INFO";
        } else if (logger.isWarnEnabled()) {
            logLevel = "WARN";
        } else if (logger.isErrorEnabled()) {
            logLevel = "ERROR";
        } else {
            logLevel = "OFF";
        }

        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
        if (!StringUtils.isNullOrWhitespaceOnly(loggerPrefix)) {
            logConsumer.withPrefix(loggerPrefix);
        }
        return new KafkaContainer(DockerImageName.parse(dockerImageVersion))
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
                .withEnv("KAFKA_LOG4J_ROOT_LOGLEVEL", logLevel)
                .withEnv("KAFKA_LOG4J_LOGGERS", "state.change.logger=" + logLevel)
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
                .withEnv(
                        "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                        String.valueOf(Duration.ofHours(2).toMillis()))
                .withEnv("KAFKA_LOG4J_TOOLS_ROOT_LOGLEVEL", logLevel)
                .withLogConsumer(logConsumer);
    }

    /**
     * Drain all records available from the given topic from the beginning until the current highest
     * offset.
     *
     * <p>This method will fetch the latest offsets for the partitions once and only return records
     * until that point.
     *
     * @param topic to fetch from
     * @param properties used to configure the created {@link PscConsumer}
     * @param committed determines the mode {@link PscConfiguration#PSC_CONSUMER_ISOLATION_LEVEL} with which
     *     the consumer reads the records.
     * @return all {@link PscConsumerMessage} in the topic
     */
    public static List<PscConsumerMessage<byte[], byte[]>> drainAllRecordsFromTopic(
            String topic, Properties properties, boolean committed) throws ConfigurationException, ConsumerException {
        final Properties consumerConfig = new Properties();
        consumerConfig.putAll(properties);
        consumerConfig.put(
                PscConfiguration.PSC_CONSUMER_ISOLATION_LEVEL,
                committed ? PscConfiguration.PSC_CONSUMER_ISOLATION_LEVEL_TRANSACTIONAL : PscConfiguration.PSC_CONSUMER_ISOLATION_LEVEL_NON_TRANSACTIONAL);
        return drainAllRecordsFromTopic(topic, consumerConfig);
    }

    /**
     * Drain all records available from the given topic from the beginning until the current highest
     * offset.
     *
     * <p>This method will fetch the latest offsets for the partitions once and only return records
     * until that point.
     *
     * @param topic to fetch from
     * @param properties used to configure the created {@link PscConsumer}
     * @return all {@link PscConsumerMessage} in the topic
     */
    public static List<PscConsumerMessage<byte[], byte[]>> drainAllRecordsFromTopic(
            String topic, Properties properties) throws ConfigurationException, ConsumerException {
        final Properties consumerConfig = new Properties();
        consumerConfig.putAll(properties);
        consumerConfig.put(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, ByteArrayDeserializer.class.getName());
        consumerConfig.put(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, ByteArrayDeserializer.class.getName());
        try (PscConsumer<byte[], byte[]> consumer = new PscConsumer<>(PscConfigurationUtils.propertiesToPscConfiguration(consumerConfig))) {
            Set<TopicUriPartition> topicPartitions = getAllPartitions(consumer, topic);
            Map<TopicUriPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
            consumer.assign(topicPartitions);
            consumer.seekToBeginning(topicPartitions);

            final List<PscConsumerMessage<byte[], byte[]>> consumerRecords = new ArrayList<>();
            while (!topicPartitions.isEmpty()) {
                PscConsumerPollMessageIterator<byte[], byte[]> records = consumer.poll(CONSUMER_POLL_DURATION);
//                LOG.debug("Fetched {} records from topic {}.", records.count(), topic);

                // Remove partitions from polling which have reached its end.
                final List<TopicUriPartition> finishedPartitions = new ArrayList<>();
                for (final TopicUriPartition topicPartition : topicPartitions) {
                    final long position = consumer.position(topicPartition);
                    final long endOffset = endOffsets.get(topicPartition);
                    LOG.debug(
                            "Endoffset {} and current position {} for partition {}",
                            endOffset,
                            position,
                            topicPartition.getPartition());
                    if (endOffset - position > 0) {
                        continue;
                    }
                    finishedPartitions.add(topicPartition);
                }
                if (topicPartitions.removeAll(finishedPartitions)) {
                    consumer.assign(topicPartitions);
                }
                consumerRecords.addAll(records.asList());
            }
            return consumerRecords;
        }
    }

    private static Set<TopicUriPartition> getAllPartitions(
            PscConsumer<byte[], byte[]> consumer, String topic) throws ConfigurationException, ConsumerException {
        return consumer.getPartitions(topic);
    }
}
