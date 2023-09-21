package com.pinterest.psc.utils;

import com.google.common.collect.Sets;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.SerializerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.integration.KafkaCluster;
import com.pinterest.psc.integration.consumer.PscConsumerRunnerResult;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.serde.StringSerializer;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import io.vavr.control.Either;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PscTestUtils {
    private static final PscLogger logger = PscLogger.getLogger(PscTestUtils.class);
    private final static int MAX_MESSAGE_IDS_TO_COMMIT = 5;

    public static <K, V> PscConsumerRunnerResult<K, V> assignAndConsume(
            PscConsumer pscConsumer,
            Set<TopicUriPartition> topicUriPartitions,
            int consumptionTimeout,
            Either<Map<TopicUriPartition, Long>, Set<MessageId>> seekPosition,
            boolean seekPositionIsTimestamp,
            boolean manuallyCommitMessageIds,
            boolean asyncCommit,
            boolean commitSelectMessageIds
    ) throws Exception {
        pscConsumer.assign(topicUriPartitions);
        if (seekPosition != null) {
            if (seekPosition.isLeft()) {
                // find potential seek-to-beginning and seek-to-end requests
                Map<TopicUriPartition, Long> topicUriPartitionToPosition = seekPosition.getLeft();
                Iterator<Map.Entry<TopicUriPartition, Long>> iterator = topicUriPartitionToPosition.entrySet().iterator();
                Set<TopicUriPartition> seekToBeginningPartitions = new HashSet<>();
                Set<TopicUriPartition> seekToEndPartitions = new HashSet<>();
                Map.Entry<TopicUriPartition, Long> entry;
                while (iterator.hasNext()) {
                    entry = iterator.next();
                    if (entry.getValue() == Long.MIN_VALUE) {
                        seekToBeginningPartitions.add(entry.getKey());
                        iterator.remove();
                    } else if (entry.getValue() == Long.MAX_VALUE) {
                        seekToEndPartitions.add(entry.getKey());
                        iterator.remove();
                    }
                }
                if (!topicUriPartitionToPosition.isEmpty()) {
                    if (seekPositionIsTimestamp)
                        pscConsumer.seekToTimestamp(topicUriPartitionToPosition);
                    else // seek position is offsets
                        pscConsumer.seekToOffset(topicUriPartitionToPosition);
                }
                if (!seekToBeginningPartitions.isEmpty())
                    pscConsumer.seekToBeginning(seekToBeginningPartitions);
                if (!seekToEndPartitions.isEmpty())
                    pscConsumer.seekToEnd(seekToEndPartitions);
            } else if (seekPosition.isRight())
                pscConsumer.seek(seekPosition.get());
        }
        List<PscConsumerMessage<K, V>> messageList = new ArrayList<>();
        PscConsumerMessage<K, V> message;
        MessageId messageId;
        //manuallyCommitMessageIds = manuallyCommitMessageIds &&
        //        !configuration.getBoolean(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED);
        Map<Integer, MessageId> committed = null;
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime <= consumptionTimeout) {
            PscConsumerPollMessageIterator<K, V> messages = pscConsumer.poll();
            assertTrue(pscConsumer.assignment().containsAll(messages.getTopicUriPartitions()));
            while (messages.hasNext()) {
                message = messages.next();
                messageList.add(message);
                messageId = message.getMessageId();
                if (manuallyCommitMessageIds) {
                    if (committed == null)
                        committed = new HashMap<>();
                    if (committed.size() < MAX_MESSAGE_IDS_TO_COMMIT && !committed.containsKey(messageId.getTopicUriPartition().getPartition()))
                        committed.put(messageId.getTopicUriPartition().getPartition(), messageId);
                }
            }
        }
        if (committed != null) {
            if (asyncCommit) {
                if (commitSelectMessageIds)
                    pscConsumer.commitAsync(Sets.newHashSet(committed.values()));
                else
                    pscConsumer.commitAsync();
            } else
                pscConsumer.commitSync(Sets.newHashSet(committed.values()));
        }

        Set<TopicUriPartition> assignment = pscConsumer.assignment();
        PscConsumerRunnerResult<K, V> pscConsumerRunnerResult = new PscConsumerRunnerResult<>(
                assignment,
                messageList,
                committed == null ? null : Sets.newHashSet(committed.values())
        );
        pscConsumer.unassign();
        pscConsumer.close();
        return pscConsumerRunnerResult;
    }

    public static <K, V> PscConsumerRunnerResult<K, V> subscribeAndConsume(
            PscConsumer pscConsumer,
            Set<String> topicUriStrs,
            int consumptionTimeout,
            Either<Map<TopicUriPartition, Long>, Set<MessageId>> seekPosition,
            boolean seekPositionIsTimestamp,
            boolean manuallyCommitMessageIds,
            boolean asyncCommit,
            boolean commitSelectMessageIds
    ) throws Exception {
        pscConsumer.subscribe(topicUriStrs);
        if (seekPosition != null) {
            if (seekPosition.isLeft()) {
                // find potential seek-to-beginning and seek-to-end requests
                Map<TopicUriPartition, Long> topicUriPartitionToPosition = seekPosition.getLeft();
                Iterator<Map.Entry<TopicUriPartition, Long>> iterator = topicUriPartitionToPosition.entrySet().iterator();
                Set<TopicUriPartition> seekToBeginningPartitions = new HashSet<>();
                Set<TopicUriPartition> seekToEndPartitions = new HashSet<>();
                Map.Entry<TopicUriPartition, Long> entry;
                while (iterator.hasNext()) {
                    entry = iterator.next();
                    if (entry.getValue() == Long.MIN_VALUE) {
                        seekToBeginningPartitions.add(entry.getKey());
                        iterator.remove();
                    } else if (entry.getValue() == Long.MAX_VALUE) {
                        seekToEndPartitions.add(entry.getKey());
                        iterator.remove();
                    }
                }
                if (!topicUriPartitionToPosition.isEmpty()) {
                    if (seekPositionIsTimestamp)
                        pscConsumer.seekToTimestamp(topicUriPartitionToPosition);
                    else // seek position is offsets
                        pscConsumer.seekToOffset(topicUriPartitionToPosition);
                }
                if (!seekToBeginningPartitions.isEmpty())
                    pscConsumer.seekToBeginning(seekToBeginningPartitions);
                if (!seekToEndPartitions.isEmpty())
                    pscConsumer.seekToEnd(seekToEndPartitions);
            } else if (seekPosition.isRight())
                pscConsumer.seek(seekPosition.get());
        }
        List<PscConsumerMessage<K, V>> messageList = new ArrayList<>();
        PscConsumerMessage<K, V> message;
        MessageId messageId;
        //manuallyCommitMessageIds = manuallyCommitMessageIds &&
        //        !configuration.getBoolean(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED);
        Map<Integer, MessageId> committed = null;
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime <= consumptionTimeout) {
            PscConsumerPollMessageIterator<K, V> messages = pscConsumer.poll();
            assertTrue(pscConsumer.assignment().containsAll(messages.getTopicUriPartitions()));
            while (messages.hasNext()) {
                message = messages.next();
                messageList.add(message);
                messageId = message.getMessageId();
                if (manuallyCommitMessageIds) {
                    if (committed == null)
                        committed = new HashMap<>();
                    if (committed.size() < MAX_MESSAGE_IDS_TO_COMMIT && !committed.containsKey(messageId.getTopicUriPartition().getPartition()))
                        committed.put(messageId.getTopicUriPartition().getPartition(), messageId);
                }
            }
        }
        if (committed != null) {
            if (asyncCommit) {
                if (commitSelectMessageIds)
                    pscConsumer.commitAsync(Sets.newHashSet(committed.values()));
                else
                    pscConsumer.commitAsync();
            } else
                pscConsumer.commitSync(Sets.newHashSet(committed.values()));
        }

        Set<TopicUriPartition> assignment = pscConsumer.assignment();
        PscConsumerRunnerResult<K, V> pscConsumerRunnerResult = new PscConsumerRunnerResult<>(
                assignment,
                messageList,
                committed == null ? null : Sets.newHashSet(committed.values())
        );
        pscConsumer.unsubscribe();
        pscConsumer.close();
        return pscConsumerRunnerResult;
    }

    public static void produceKafkaMessages(
            int count,
            SharedKafkaTestResource sharedKafkaTestResource,
            KafkaCluster kafkaCluster,
            String topic
    ) throws SerializerException {
        produceKafkaMessages(count, 0, sharedKafkaTestResource, kafkaCluster, topic, null);
    }

    public static void produceKafkaMessages(
            int count,
            int startingId,
            SharedKafkaTestResource sharedKafkaTestResource,
            KafkaCluster kafkaCluster,
            String topic
    ) throws SerializerException {
        produceKafkaMessages(count, startingId, sharedKafkaTestResource, kafkaCluster, topic, null, null);
    }

    public static void produceKafkaMessages(
            int count,
            SharedKafkaTestResource sharedKafkaTestResource,
            KafkaCluster kafkaCluster,
            String topic,
            Properties properties
    ) throws SerializerException {
        produceKafkaMessages(count, 0, sharedKafkaTestResource, kafkaCluster, topic, null, properties);
    }

    public static void produceKafkaMessages(
            int count,
            SharedKafkaTestResource sharedKafkaTestResource,
            KafkaCluster kafkaCluster,
            String topic,
            Integer partition
    ) throws SerializerException {
        produceKafkaMessages(count, 0, sharedKafkaTestResource, kafkaCluster, topic, partition);
    }

    public static void produceKafkaMessages(
            int count,
            int startingId,
            SharedKafkaTestResource sharedKafkaTestResource,
            KafkaCluster kafkaCluster,
            String topic,
            Integer partition
    ) throws SerializerException {
        produceKafkaMessages(count, startingId, sharedKafkaTestResource, kafkaCluster, topic, partition, new Properties());
    }

    public static void produceKafkaMessages(
            int count,
            int startingId,
            SharedKafkaTestResource sharedKafkaTestResource,
            KafkaCluster kafkaCluster,
            String topic,
            Integer partition,
            Properties properties
    ) throws SerializerException {
        if (properties == null)
            properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrap());
        KafkaProducer<byte[], byte[]> kafkaProducer = sharedKafkaTestResource.getKafkaTestUtils().getKafkaProducer(
                ByteArraySerializer.class, ByteArraySerializer.class, properties
        );

        StringSerializer pscStringSerializer = new StringSerializer();
        for (int i = startingId; i < startingId + count; ++i) {
            byte[] keyBytes = pscStringSerializer.serialize("" + i);
            byte[] valueBytes = pscStringSerializer.serialize("" + i);
            ProducerRecord<byte[], byte[]> producerRecord = partition == null ?
                    new ProducerRecord<>(topic, keyBytes, valueBytes) :
                    new ProducerRecord<>(topic, partition, keyBytes, valueBytes);
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    // assumes count <= num partitions in messages
    public static Set<MessageId> getRandomMessageIdsWithDistinctPartitions(
            List<PscConsumerMessage<String, String>> messages,
            int count
    ) {
        Set<MessageId> messageIds = new HashSet<>();
        int messageCount = messages.size();
        Random random = new Random();
        while (messageIds.size() < count) {
            MessageId nextMessageId = messages.get(random.nextInt(messageCount)).getMessageId();
            boolean duplicatePartition = false;
            for (MessageId messageId : messageIds) {
                if (nextMessageId.getTopicUriPartition().getTopicUriAsString().equals(messageId.getTopicUriPartition().getTopicUriAsString()) &&
                        nextMessageId.getTopicUriPartition().getPartition() == messageId.getTopicUriPartition().getPartition())
                    duplicatePartition = true;
            }
            if (!duplicatePartition)
                messageIds.add(nextMessageId);
        }
        return messageIds;
    }

    public static boolean verifyMessagesStartWithMessageIds(
            List<PscConsumerMessage<String, String>> messages,
            Set<MessageId> messageIds
    ) {
        int countMatched = 0;
        int expectedCount = messageIds.size();
        for (PscConsumerMessage<String, String> message : messages) {
            Iterator<MessageId> messageIdIterator = messageIds.iterator();
            while (messageIdIterator.hasNext()) {
                MessageId messageId = messageIdIterator.next();
                if (message.getMessageId().getTopicUriPartition().getTopicUriAsString().equals(messageId.getTopicUriPartition().getTopicUriAsString()) &&
                        message.getMessageId().getTopicUriPartition().getPartition() == messageId.getTopicUriPartition().getPartition()) {
                    // the message id we seek to represents the last consumed message, so the next consumed offset is 1 plus that
                    assertEquals(messageId.getOffset() + 1, message.getMessageId().getOffset());
                    ++countMatched;
                    messageIdIterator.remove();
                }
            }
            if (messageIds.isEmpty() && countMatched == expectedCount)
                return true;
        }
        return false;
    }

    public static ConsumerRecord consumeKafkaMessages(
            int count,
            SharedKafkaTestResource sharedKafkaTestResource,
            KafkaCluster kafkaCluster,
            String topic3,
            int partition,
            long lastConsumedOffset
    ) {
        if (count < 1)
            return null;

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrap());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-kafka-group");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        KafkaConsumer<byte[], byte[]> kafkaConsumer = sharedKafkaTestResource.getKafkaTestUtils().getKafkaConsumer(
                ByteArrayDeserializer.class, ByteArrayDeserializer.class, properties
        );
        TopicPartition topicPartition = new TopicPartition(topic3, partition);
        kafkaConsumer.assign(Collections.singleton(topicPartition));
        if (lastConsumedOffset >= 0)
            kafkaConsumer.seek(topicPartition, lastConsumedOffset + 1);
        else
            kafkaConsumer.seekToBeginning(Collections.singleton(topicPartition));

        int consumed = 0;
        long offset = -1;
        ConsumerRecord<byte[], byte[]> message = null;
        while (consumed < count) {
            ConsumerRecords<byte[], byte[]> messages = kafkaConsumer.poll(Duration.ofMillis(50));
            if (!messages.isEmpty()) {
                assertEquals(1, messages.count());
                message = messages.iterator().next();
                kafkaConsumer.commitSync();
                offset = message.offset();
                // Kafka consumer committed offset is the next offset to fetch
                assertEquals(1 + offset, kafkaConsumer.committed(topicPartition).offset());
                // Kafka consumer position is the next offset to fetch
                assertEquals(1 + offset, kafkaConsumer.position(topicPartition));
                ++consumed;
            }
        }
        kafkaConsumer.close();
        assertEquals(lastConsumedOffset + count, offset);
        return message;
    }

    public static PscConsumerMessage consumePscMessages(
            int count,
            TopicUriPartition topicUriPartition,
            long lastConsumedOffset
    ) throws ConfigurationException, ConsumerException {
        if (count < 1)
            return null;

        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "test-psc-consumer");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-psc-group");
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, com.pinterest.psc.serde.ByteArrayDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, com.pinterest.psc.serde.ByteArrayDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_POLL_MESSAGES_MAX, "1");

        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.assign(Collections.singleton(topicUriPartition));
        if (lastConsumedOffset >= 0) {
            MessageId messageId = new MessageId(topicUriPartition, lastConsumedOffset);
            pscConsumer.seek(messageId);
        } else
            pscConsumer.seekToBeginning(Collections.singleton(topicUriPartition));

        int consumed = 0;
        long offset = -1;
        PscConsumerMessage<String, String> message = null;
        while (consumed < count) {
            PscConsumerPollMessageIterator<String, String> messagesIt = pscConsumer.poll();
            if (messagesIt.hasNext()) {
                message = messagesIt.next();
                assertFalse(messagesIt.hasNext());
                pscConsumer.commitSync();
                offset = message.getMessageId().getOffset();
                // PSC consumer committed offset is the next offset to fetch
                assertEquals(1 + offset, pscConsumer.committed(topicUriPartition).getOffset());
                // PSC consumer position is the next offset to fetch
                assertEquals(1 + offset, pscConsumer.position(topicUriPartition));
                ++consumed;
            }
        }
        pscConsumer.close();
        assertEquals(lastConsumedOffset + count, offset);
        return message;
    }

    public static void createTopicAndVerify(SharedKafkaTestResource testResource, String topic, int partitions)
            throws InterruptedException {
        logger.info("Going to create topic {}.", topic);
        testResource.getKafkaTestUtils().createTopic(topic, partitions, (short) 1);
        while (!testResource.getKafkaTestUtils().getTopicNames().contains(topic)) {
            Thread.sleep(200);
        }
        logger.info("Topic {} was created.", topic);
    }

    public static void deleteTopicAndVerify(SharedKafkaTestResource testResource, String topic)
            throws InterruptedException, ExecutionException {
        logger.info("Going to delete topic {}.", topic);
        testResource.getKafkaTestUtils().getAdminClient().deleteTopics(Collections.singletonList(topic)).all().get();
        while (testResource.getKafkaTestUtils().getTopicNames().contains(topic)) {
            Thread.sleep(200);
        }
        logger.info("Topic {} was deleted.", topic);
    }
}
