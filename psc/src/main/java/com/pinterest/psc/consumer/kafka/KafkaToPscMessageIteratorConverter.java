package com.pinterest.psc.consumer.kafka;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.PscMessage;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaMessageId;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.consumer.ToPscMessageIteratorConverter;
import com.pinterest.psc.interceptor.ConsumerInterceptors;
import com.pinterest.psc.logging.PscLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaToPscMessageIteratorConverter<K, V> extends ToPscMessageIteratorConverter<K, V> {
    private static final PscLogger logger = PscLogger.getLogger(KafkaToPscMessageIteratorConverter.class);
    private final Iterator<ConsumerRecord<byte[], byte[]>> kafkaConsumerRecordIterator;
    private final Map<TopicPartition, Iterator<ConsumerRecord<byte[], byte[]>>> perTopicPartitionKafkaConsumerRecordIterator;
    private final Set<TopicUriPartition> topicUriPartitions;
    private final Map<String, TopicUri> kafkaTopicToTopicUri;

    public KafkaToPscMessageIteratorConverter(
            Iterator<ConsumerRecord<byte[], byte[]>> kafkaConsumerRecordIterator,
            Map<TopicPartition, Iterator<ConsumerRecord<byte[], byte[]>>> perTopicPartitionKafkaConsumerRecordIterator,
            Set<TopicPartition> topicPartitions,
            Map<String, TopicUri> kafkaTopicToTopicUri,
            ConsumerInterceptors<K, V> consumerInterceptors
    ) {
        super(consumerInterceptors);
        this.kafkaConsumerRecordIterator = kafkaConsumerRecordIterator;
        this.perTopicPartitionKafkaConsumerRecordIterator = perTopicPartitionKafkaConsumerRecordIterator;
        this.kafkaTopicToTopicUri = kafkaTopicToTopicUri;
        this.topicUriPartitions = topicPartitions.stream().map(topicPartition -> {
            TopicUriPartition topicUriPartition = new TopicUriPartition(
                    kafkaTopicToTopicUri.get(topicPartition.topic()).getTopicUriAsString(),
                    topicPartition.partition()
            );
            BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, kafkaTopicToTopicUri.get(topicPartition.topic()));
            return topicUriPartition;
        }).collect(Collectors.toSet());
    }

    @Override
    public boolean hasNext() {
        return kafkaConsumerRecordIterator.hasNext();
    }

    @Override
    public PscConsumerPollMessageIterator<K, V> iteratorFor(TopicUriPartition topicUriPartition) {
        TopicPartition topicPartition = new TopicPartition(topicUriPartition.getTopicUri().getTopic(), topicUriPartition.getPartition());
        return new KafkaToPscMessageIteratorConverter<>(
                perTopicPartitionKafkaConsumerRecordIterator == null ?
                        emptyConsumerRecordIterator() :
                        perTopicPartitionKafkaConsumerRecordIterator.getOrDefault(
                                topicPartition,
                                emptyConsumerRecordIterator()
                        ),
                Collections.emptyMap(),
                Collections.singleton(topicPartition),
                kafkaTopicToTopicUri,
                consumerInterceptors
        );
    }

    @Override
    public Set<TopicUriPartition> getTopicUriPartitions() {
        return topicUriPartitions;
    }

    @Override
    protected PscConsumerMessage<byte[], byte[]> getNextBackendMessage() {
        ConsumerRecord<byte[], byte[]> kafkaConsumerRecord = kafkaConsumerRecordIterator.next();
        return convertKafkaConsumerRecordToPscConsumerMessage(kafkaConsumerRecord);
    }

    private PscConsumerMessage<byte[], byte[]> convertKafkaConsumerRecordToPscConsumerMessage(
            ConsumerRecord<byte[], byte[]> kafkaConsumerRecord
    ) {
        TopicUri topicUri = kafkaTopicToTopicUri.get(kafkaConsumerRecord.topic());
        TopicUriPartition topicUriPartition = new TopicUriPartition(
                topicUri.getTopicUriAsString(),
                kafkaConsumerRecord.partition()
        );
        BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);
        KafkaMessageId messageId = new KafkaMessageId(
                topicUriPartition,
                kafkaConsumerRecord.offset(),
                kafkaConsumerRecord.timestamp(),
                kafkaConsumerRecord.serializedKeySize(),
                kafkaConsumerRecord.serializedValueSize()
        );
        PscConsumerMessage<byte[], byte[]> pscConsumerMessage = new PscConsumerMessage<>(
                messageId,
                kafkaConsumerRecord.key(),
                kafkaConsumerRecord.value(),
                kafkaConsumerRecord.timestamp()
        );

        kafkaConsumerRecord.headers().forEach(header -> pscConsumerMessage.setHeader(header.key(), header.value()));

        int keySize = messageId.getSerializedKeySizeBytes();
        int valueSize = messageId.getSerializedValueSizeBytes();

        if (pscConsumerMessage.getHeaders() != null) {
            if (pscConsumerMessage.getHeaders().containsKey(PscMessage.PSC_MESSAGE_HEADER_KEY_SIZE_BYTES) &&
                    PscCommon.byteArrayToInt(pscConsumerMessage.getHeader(PscMessage.PSC_MESSAGE_HEADER_KEY_SIZE_BYTES)) != keySize) {
                logger.warn("[Kafka] Size of consumed message key does not match size of key from header!");
                pscConsumerMessage.addTag(PscConsumerMessage.DefaultPscConsumerMessageTags.KEY_SIZE_MISMATCH_WITH_KEY_SIZE_HEADER);
                pscConsumerMessage.setHeader(PscMessage.PSC_MESSAGE_HEADER_KEY_SIZE_BYTES, PscCommon.intToByteArray(keySize));
            }
            if (pscConsumerMessage.getHeaders().containsKey(PscMessage.PSC_MESSAGE_HEADER_VALUE_SIZE_BYTES) &&
                    PscCommon.byteArrayToInt(pscConsumerMessage.getHeader(PscMessage.PSC_MESSAGE_HEADER_VALUE_SIZE_BYTES)) != valueSize) {
                logger.warn("[Kafka] Size of consumed message value does not match size of value from header!");
                pscConsumerMessage.addTag(PscConsumerMessage.DefaultPscConsumerMessageTags.VALUE_SIZE_MISMATCH_WITH_VALUE_SIZE_HEADER);
                pscConsumerMessage.setHeader(PscMessage.PSC_MESSAGE_HEADER_VALUE_SIZE_BYTES, PscCommon.intToByteArray(valueSize));
            }
        } else {
            pscConsumerMessage.setHeader(PscMessage.PSC_MESSAGE_HEADER_KEY_SIZE_BYTES, PscCommon.intToByteArray(keySize));
            pscConsumerMessage.setHeader(PscMessage.PSC_MESSAGE_HEADER_VALUE_SIZE_BYTES, PscCommon.intToByteArray(valueSize));
        }

        return pscConsumerMessage;
    }

    private static <K, V> Iterator<ConsumerRecord<K, V>> emptyConsumerRecordIterator() {
        return Collections.emptyIterator();
    }

    @Override
    public void close() throws IOException {
        // do nothing since Kafka iterators are not closeable
    }
}
