package com.pinterest.psc.metadata.client.kafka;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metadata.MetadataUtils;
import com.pinterest.psc.metadata.TopicRnMetadata;
import com.pinterest.psc.metadata.client.PscBackendMetadataClient;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * A Kafka-specific implementation of the {@link PscBackendMetadataClient}.
 */
public class PscKafkaMetadataClient extends PscBackendMetadataClient {

    private static final PscLogger logger = PscLogger.getLogger(PscKafkaMetadataClient.class);
    private AdminClient kafkaAdminClient;

    @Override
    public void initialize(
            TopicUri topicUri,
            Environment env,
            PscConfigurationInternal pscConfigurationInternal
    ) throws ConfigurationException {
        super.initialize(topicUri, env, pscConfigurationInternal);
        Properties properties = PscConfigurationUtils.pscConfigurationInternalToProperties(pscConfigurationInternal);
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, discoveryConfig.getConnect());
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, pscConfigurationInternal.getMetadataClientId());
        this.kafkaAdminClient = AdminClient.create(properties);
        logger.info("Initialized Kafka AdminClient with properties: " + properties);
    }

    @Override
    public List<TopicRn> listTopicRns(Duration duration)
            throws ExecutionException, InterruptedException, TimeoutException {
        ListTopicsResult listTopicsResult = kafkaAdminClient.listTopics();
        Collection<TopicListing> topicListing = listTopicsResult.listings().get(duration.toMillis(), TimeUnit.MILLISECONDS);
        return topicListing.stream().map(tl -> MetadataUtils.createTopicRn(topicUri, tl.name())).collect(Collectors.toList());
    }

    @Override
    public Map<TopicRn, TopicRnMetadata> describeTopicRns(
            Collection<TopicRn> topicRns,
            Duration duration
    ) throws ExecutionException, InterruptedException, TimeoutException {
        Collection<String> topicNames = topicRns.stream().map(TopicRn::getTopic).collect(Collectors.toSet());
        Map<String, TopicDescription> topicMetadata = kafkaAdminClient.describeTopics(topicNames).all().get(duration.toMillis(), TimeUnit.MILLISECONDS);
        Map<TopicRn, TopicRnMetadata> result = new HashMap<>();
        for (Map.Entry<String, TopicDescription> entry : topicMetadata.entrySet()) {
            String topicName = entry.getKey();
            TopicDescription description = entry.getValue();
            TopicRn topicRn = MetadataUtils.createTopicRn(topicUri, topicName);
            List<TopicUriPartition> topicUriPartitions = new ArrayList<>();
            for (TopicPartitionInfo partitionInfo : description.partitions()) {
                topicUriPartitions.add(
                        createKafkaTopicUriPartition(topicRn, partitionInfo.partition())
                );
            }
            result.put(topicRn, new TopicRnMetadata(topicRn, topicUriPartitions));
        }
        return result;
    }

    @Override
    public Map<TopicUriPartition, Long> listOffsets(
            Map<TopicUriPartition, PscMetadataClient.MetadataClientOption> topicUriPartitionsAndOptions,
            Duration duration
    ) throws ExecutionException, InterruptedException, TimeoutException {
        Map<TopicPartition, OffsetSpec> topicPartitionOffsets = new HashMap<>();
        for (Map.Entry<TopicUriPartition, PscMetadataClient.MetadataClientOption> entry : topicUriPartitionsAndOptions.entrySet()) {
            TopicUriPartition topicUriPartition = entry.getKey();
            PscMetadataClient.MetadataClientOption option = entry.getValue();
            OffsetSpec offsetSpec;
            if (option == PscMetadataClient.MetadataClientOption.OFFSET_SPEC_EARLIEST)
                offsetSpec = OffsetSpec.earliest();
            else if (option == PscMetadataClient.MetadataClientOption.OFFSET_SPEC_LATEST)
                offsetSpec = OffsetSpec.latest();
            else
                throw new IllegalArgumentException("Unsupported MetadataClientOption for listOffsets(): " + option);
            topicPartitionOffsets.put(
                    new TopicPartition(topicUriPartition.getTopicUri().getTopic(), topicUriPartition.getPartition()), offsetSpec);
        }
        return listOffsetsInternal(topicPartitionOffsets, duration);
    }

    private Map<TopicUriPartition, Long> listOffsetsInternal(
            Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpecMap,
            Duration duration
    ) throws ExecutionException, InterruptedException, TimeoutException {
        ListOffsetsResult listOffsetsResult = kafkaAdminClient.listOffsets(topicPartitionOffsetSpecMap);
        Map<TopicUriPartition, Long> result = new HashMap<>();
        listOffsetsResult.all().get(duration.toMillis(), TimeUnit.MILLISECONDS).entrySet().forEach(e -> {
            TopicPartition tp = e.getKey();
            ListOffsetsResult.ListOffsetsResultInfo info = e.getValue();
            TopicRn topicRn = MetadataUtils.createTopicRn(topicUri, tp.topic());
            result.put(
                    createKafkaTopicUriPartition(topicRn, tp.partition()),
                    info.offset()
            );
        });
        return result;
    }

    @Override
    public Map<TopicUriPartition, Long> listOffsetsForTimestamps(
            Map<TopicUriPartition, Long> topicUriPartitionsAndTimes,
            Duration duration
    ) throws ExecutionException, InterruptedException, TimeoutException {
        Map<TopicPartition, OffsetSpec> topicPartitionTimes = new HashMap<>();
        for (Map.Entry<TopicUriPartition, Long> entry : topicUriPartitionsAndTimes.entrySet()) {
            TopicUriPartition topicUriPartition = entry.getKey();
            Long time = entry.getValue();
            topicPartitionTimes.put(
                    new TopicPartition(topicUriPartition.getTopicUri().getTopic(), topicUriPartition.getPartition()), OffsetSpec.forTimestamp(time));
        }
        return listOffsetsInternal(topicPartitionTimes, duration);
    }

    @Override
    public Map<TopicUriPartition, Long> listOffsetsForConsumerGroup(
            String consumerGroupId,
            Collection<TopicUriPartition> topicUriPartitions,
            Duration duration
    ) throws ExecutionException, InterruptedException, TimeoutException {
        ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions();
        options.topicPartitions(topicUriPartitions.stream().map(tup ->
                new TopicPartition(tup.getTopicUri().getTopic(), tup.getPartition())).collect(Collectors.toList()));
        Map<TopicPartition, OffsetAndMetadata> offsets = kafkaAdminClient
                .listConsumerGroupOffsets(consumerGroupId, options)
                .partitionsToOffsetAndMetadata()
                .get(duration.toMillis(), TimeUnit.MILLISECONDS);
        Map<TopicUriPartition, Long> result = new HashMap<>();
        offsets.forEach((tp, offsetAndMetadata) -> {
            TopicRn topicRn = MetadataUtils.createTopicRn(topicUri, tp.topic());
            Long offset = offsetAndMetadata == null ? null : offsetAndMetadata.offset();
            if (offset == null) {
                logger.warn("Consumer group {} has no committed offset for topic partition {}", consumerGroupId, tp);
                return;
            }
            result.put(
                    createKafkaTopicUriPartition(topicRn, tp.partition()),
                    offset
            );
        });
        return result;
    }

    private TopicUriPartition createKafkaTopicUriPartition(TopicRn topicRn, int partition) {
        return new TopicUriPartition(new KafkaTopicUri(new BaseTopicUri(topicUri.getProtocol(), topicRn)), partition);
    }

    @Override
    public void close() throws IOException {
        if (kafkaAdminClient != null)
            kafkaAdminClient.close();
        logger.info("Closed PscKafkaMetadataClient");
    }
}
