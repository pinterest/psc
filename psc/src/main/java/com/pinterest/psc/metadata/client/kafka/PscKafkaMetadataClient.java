package com.pinterest.psc.metadata.client.kafka;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.discovery.ServiceDiscoveryManager;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metadata.MetadataUtils;
import com.pinterest.psc.metadata.TopicRnMetadata;
import com.pinterest.psc.metadata.client.PscBackendMetadataClient;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class PscKafkaMetadataClient extends PscBackendMetadataClient {

    private static final PscLogger logger = PscLogger.getLogger(PscKafkaMetadataClient.class);
    private AdminClient kafkaAdminClient;

    @Override
    public void initialize(TopicUri topicUri, Environment env, PscConfigurationInternal pscConfigurationInternal) throws ConfigurationException {
        super.initialize(topicUri, env, pscConfigurationInternal);
        Properties properties = PscConfigurationUtils.pscConfigurationInternalToProperties(pscConfigurationInternal);
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, discoveryConfig.getConnect());
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, pscConfigurationInternal.getMetadataClientId());
        this.kafkaAdminClient = AdminClient.create(properties);
        logger.info("Initialized Kafka AdminClient with properties: " + properties);
    }

    @Override
    public List<TopicRn> listTopicRns(long timeout, TimeUnit timeUnit) throws ExecutionException, InterruptedException, TimeoutException {
        ListTopicsResult listTopicsResult = kafkaAdminClient.listTopics();
        Collection<TopicListing> topicListing = listTopicsResult.listings().get(timeout, timeUnit);
        return topicListing.stream().map(tl -> MetadataUtils.createTopicRn(topicUri, tl.name())).collect(Collectors.toList());
    }

    @Override
    public Map<TopicRn, TopicRnMetadata> describeTopicRns(Set<TopicRn> topicRns, long timeout, TimeUnit timeUnit) throws ExecutionException, InterruptedException, TimeoutException {
        Collection<String> topicNames = topicRns.stream().map(TopicRn::getTopic).collect(Collectors.toSet());
        Map<String, TopicDescription> topicMetadata = kafkaAdminClient.describeTopics(topicNames).all().get(timeout, timeUnit);
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
    public Map<TopicUriPartition, MessageId> listOffsets(Map<TopicUriPartition, PscMetadataClient.MetadataClientOption> topicUriPartitionsAndOptions, long timeout, TimeUnit timeUnit) throws ExecutionException, InterruptedException, TimeoutException {
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
            topicPartitionOffsets.put(new TopicPartition(topicUriPartition.getTopicUri().getTopic(), topicUriPartition.getPartition()), offsetSpec);
        }
        ListOffsetsResult listOffsetsResult = kafkaAdminClient.listOffsets(topicPartitionOffsets);
        Map<TopicUriPartition, MessageId> result = new HashMap<>();
        listOffsetsResult.all().get(timeout, timeUnit).entrySet().forEach(e -> {
            TopicPartition tp = e.getKey();
            ListOffsetsResult.ListOffsetsResultInfo info = e.getValue();
            TopicRn topicRn = MetadataUtils.createTopicRn(topicUri, tp.topic());
            result.put(
                    createKafkaTopicUriPartition(topicRn, tp.partition()),
                    new MessageId(createKafkaTopicUriPartition(topicRn, tp.partition()), info.offset(), info.timestamp())
            );
        });
        return result;
    }

    private TopicUriPartition createKafkaTopicUriPartition(TopicRn topicRn, int partition) {
        return new TopicUriPartition(new KafkaTopicUri(new BaseTopicUri(topicUri.getProtocol(), topicRn)), partition);
    }

    @Override
    public void close() throws Exception {
        if (kafkaAdminClient != null)
            kafkaAdminClient.close();
        logger.info("Closed PscKafkaMetadataClient");
    }
}
