package com.pinterest.psc.metadata.client.memq;

import com.pinterest.memq.client.commons.ConsumerConfigs;
import com.pinterest.memq.client.commons.serde.ByteArrayDeserializer;
import com.pinterest.memq.client.consumer.MemqConsumer;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.config.PscMetadataClientToMemqConsumerConfigConverter;
import com.pinterest.psc.consumer.memq.MemqTopicUri;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metadata.MetadataUtils;
import com.pinterest.psc.metadata.TopicUriMetadata;
import com.pinterest.psc.metadata.client.PscBackendMetadataClient;
import com.pinterest.psc.metadata.client.PscMetadataClient;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

/**
 * A Memq-specific implementation of the {@link PscBackendMetadataClient}.
 * Uses short-lived {@link MemqConsumer} instances to query metadata since Memq
 * does not have a dedicated admin client.
 */
public class PscMemqMetadataClient extends PscBackendMetadataClient {

    private static final PscLogger logger = PscLogger.getLogger(PscMemqMetadataClient.class);
    private Properties baseProperties;

    @Override
    public void initialize(
            TopicUri topicUri,
            Environment env,
            PscConfigurationInternal pscConfigurationInternal
    ) throws ConfigurationException {
        super.initialize(topicUri, env, pscConfigurationInternal);
        baseProperties = new PscMetadataClientToMemqConsumerConfigConverter()
                .convert(pscConfigurationInternal, topicUri);
        baseProperties.setProperty(ConsumerConfigs.BOOTSTRAP_SERVERS, discoveryConfig.getConnect());
        baseProperties.setProperty(ConsumerConfigs.CLIENT_ID, pscConfigurationInternal.getMetadataClientId());
        baseProperties.setProperty(ConsumerConfigs.KEY_DESERIALIZER_CLASS_KEY,
                ByteArrayDeserializer.class.getName());
        baseProperties.put(ConsumerConfigs.KEY_DESERIALIZER_CLASS_CONFIGS_KEY, new Properties());
        baseProperties.setProperty(ConsumerConfigs.VALUE_DESERIALIZER_CLASS_KEY,
                ByteArrayDeserializer.class.getName());
        baseProperties.put(ConsumerConfigs.VALUE_DESERIALIZER_CLASS_CONFIGS_KEY, new Properties());
        baseProperties.setProperty(ConsumerConfigs.DIRECT_CONSUMER, "false");
        logger.info("Initialized PscMemqMetadataClient with base properties: " + baseProperties);
    }

    @Override
    public List<TopicRn> listTopicRns(Duration duration)
            throws ExecutionException, InterruptedException, TimeoutException {
        throw new UnsupportedOperationException(
                "[Memq] Listing all topics is not supported by the Memq backend.");
    }

    @Override
    public Map<TopicUri, TopicUriMetadata> describeTopicUris(
            Collection<TopicUri> topicUris,
            Duration duration
    ) throws ExecutionException, InterruptedException, TimeoutException {
        Map<TopicUri, TopicUriMetadata> result = new HashMap<>();
        for (TopicUri tu : topicUris) {
            try (MemqConsumer<byte[], byte[]> consumer = createConsumer(tu.getTopic())) {
                List<Integer> partitions = consumer.getPartition();
                List<TopicUriPartition> topicUriPartitions = new ArrayList<>();
                for (int partition : partitions) {
                    topicUriPartitions.add(createMemqTopicUriPartition(tu, partition));
                }
                result.put(tu, new TopicUriMetadata(tu, topicUriPartitions));
            } catch (IOException e) {
                throw new ExecutionException("Failed to close Memq metadata consumer for " + tu, e);
            } catch (Exception e) {
                throw new ExecutionException("Failed to describe topic " + tu, e);
            }
        }
        return result;
    }

    @Override
    public Map<TopicUriPartition, Long> listOffsets(
            Map<TopicUriPartition, PscMetadataClient.MetadataClientOption> topicUriPartitionsAndOptions,
            Duration duration
    ) throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, Set<Integer>> earliestByTopic = new HashMap<>();
        Map<String, Set<Integer>> latestByTopic = new HashMap<>();
        Map<String, TopicUri> topicToUri = new HashMap<>();

        for (Map.Entry<TopicUriPartition, PscMetadataClient.MetadataClientOption> entry :
                topicUriPartitionsAndOptions.entrySet()) {
            TopicUriPartition tup = entry.getKey();
            String topic = tup.getTopicUri().getTopic();
            topicToUri.put(topic, tup.getTopicUri());

            if (entry.getValue() == PscMetadataClient.MetadataClientOption.OFFSET_SPEC_EARLIEST) {
                earliestByTopic.computeIfAbsent(topic, k -> new HashSet<>()).add(tup.getPartition());
            } else if (entry.getValue() == PscMetadataClient.MetadataClientOption.OFFSET_SPEC_LATEST) {
                latestByTopic.computeIfAbsent(topic, k -> new HashSet<>()).add(tup.getPartition());
            } else {
                throw new IllegalArgumentException(
                        "Unsupported MetadataClientOption for listOffsets(): " + entry.getValue());
            }
        }

        Map<TopicUriPartition, Long> result = new HashMap<>();
        Set<String> allTopics = new HashSet<>();
        allTopics.addAll(earliestByTopic.keySet());
        allTopics.addAll(latestByTopic.keySet());

        for (String topic : allTopics) {
            try (MemqConsumer<byte[], byte[]> consumer = createConsumer(topic)) {
                Set<Integer> earliestPartitions = earliestByTopic.getOrDefault(topic, new HashSet<>());
                if (!earliestPartitions.isEmpty()) {
                    Map<Integer, Long> offsets = consumer.getEarliestOffsets(earliestPartitions);
                    for (Map.Entry<Integer, Long> e : offsets.entrySet()) {
                        TopicRn topicRn = MetadataUtils.createTopicRn(topicUri, topic);
                        result.put(createMemqTopicUriPartition(topicRn, e.getKey()), e.getValue());
                    }
                }

                Set<Integer> latestPartitions = latestByTopic.getOrDefault(topic, new HashSet<>());
                if (!latestPartitions.isEmpty()) {
                    Map<Integer, Long> offsets = consumer.getLatestOffsets(latestPartitions);
                    for (Map.Entry<Integer, Long> e : offsets.entrySet()) {
                        TopicRn topicRn = MetadataUtils.createTopicRn(topicUri, topic);
                        result.put(createMemqTopicUriPartition(topicRn, e.getKey()), e.getValue());
                    }
                }
            } catch (IOException e) {
                throw new ExecutionException("Failed to close Memq metadata consumer for topic " + topic, e);
            } catch (Exception e) {
                throw new ExecutionException("Failed to list offsets for topic " + topic, e);
            }
        }
        return result;
    }

    @Override
    public Map<TopicUriPartition, Long> listOffsetsForTimestamps(
            Map<TopicUriPartition, Long> topicUriPartitionsAndTimes,
            Duration duration
    ) throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, Map<Integer, Long>> timestampsByTopic = new HashMap<>();
        Map<String, Map<Integer, TopicUriPartition>> partitionLookupByTopic = new HashMap<>();

        for (Map.Entry<TopicUriPartition, Long> entry : topicUriPartitionsAndTimes.entrySet()) {
            TopicUriPartition tup = entry.getKey();
            String topic = tup.getTopicUri().getTopic();
            timestampsByTopic.computeIfAbsent(topic, k -> new HashMap<>())
                    .put(tup.getPartition(), entry.getValue());
            partitionLookupByTopic.computeIfAbsent(topic, k -> new HashMap<>())
                    .put(tup.getPartition(), tup);
        }

        Map<TopicUriPartition, Long> result = new HashMap<>();
        for (Map.Entry<String, Map<Integer, Long>> entry : timestampsByTopic.entrySet()) {
            String topic = entry.getKey();
            try (MemqConsumer<byte[], byte[]> consumer = createConsumer(topic)) {
                Map<Integer, Long> offsets = consumer.offsetsOfTimestamps(entry.getValue());
                Map<Integer, TopicUriPartition> partitionLookup = partitionLookupByTopic.get(topic);
                for (Map.Entry<Integer, Long> offsetEntry : offsets.entrySet()) {
                    TopicRn topicRn = MetadataUtils.createTopicRn(topicUri, topic);
                    result.put(
                            createMemqTopicUriPartition(topicRn, offsetEntry.getKey()),
                            offsetEntry.getValue()
                    );
                }
            } catch (IOException e) {
                throw new ExecutionException(
                        "Failed to close Memq metadata consumer for topic " + topic, e);
            } catch (Exception e) {
                throw new ExecutionException(
                        "Failed to list offsets for timestamps for topic " + topic, e);
            }
        }
        return result;
    }

    @Override
    public Map<TopicUriPartition, Long> listOffsetsForConsumerGroup(
            String consumerGroupId,
            Collection<TopicUriPartition> topicUriPartitions,
            Duration duration
    ) throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, Set<Integer>> partitionsByTopic = new HashMap<>();
        for (TopicUriPartition tup : topicUriPartitions) {
            String topic = tup.getTopicUri().getTopic();
            partitionsByTopic.computeIfAbsent(topic, k -> new HashSet<>()).add(tup.getPartition());
        }

        Map<TopicUriPartition, Long> result = new HashMap<>();
        for (Map.Entry<String, Set<Integer>> entry : partitionsByTopic.entrySet()) {
            String topic = entry.getKey();
            try (MemqConsumer<byte[], byte[]> consumer = createConsumer(topic, consumerGroupId)) {
                for (int partition : entry.getValue()) {
                    long committedOffset = consumer.committed(partition);
                    if (committedOffset == -1L) {
                        logger.warn(
                                "Consumer group {} has no committed offset for topic {} partition {}",
                                consumerGroupId, topic, partition
                        );
                        continue;
                    }
                    TopicRn topicRn = MetadataUtils.createTopicRn(topicUri, topic);
                    result.put(createMemqTopicUriPartition(topicRn, partition), committedOffset);
                }
            } catch (IOException e) {
                throw new ExecutionException(
                        "Failed to close Memq metadata consumer for topic " + topic, e);
            } catch (Exception e) {
                throw new ExecutionException(
                        "Failed to list consumer group offsets for topic " + topic, e);
            }
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        logger.info("Closed PscMemqMetadataClient");
    }

    private MemqConsumer<byte[], byte[]> createConsumer(String topic) throws Exception {
        return createConsumer(topic, null);
    }

    private MemqConsumer<byte[], byte[]> createConsumer(String topic, String groupId) throws Exception {
        Properties props = new Properties();
        props.putAll(baseProperties);
        props.setProperty(ConsumerConfigs.CLIENT_ID,
                baseProperties.getProperty(ConsumerConfigs.CLIENT_ID) + "_metadata");
        if (groupId != null) {
            props.setProperty(ConsumerConfigs.GROUP_ID, groupId);
        } else {
            props.setProperty(ConsumerConfigs.GROUP_ID,
                    topic + "_metadata_cg_" + ThreadLocalRandom.current().nextInt());
        }
        MemqConsumer<byte[], byte[]> consumer = new MemqConsumer<>(props);
        consumer.subscribe(topic);
        return consumer;
    }

    private TopicUriPartition createMemqTopicUriPartition(TopicRn topicRn, int partition) {
        return new TopicUriPartition(
                new MemqTopicUri(new BaseTopicUri(topicUri.getProtocol(), topicRn)), partition);
    }

    private TopicUriPartition createMemqTopicUriPartition(TopicUri topicUri, int partition) {
        return new TopicUriPartition(new MemqTopicUri(topicUri), partition);
    }
}
