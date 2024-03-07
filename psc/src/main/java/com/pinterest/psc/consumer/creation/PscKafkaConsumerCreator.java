package com.pinterest.psc.consumer.creation;

import com.google.common.collect.Sets;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.ConsumerRebalanceListener;
import com.pinterest.psc.consumer.OffsetCommitCallback;
import com.pinterest.psc.consumer.PscBackendConsumer;
import com.pinterest.psc.consumer.kafka.PscKafkaConsumer;
import com.pinterest.psc.discovery.ServiceDiscoveryManager;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.interceptor.ConsumerInterceptors;
import com.pinterest.psc.logging.PscLogger;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@PscConsumerCreatorPlugin(backend = PscUtils.BACKEND_TYPE_KAFKA)
public class PscKafkaConsumerCreator<K, V> extends PscBackendConsumerCreator<K, V> {

    public Set<PscBackendConsumer<K, V>> getConsumers(Environment environment,
                                                      PscConfigurationInternal pscConfigurationInternal,
                                                      ConsumerInterceptors<K, V> consumerInterceptors,
                                                      Set<TopicUri> topicUris, boolean shouldWakeup,
                                                      boolean subscribe)
        throws ConsumerException, ConfigurationException {
        return getConsumers(environment, pscConfigurationInternal, consumerInterceptors, topicUris,
            null, shouldWakeup, subscribe);
    }

    private static final PscLogger logger = PscLogger.getLogger(PscKafkaConsumerCreator.class);

    @Override
    public Set<PscBackendConsumer<K, V>> getConsumers(
            Environment environment,
            PscConfigurationInternal pscConfigurationInternal,
            ConsumerInterceptors<K, V> consumerInterceptors,
            Set<TopicUri> topicUris,
            ConsumerRebalanceListener consumerRebalanceListener,
            boolean shouldWakeup,
            boolean subscribe
    ) throws ConsumerException, ConfigurationException {
        Set<PscBackendConsumer<K, V>> consumers = new HashSet<>();
        Map<String, Set<TopicUri>> clusterTopics = new HashMap<>();

        for (TopicUri topicUri : topicUris) {
            KafkaTopicUri kafkaUri = (KafkaTopicUri) topicUri;
            clusterTopics.computeIfAbsent(kafkaUri.getTopicUriPrefix(), p -> new HashSet<>()).add(kafkaUri);
        }

        for (Map.Entry<String, Set<TopicUri>> entry : clusterTopics.entrySet()) {
            PscKafkaConsumer<K, V> pscKafkaConsumer = (PscKafkaConsumer) clusterConsumerCache.get(entry.getKey());
            TopicUri sampleKafkaTopicUri = entry.getValue().iterator().next();
            if (pscKafkaConsumer == null) {
                ServiceDiscoveryConfig discoveryConfig;
                discoveryConfig = ServiceDiscoveryManager.getServiceDiscoveryConfig(
                        environment,
                        pscConfigurationInternal.getDiscoveryConfiguration(),
                        sampleKafkaTopicUri
                );
                pscKafkaConsumer = new PscKafkaConsumer<>();
                pscKafkaConsumer.initialize(pscConfigurationInternal, discoveryConfig, sampleKafkaTopicUri);
                clusterConsumerCache.put(entry.getKey(), pscKafkaConsumer);
                if (shouldWakeup)
                    pscKafkaConsumer.wakeup();
            }

            pscKafkaConsumer.setConsumerInterceptors(consumerInterceptors);
            if (subscribe) {
                if (consumerRebalanceListener == null) {
                    pscKafkaConsumer.subscribe(entry.getValue());
                } else {
                    pscKafkaConsumer.subscribe(entry.getValue(), new ConsumerRebalanceListener() {
                        @Override
                        public void onPartitionsRevoked(Collection<TopicUriPartition> partitions) {
                            consumerRebalanceListener.onPartitionsRevoked(partitions);
                        }

                        @Override
                        public void onPartitionsAssigned(Collection<TopicUriPartition> partitions) {
                            consumerRebalanceListener.onPartitionsAssigned(partitions);
                        }
                    });
                }
            }
            consumers.add(pscKafkaConsumer);
        }

        if (subscribe) {
            // remove all consumers in the cache that shouldn't exist in the current subscription
            for (String diffUri : Sets.difference(clusterConsumerCache.keySet(), clusterTopics.keySet())) {
                clusterConsumerCache.remove(diffUri);
            }
        }

        return consumers;
    }

    @Override
    public Set<PscBackendConsumer<K, V>> getAssignmentConsumers(
            Environment environment,
            PscConfigurationInternal pscConfigurationInternal,
            ConsumerInterceptors<K, V> consumerInterceptors,
            Set<TopicUriPartition> topicUriPartitions,
            boolean shouldWakeup,
            boolean assign
    ) throws ConsumerException, ConfigurationException {
        Set<PscBackendConsumer<K, V>> consumers = new HashSet<>();
        Map<String, Set<TopicUriPartition>> clusterPartitions = new HashMap<>();

        for (TopicUriPartition topicUriPartition : topicUriPartitions) {
            KafkaTopicUri kafkaTopicUri = (KafkaTopicUri) topicUriPartition.getTopicUri();
            clusterPartitions
                    .computeIfAbsent(kafkaTopicUri.getTopicUriPrefix(), p -> new HashSet<>())
                    .add(topicUriPartition);
        }

        for (Map.Entry<String, Set<TopicUriPartition>> entry : clusterPartitions.entrySet()) {
            PscKafkaConsumer<K, V> pscKafkaConsumer = (PscKafkaConsumer) clusterConsumerCache.get(entry.getKey());
            KafkaTopicUri sampleKafkaTopicUri = (KafkaTopicUri) entry.getValue().iterator().next().getTopicUri();
            if (pscKafkaConsumer == null) {
                ServiceDiscoveryConfig discoveryConfig;
                discoveryConfig = ServiceDiscoveryManager.getServiceDiscoveryConfig(
                        environment,
                        pscConfigurationInternal.getDiscoveryConfiguration(),
                        sampleKafkaTopicUri
                );
                pscKafkaConsumer = new PscKafkaConsumer<>();
                pscKafkaConsumer.initialize(pscConfigurationInternal, discoveryConfig, sampleKafkaTopicUri);
                clusterConsumerCache.put(entry.getKey(), pscKafkaConsumer);
                if (shouldWakeup)
                    pscKafkaConsumer.wakeup();
            }

            pscKafkaConsumer.setConsumerInterceptors(consumerInterceptors);
            if (assign)
                pscKafkaConsumer.assign(entry.getValue());
            consumers.add(pscKafkaConsumer);
        }

        if (assign) {
            // remove all consumers in the cache that shouldn't exist in the current assignment
            for (String diffUri : Sets.difference(clusterConsumerCache.keySet(), clusterPartitions.keySet())) {
                clusterConsumerCache.remove(diffUri);
            }
        }

        return consumers;
    }

    @Override
    public Set<PscBackendConsumer<K, V>> getCommitConsumers(Environment environment,
        PscConfigurationInternal pscConfigurationInternal,
        ConsumerInterceptors<K, V> consumerInterceptors,
        Set<TopicUriPartition> topicUriPartitions,
        boolean shouldWakeup)
        throws ConsumerException, ConfigurationException {
        return getAssignmentConsumers(
            environment,
            pscConfigurationInternal,
            consumerInterceptors,
            topicUriPartitions,
            shouldWakeup,
            false);
    }

    @Override
    public TopicUri validateBackendTopicUri(TopicUri baseTopicUri) throws TopicUriSyntaxException {
        return KafkaTopicUri.validate(baseTopicUri);
    }

    @Override
    public void reset() {
        for (PscBackendConsumer<K, V> pscBackendConsumer : clusterConsumerCache.values()) {
            try {
                pscBackendConsumer.close();
            } catch (Exception e) {
                logger.warn("Could not close backend Kafka consumer.");
            }
        }
        clusterConsumerCache.clear();
    }
}
