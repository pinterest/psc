package com.pinterest.psc.consumer.creation;

import com.google.common.collect.Sets;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.ConsumerRebalanceListener;
import com.pinterest.psc.consumer.PscBackendConsumer;
import com.pinterest.psc.consumer.memq.MemqTopicUri;
import com.pinterest.psc.consumer.memq.PscMemqConsumer;
import com.pinterest.psc.discovery.ServiceDiscoveryManager;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.interceptor.ConsumerInterceptors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@PscConsumerCreatorPlugin(backend = "memq")
public class PscMemqConsumerCreator<K, V> extends PscBackendConsumerCreator<K, V> {

    @Override
    public Set<PscBackendConsumer<K, V>> getConsumers(Environment environment,
                                                      PscConfigurationInternal pscConfigurationInternal,
                                                      ConsumerInterceptors<K, V> consumerInterceptors,
                                                      Set<TopicUri> topicUris, boolean shouldWakeup,
                                                      boolean subscribe)
        throws ConsumerException, ConfigurationException {
        return getConsumers(environment, pscConfigurationInternal, consumerInterceptors, topicUris,
            null, shouldWakeup, subscribe);
    }

    @Override
    public Set<PscBackendConsumer<K, V>> getConsumers(Environment environment,
                                                      PscConfigurationInternal pscConfigurationInternal,
                                                      ConsumerInterceptors<K, V> consumerInterceptors,
                                                      Set<TopicUri> topicUris,
                                                      ConsumerRebalanceListener consumerRebalanceListener,
                                                      boolean shouldWakeup,
                                                      boolean subscribe) throws ConsumerException,
            ConfigurationException {
        Set<PscBackendConsumer<K, V>> consumers = new HashSet<>();
        // Memq consumer does not support subscribing to more than one topic; however we
        // use a set in this map
        // just in case that limitation is lifted in the future.
        Map<String, Set<TopicUri>> clusterTopics = new HashMap<>();

        for (TopicUri topicUri : topicUris) {
            MemqTopicUri memqUri = (MemqTopicUri) topicUri;
            clusterTopics.computeIfAbsent(memqUri.getTopicUriPrefix(), p -> new HashSet<>()).add(memqUri);
        }

        for (Map.Entry<String, Set<TopicUri>> entry : clusterTopics.entrySet()) {
            PscMemqConsumer<K, V> pscMemqConsumer = (PscMemqConsumer) clusterConsumerCache.get(entry.getKey());
            TopicUri sampleMemeTopicUri = entry.getValue().iterator().next();
            if (pscMemqConsumer == null) {
                ServiceDiscoveryConfig discoveryConfig;
                discoveryConfig = ServiceDiscoveryManager.getServiceDiscoveryConfig(environment,
                        pscConfigurationInternal.getDiscoveryConfiguration(), sampleMemeTopicUri);
                pscMemqConsumer = new PscMemqConsumer<>();
                pscMemqConsumer.initialize(pscConfigurationInternal, discoveryConfig, sampleMemeTopicUri);
                clusterConsumerCache.put(entry.getKey(), pscMemqConsumer);
                if (shouldWakeup)
                    pscMemqConsumer.wakeup();
            }

            pscMemqConsumer.setConsumerInterceptors(consumerInterceptors);

            if (subscribe) {
                pscMemqConsumer.subscribe(entry.getValue(), consumerRebalanceListener);
            }
            consumers.add(pscMemqConsumer);
        }

        if (subscribe) {
            // remove all consumers in the cache that shouldn't exist in the current
            // subscription
            for (String diffUri : Sets.difference(clusterConsumerCache.keySet(),
                    clusterTopics.keySet())) {
                clusterConsumerCache.remove(diffUri);
            }
        }

        return consumers;
    }

    @Override
    public Set<PscBackendConsumer<K, V>> getAssignmentConsumers(Environment environment,
                                                                PscConfigurationInternal pscConfigurationInternal,
                                                                ConsumerInterceptors<K, V> consumerInterceptors,
                                                                Set<TopicUriPartition> topicUriPartitions,
                                                                boolean shouldWakeup,
                                                                boolean assign) throws ConsumerException {
        Set<PscBackendConsumer<K, V>> consumers = new HashSet<>();
        Map<String, Set<TopicUriPartition>> clusterPartitions = new HashMap<>();

        for (TopicUriPartition topicUriPartition : topicUriPartitions) {
            MemqTopicUri memqTopicUri = (MemqTopicUri) topicUriPartition.getTopicUri();
            clusterPartitions.computeIfAbsent(memqTopicUri.getTopicUriPrefix(), p -> new HashSet<>())
                    .add(topicUriPartition);
        }

        for (Map.Entry<String, Set<TopicUriPartition>> entry : clusterPartitions.entrySet()) {
            PscMemqConsumer<K, V> pscMemqConsumer = (PscMemqConsumer) clusterConsumerCache.get(entry.getKey());
            TopicUri sampleMemqTopicUri = entry.getValue().iterator().next().getTopicUri();
            if (pscMemqConsumer == null) {
                ServiceDiscoveryConfig discoveryConfig;
                try {
                    discoveryConfig = ServiceDiscoveryManager.getServiceDiscoveryConfig(environment,
                            pscConfigurationInternal.getDiscoveryConfiguration(), sampleMemqTopicUri);
                } catch (Exception e) {
                    throw new ConsumerException(e);
                }
                pscMemqConsumer = new PscMemqConsumer<>();
                pscMemqConsumer.initialize(pscConfigurationInternal, discoveryConfig, sampleMemqTopicUri);
                clusterConsumerCache.put(entry.getKey(), pscMemqConsumer);
                if (shouldWakeup)
                    pscMemqConsumer.wakeup();
            }

            pscMemqConsumer.setConsumerInterceptors(consumerInterceptors);
            if (assign)
                pscMemqConsumer.assign(entry.getValue());
            consumers.add(pscMemqConsumer);
        }

        if (assign) {
            // remove all consumers in the cache that shouldn't exist in the current
            // assignment
            for (String diffUri : Sets.difference(clusterConsumerCache.keySet(),
                    clusterPartitions.keySet())) {
                clusterConsumerCache.remove(diffUri);
            }
        }

        return consumers;
    }

    @Override
    public TopicUri validateBackendTopicUri(TopicUri baseTopicUri) throws TopicUriSyntaxException {
        return MemqTopicUri.validate(baseTopicUri);
    }
}
