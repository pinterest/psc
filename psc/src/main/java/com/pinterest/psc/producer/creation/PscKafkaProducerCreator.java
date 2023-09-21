package com.pinterest.psc.producer.creation;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.discovery.ServiceDiscoveryManager;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.interceptor.ProducerInterceptors;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.producer.PscBackendProducer;
import com.pinterest.psc.producer.kafka.PscKafkaProducer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@PscProducerCreatorPlugin(backend = PscUtils.BACKEND_TYPE_KAFKA)
public class PscKafkaProducerCreator<K, V> extends PscBackendProducerCreator<K, V> {
    private static final PscLogger logger = PscLogger.getLogger(PscKafkaProducerCreator.class);

    @Override
    public Set<PscBackendProducer<K, V>> getProducers(
            Environment environment,
            PscConfigurationInternal pscConfigurationInternal,
            ProducerInterceptors<K, V> producerInterceptors,
            Set<TopicUri> topicUris
    ) throws ConfigurationException {
        Set<PscBackendProducer<K, V>> producers = new HashSet<>();
        Map<String, Set<TopicUri>> clusterTopics = new HashMap<>();

        for (TopicUri topicUri : topicUris) {
            KafkaTopicUri kafkaUri = (KafkaTopicUri) topicUri;
            clusterTopics.computeIfAbsent(kafkaUri.getTopicUriPrefix(), p -> new HashSet<>()).add(kafkaUri);
        }

        for (Map.Entry<String, Set<TopicUri>> entry : clusterTopics.entrySet()) {
            PscKafkaProducer<K, V> pscKafkaProducer = clusterProducerCache.get(entry.getKey());
            TopicUri sampleKafkaTopicUri = entry.getValue().iterator().next();
            if (pscKafkaProducer == null) {
                ServiceDiscoveryConfig discoveryConfig;
                discoveryConfig = ServiceDiscoveryManager.getServiceDiscoveryConfig(
                        environment,
                        pscConfigurationInternal.getDiscoveryConfiguration(),
                        sampleKafkaTopicUri
                );
                pscKafkaProducer = new PscKafkaProducer<>();
                pscKafkaProducer.initialize(pscConfigurationInternal, discoveryConfig, environment, sampleKafkaTopicUri);
                clusterProducerCache.put(entry.getKey(), pscKafkaProducer);
            }

            pscKafkaProducer.setProducerInterceptors(producerInterceptors);
            producers.add(pscKafkaProducer);
        }

        return producers;
    }

    @Override
    public TopicUri validateBackendTopicUri(TopicUri baseTopicUri) throws TopicUriSyntaxException {
        return KafkaTopicUri.validate(baseTopicUri);
    }

    @Override
    public void reset() {
        clusterProducerCache.clear();
    }
}
