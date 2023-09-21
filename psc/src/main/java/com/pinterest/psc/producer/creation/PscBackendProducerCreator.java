package com.pinterest.psc.producer.creation;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.interceptor.ProducerInterceptors;
import com.pinterest.psc.producer.PscBackendProducer;
import com.pinterest.psc.producer.kafka.PscKafkaProducer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class PscBackendProducerCreator<K, V> {

    /**
     * This map keeps a producer for each cluster that has topics to which messages were produced.
     */
    Map<String, PscKafkaProducer<K, V>> clusterProducerCache = new HashMap<>();

    /**
     * Creates/updates the backend producers and returns a set of active producers. The creator is responsible for
     * reusing resources
     *
     * @param environment                  the Environment of the current PscProducer instance
     * @param pscConfigurationInternal     the PscConfiguration of the current initialization of the producer
     * @param producerInterceptors         the producer interceptors for producer APIs
     * @param topicUris                    TopicUris that this backend should handle
     * @return a Set of PscBackendProducers that are active and should not be removed
     * @throws ProducerException      if extracting a backend producer fails due to invalid topic URI, or lack of
     *                                an applicable service discovery method, or ...
     * @throws ConfigurationException thrown if service discovery fails based on the provided configuration
     */
    public abstract Set<PscBackendProducer<K, V>> getProducers(
            Environment environment,
            PscConfigurationInternal pscConfigurationInternal,
            ProducerInterceptors<K, V> producerInterceptors,
            Set<TopicUri> topicUris
    ) throws ProducerException, ConfigurationException;

    /**
     * Creates/updates the backend producer and returns an active producer. The creator is responsible for reusing
     * resources.
     *
     * @param environment                  the Environment of the current PscProducer instance
     * @param pscConfigurationInternal     the PscConfiguration of the current initialization of the producer
     * @param producerInterceptors         the producer interceptors for producer APIs
     * @param topicUri                     TopicUri that this backend should handle
     * @return a Set of PscBackendProducers that are active and should not be removed
     * @throws ProducerException      if extracting a backend producer fails due to invalid topic URI, or lack of
     *                                an applicable service discovery method, or ...
     * @throws ConfigurationException thrown if service discovery fails based on the provided configuration
     */
    public PscBackendProducer<K, V> getProducer(
            Environment environment,
            PscConfigurationInternal pscConfigurationInternal,
            ProducerInterceptors<K, V> producerInterceptors,
            TopicUri topicUri
    ) throws ProducerException, ConfigurationException {
        Set<PscBackendProducer<K, V>> pscBackendProducers = getProducers(
                environment,
                pscConfigurationInternal,
                producerInterceptors,
                Collections.singleton(topicUri)
        );

        if (pscBackendProducers.isEmpty())
            throw new ProducerException("No producer was found for topic uri " + topicUri);

        return pscBackendProducers.iterator().next();
    }

    public abstract TopicUri validateBackendTopicUri(TopicUri baseTopicUri) throws TopicUriSyntaxException;

    /**
     * Removes the given backend producer associated with given topic URI partitions from the internal cache, so that
     * the next retrieval leads to creating a new backend producer.
     *
     * @param topicUris set of topic URI partitions to use to retrieve the backend producer
     * @param backendProducer the backend producer to dismiss
     */
    public void dismissBackendProducer(Set<TopicUri> topicUris, PscBackendProducer<K, V> backendProducer) {
        Set<String> topicUriPrefixes = topicUris.stream().map(topicUri -> topicUri.getTopicUriPrefix()).collect(Collectors.toSet());
        for (String topicUriPrefix : topicUriPrefixes)
            clusterProducerCache.remove(topicUriPrefix, backendProducer);
    }

    /**
     * Optional handler on the PscProducer to do necessary clean ups and release resources used by existing producers.
     */
    public void reset() {
    }
}