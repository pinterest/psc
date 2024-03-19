package com.pinterest.psc.consumer.creation;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.ConsumerRebalanceListener;
import com.pinterest.psc.consumer.PscBackendConsumer;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.interceptor.ConsumerInterceptors;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class PscBackendConsumerCreator<K, V> {

    /**
     * This map keeps a consumer for each cluster that has topics subscribed to
     * It should be cleaned up if there are clusters that don't have any topics after a subscribe call
     */
    Map<String, PscBackendConsumer<K, V>> clusterConsumerCache = new ConcurrentHashMap<>();

    /**
     * Creates/updates the backend consumer(s) and returns a set of active consumers
     * The creator is responsible of reusing resources
     *
     * @param environment                  the Environment of the current PscConsumer instance
     * @param pscConfigurationInternal     the PscConfiguration of the current initialization of the consumer
     * @param consumerInterceptors         the consumer interceptors for consumer APIs
     * @param topicUris                    TopicUris that this backend should handle
     * @param shouldWakeup                 whether the retrieved consumers should be woken up
     * @param subscribe                    whether the retrieved consumers should subscribe to the given topic URIs
     * @return a Set of PscBackendConsumers that are active and should not be removed/unsubscribed
     * @throws ConsumerException      thrown if extracting a backend consumer fails due to invalid topic URI, or lack of
     *                                an applicable service discovery method, or ...
     * @throws ConfigurationException thrown if service discovery fails based on the provided configuration
     */
    public abstract Set<PscBackendConsumer<K, V>> getConsumers(
            Environment environment,
            PscConfigurationInternal pscConfigurationInternal,
            ConsumerInterceptors<K, V> consumerInterceptors,
            Set<TopicUri> topicUris,
            boolean shouldWakeup,
            boolean subscribe
    ) throws ConsumerException, ConfigurationException;

    /**
     * Creates/updates the backend consumer(s) and returns a set of active consumers
     * The creator is responsible of reusing resources
     *
     * @param environment                  the Environment of the current PscConsumer instance
     * @param pscConfigurationInternal     the PscConfiguration of the current initialization of the consumer
     * @param consumerInterceptors         the consumer interceptors for consumer APIs
     * @param topicUris                    TopicUris that this backend should handle
     * @param consumerRebalanceListener    the ConsumerRebalanceListener callback interface implementation
     * @param shouldWakeup                 whether the retrieved consumers should be woken up
     * @param subscribe                    whether the retrieved consumers should subscribe to the given topic URIs
     * @return a Set of PscBackendConsumers that are active and should not be removed/unsubscribed
     * @throws ConsumerException      thrown if extracting a backend consumer fails due to invalid topic URI, or lack of
     *                                an applicable service discovery method, or ...
     * @throws ConfigurationException thrown if service discovery fails based on the provided configuration
     */
    public abstract Set<PscBackendConsumer<K, V>> getConsumers(
        Environment environment,
        PscConfigurationInternal pscConfigurationInternal,
        ConsumerInterceptors<K, V> consumerInterceptors,
        Set<TopicUri> topicUris,
        ConsumerRebalanceListener consumerRebalanceListener,
        boolean shouldWakeup,
        boolean subscribe
    ) throws ConsumerException, ConfigurationException;

    /**
     * Creates/updates the backend consumer and returns an active consumer
     * The creator is responsible of reusing resources
     *
     * @param environment                  the Environment of the current PscConsumer instance
     * @param pscConfigurationInternal     the PscConfiguration of the current initialization of the consumer
     * @param consumerInterceptors         the consumer interceptors for consumer APIs
     * @param topicUri                     TopicUri that this backend should handle
     * @param shouldWakeup                 whether the retrieved consumers should be woken up
     * @param subscribe                    whether the retrieved consumers should subscribe to the given topic URIs
     * @return a Set of PscBackendConsumers that are active and should not be removed/unsubscribed
     * @throws ConsumerException      thrown if extracting a backend consumer fails due to invalid topic URI, or lack of
     *                                an applicable service discovery method, or ...
     * @throws ConfigurationException thrown if service discovery fails based on the provided configuration
     */
    public PscBackendConsumer<K, V> getConsumer(
            Environment environment,
            PscConfigurationInternal pscConfigurationInternal,
            ConsumerInterceptors<K, V> consumerInterceptors,
            TopicUri topicUri,
            boolean shouldWakeup,
            boolean subscribe
    ) throws ConsumerException, ConfigurationException {
        Set<PscBackendConsumer<K, V>> pscBackendConsumers = getConsumers(
                environment,
                pscConfigurationInternal,
                consumerInterceptors,
                Collections.singleton(topicUri),
                shouldWakeup,
                subscribe
        );

        if (pscBackendConsumers.isEmpty())
            throw new ConsumerException("No consumer was found for topic uri " + topicUri);

        return pscBackendConsumers.iterator().next();
    }

    /**
     * Creates/updates the backend consumer and returns an active consumer
     * The creator is responsible of reusing resources
     *
     * @param environment                  the Environment of the current PscConsumer instance
     * @param pscConfigurationInternal     the PscConfiguration of the current initialization of the consumer
     * @param consumerInterceptors         the consumer interceptors for consumer APIs
     * @param topicUri                     TopicUri that this backend should handle
     * @param consumerRebalanceListener    the ConsumerRebalanceListener callback interface implementation
     * @param shouldWakeup                 whether the retrieved consumers should be woken up
     * @param subscribe                    whether the retrieved consumers should subscribe to the given topic URIs
     * @return a Set of PscBackendConsumers that are active and should not be removed/unsubscribed
     * @throws ConsumerException      thrown if extracting a backend consumer fails due to invalid topic URI, or lack of
     *                                an applicable service discovery method, or ...
     * @throws ConfigurationException thrown if service discovery fails based on the provided configuration
     */
    public PscBackendConsumer<K, V> getConsumer(
        Environment environment,
        PscConfigurationInternal pscConfigurationInternal,
        ConsumerInterceptors<K, V> consumerInterceptors,
        ConsumerRebalanceListener consumerRebalanceListener,
        TopicUri topicUri,
        boolean shouldWakeup,
        boolean subscribe
    ) throws ConsumerException, ConfigurationException {
        Set<PscBackendConsumer<K, V>> pscBackendConsumers = getConsumers(
            environment,
            pscConfigurationInternal,
            consumerInterceptors,
            Collections.singleton(topicUri),
            consumerRebalanceListener,
            shouldWakeup,
            subscribe
        );

        if (pscBackendConsumers.isEmpty())
            throw new ConsumerException("No consumer was found for topic uri " + topicUri);

        return pscBackendConsumers.iterator().next();
    }

    /**
     * Creates/updates the backend consumer(s) and returns a set of active consumers
     * The creator is responsible of reusing resources
     *
     * @param environment                  the Environment of the current PscConsumer instance
     * @param pscConfigurationInternal     the PscConfiguration of the current initialization of the consumer
     * @param consumerInterceptors         the consumer interceptors for consumer APIs
     * @param topicUriPartitions           {@link TopicUriPartition}s that this backend should handle
     * @param shouldWakeup                 whether the retrieved consumers should be woken up
     * @param assign                       whether the given topic URI partitions should be assigned to retrieved consumers
     * @return a Set of PscBackendConsumers that are active and should not be removed/unsubscribed
     * @throws ConsumerException      thrown if extracting a backend consumer fails due to invalid topic URI, or lack of
     *                                an applicable service discovery method, or ...
     * @throws ConfigurationException thrown if service discovery fails based on the provided configuration
     */
    public abstract Set<PscBackendConsumer<K, V>> getAssignmentConsumers(
            Environment environment,
            PscConfigurationInternal pscConfigurationInternal,
            ConsumerInterceptors<K, V> consumerInterceptors,
            Set<TopicUriPartition> topicUriPartitions,
            boolean shouldWakeup,
            boolean assign
    ) throws ConsumerException, ConfigurationException;

    /**
     * Creates/updates the backend consumer and returns an active consumers
     * The creator is responsible of reusing resources
     *
     * @param environment                  the Environment of the current PscConsumer instance
     * @param pscConfigurationInternal     the PscConfiguration of the current initialization of the consumer
     * @param consumerInterceptors         the consumer interceptors for consumer APIs
     * @param topicUriPartition            {@link TopicUriPartition} that this backend should handle
     * @param shouldWakeup                 whether the retrieved consumers should be woken up
     * @param assign                       whether the given topic URI partitions should be assigned to retrieved consumers
     * @return a Set of PscBackendConsumers that are active and should not be removed/unsubscribed
     * @throws ConsumerException      thrown if extracting a backend consumer fails due to invalid topic URI, or lack of
     *                                an applicable service discovery method, or ...
     * @throws ConfigurationException thrown if service discovery fails based on the provided configuration
     */
    public PscBackendConsumer<K, V> getAssignmentConsumer(
            Environment environment,
            PscConfigurationInternal pscConfigurationInternal,
            ConsumerInterceptors<K, V> consumerInterceptors,
            TopicUriPartition topicUriPartition,
            boolean shouldWakeup,
            boolean assign
    ) throws ConsumerException, ConfigurationException {
        Set<PscBackendConsumer<K, V>> pscBackendConsumers = getAssignmentConsumers(
                environment,
                pscConfigurationInternal,
                consumerInterceptors,
                Collections.singleton(topicUriPartition),
                shouldWakeup,
                assign
        );

        if (pscBackendConsumers.isEmpty())
            throw new ConsumerException("No consumer was found for topic URI partition" + topicUriPartition);

        return pscBackendConsumers.iterator().next();
    }

    /**
     * Creates/updates the backend consumer(s) and returns a set of active consumers
     * The creator is responsible of reusing resources
     *
     * @param environment                  the Environment of the current PscConsumer instance
     * @param pscConfigurationInternal     the PscConfiguration of the current initialization of the consumer
     * @param consumerInterceptors         the consumer interceptors for consumer APIs
     * @param topicUriPartitions           {@link TopicUriPartition}s that this backend should handle
     * @param shouldWakeup                 whether the retrieved consumers should be woken up
     * @return a Set of PscBackendConsumers that are active and should not be removed/unsubscribed
     * @throws ConsumerException      thrown if extracting a backend consumer fails due to invalid topic URI, or lack of
     *                                an applicable service discovery method, or ...
     * @throws ConfigurationException thrown if service discovery fails based on the provided configuration
     */
    public abstract Set<PscBackendConsumer<K, V>> getCommitConsumers(
        Environment environment,
        PscConfigurationInternal pscConfigurationInternal,
        ConsumerInterceptors<K, V> consumerInterceptors,
        Set<TopicUriPartition> topicUriPartitions,
        boolean shouldWakeup
    ) throws ConsumerException, ConfigurationException;

    /**
     * Create/updates consumers suitable for performing commits for this backend
     * @param environment                  the Environment of the current PscConsumer instance
     * @param pscConfigurationInternal     the PscConfiguration of the current initialization of the consumer
     * @param consumerInterceptors         the consumer interceptors for consumer APIs
     * @param topicUriPartition            {@link TopicUriPartition} that this backend should handle
     * @param shouldWakeup                 whether the retrieved consumers should be woken up
     * @return a Set of PscBackendConsumers that are active and should not be removed/unsubscribed
     * @throws ConsumerException      thrown if extracting a backend consumer fails due to invalid topic URI, or lack of
     *                                an applicable service discovery method, or ...
     * @throws ConfigurationException thrown if service discovery fails based on the provided configuration
     */
    public PscBackendConsumer<K, V> getCommitConsumer(
        Environment environment,
        PscConfigurationInternal pscConfigurationInternal,
        ConsumerInterceptors<K, V> consumerInterceptors,
        TopicUriPartition topicUriPartition,
        boolean shouldWakeup
    ) throws ConsumerException, ConfigurationException {
        Set<PscBackendConsumer<K, V>> pscBackendConsumers = getCommitConsumers(
            environment,
            pscConfigurationInternal,
            consumerInterceptors,
            Collections.singleton(topicUriPartition),
            shouldWakeup
        );

        if (pscBackendConsumers.isEmpty())
            throw new ConsumerException("No consumer was found for topic URI partition" + topicUriPartition);

        return pscBackendConsumers.iterator().next();
    }

    public abstract TopicUri validateBackendTopicUri(TopicUri baseTopicUri) throws TopicUriSyntaxException;

    /**
     * Removes the given backend consumer associated with given topic URI partitions from the internal cache, so that
     * the next retrieval leads to creating a new backend consumer.
     *
     * @param topicUriPartitions set of topic URI partitions to use to retrieve the backend consumer
     * @param backendConsumer the backend consumer to dismiss
     */
    public void dismissBackendConsumer(Set<TopicUriPartition> topicUriPartitions, PscBackendConsumer<K, V> backendConsumer) {
        Set<String> topicUriPrefixes = topicUriPartitions.stream().map(topicUriPartition -> topicUriPartition.getTopicUri().getTopicUriPrefix()).collect(Collectors.toSet());
        for (String topicUriPrefix : topicUriPrefixes)
            clusterConsumerCache.remove(topicUriPrefix, backendConsumer);
    }

    /**
     * Optional handler when unsubscribe() is called on the PscConsumer and clean up needs to be done in the creator to release resources
     * of existing consumers.
     */
    public void reset() {
    }
}
