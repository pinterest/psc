package com.pinterest.flink.connector.psc;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;

import java.util.Properties;

public class PscFlinkConfiguration {

    /**
     * Configuration key for the cluster URI. This is required for the following client types that use the {@link com.pinterest.flink.connector.psc.source.PscSource}
     * and {@link com.pinterest.flink.connector.psc.sink.PscSink} classes:
     *
     * <li>PscSinks using EXACTLY-ONCE semantic: The cluster URI specifies the cluster to which the producer will connect.
     * When a PscSink is initialized with EXACTLY-ONCE semantic, it will need to create a transactional producer. Transactional
     * producers require a cluster URI to be specified because transaction lifecycles are managed by the backend
     * PubSub cluster, and it is generally not possible to have transactions span multiple clusters. The cluster URI
     * specified for a transactional producer must use the same protocol as the topic URI to which the producer
     *  is writing data. This is so that the {@link com.pinterest.psc.producer.PscProducer} can pre-construct a
     * {@link com.pinterest.psc.producer.PscBackendProducer} to perform transaction initialization logic upon
     *  creation of a producer prior to the first send() call.</li>
     *
     * <li>All PscSources: Unlike regular PscConsumers, consumers in {@link com.pinterest.flink.connector.psc.source.PscSource} require
     * cluster URI to connect to the cluster for metadata queries during their lifecycles. The cluster URI is supplied to
     * a {@link com.pinterest.psc.metadata.client.PscMetadataClient} for this purpose, therefore the supplied cluster URI
     * also needs to point to the same cluster as the one where the Source is reading from. For consumers, the protocol
     * of the supplied cluster URI does not need to match the protocol used in actual reads. The protocol for the cluster URI
     * needs to be one that can successfully perform metadata queries against the cluster by the
     * {@link com.pinterest.psc.metadata.client.PscMetadataClient}.</li>
     *
     * <p>Due to the above reasons, a single PscSink (only when using EXACTLY-ONCE semantic) cannot write to multiple clusters, and
     * a single PscSource (regardless of configuration) cannot read from multiple clusters.</p>
     *
     * <p>Format: The cluster URI must be a valid {@link TopicUri}, and typically looks exactly like a topic URI, but without the
     * topic name, and ending in a colon ":". For example, a cluster URI for a Kafka cluster "my_cluster" might look like: </p>
     *
     * <p><code>plaintext:/rn:kafka:env:cloud_region::my_cluster:</code></p>
     *
     * <p>Supplying the cluster URI above to an EXACTLY-ONCE PscSink would allow the producer to perform transactional writes
     * to topics in "my_cluster" using the same protocol (plaintext). If the producer needs to write in a different protocol,
     * that protocol should be specified in the topic URIs in the send() calls, and the cluster URI should be updated to match.</p>
     *
     * <p>Supplying the cluster URI above to a PscSource would allow the PscSource to perform metadata queries against
     * "my_cluster" using the same protocol (plaintext). If the PscSource needs to read using a different protocol,
     * that protocol should be specified in the topic URIs supplied to the PscSource, but the cluster URI does not
     * need to have the same protocol.</p>
     */
    public static final String CLUSTER_URI_CONFIG = "psc.cluster.uri";

    public static TopicUri validateAndGetBaseClusterUri(Properties properties) throws TopicUriSyntaxException {
        if (!properties.containsKey(CLUSTER_URI_CONFIG)) {
            throw new IllegalArgumentException("Cluster URI not found");
        }
        return BaseTopicUri.validate(properties.getProperty(CLUSTER_URI_CONFIG));
    }
}
