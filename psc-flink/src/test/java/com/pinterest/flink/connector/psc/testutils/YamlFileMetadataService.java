/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.connector.psc.testutils;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.connector.psc.dynamic.metadata.ClusterMetadata;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscMetadataService;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Reads metadata from yaml file and lazily refreshes periodically. This implementation assumes that
 * specified topics exist in the clusters that are contained in the yaml metadata. Therefore, topic
 * is used as the stream name. This is designed to integrate with K8s configmap and cluster
 * migration.
 *
 * <p>Files must be of the form:
 *
 * <pre>{@code
 * - streamId: stream0
 *   clusterMetadataList:
 *     - clusterId: cluster0
 *       bootstrapServers: bootstrap-server-0:443
 *       topics:
 *         - topic0
 *         - topic1
 *     - clusterId: cluster1
 *       bootstrapServers: bootstrap-server-1:443
 *       topics:
 *         - topic2
 *         - topic3
 * - streamId: stream1
 *   clusterMetadataList:
 *     - clusterId: cluster2
 *       bootstrapServers: bootstrap-server-2:443
 *       topics:
 *         - topic4
 *         - topic5
 * }</pre>
 *
 * <p>Typically, usage will look like: first consuming from one cluster, second adding new cluster
 * and consuming from both clusters, and third consuming from only from the new cluster after all
 * data from the old cluster has been read.
 */
public class YamlFileMetadataService implements PscMetadataService {
    private static final Logger logger = LoggerFactory.getLogger(YamlFileMetadataService.class);
    private final String metadataFilePath;
    private final Duration refreshInterval;
    private Instant lastRefresh;
    // current metadata should be accessed from #getAllStreams()
    private transient Set<PscStream> streamMetadata;
    private transient Yaml yaml;

    /**
     * Constructs a metadata service based on cluster information stored in a file.
     *
     * @param metadataFilePath location of the metadata file
     * @param metadataTtl ttl of metadata that controls how often to refresh
     */
    public YamlFileMetadataService(String metadataFilePath, Duration metadataTtl) {
        this.metadataFilePath = metadataFilePath;
        this.refreshInterval = metadataTtl;
        this.lastRefresh = Instant.MIN;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This obtains the all stream metadata and enforces the ttl configuration on the metadata.
     */
    @Override
    public Set<PscStream> getAllStreams() {
        refreshIfNeeded();
        return streamMetadata;
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, PscStream> describeStreams(Collection<String> streamIds) {
        ImmutableMap.Builder<String, PscStream> builder = ImmutableMap.builder();
        Set<PscStream> streams = getAllStreams();
        for (PscStream stream : streams) {
            if (streamIds.contains(stream.getStreamId())) {
                builder.put(stream.getStreamId(), stream);
            }
        }

        return builder.build();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClusterActive(String kafkaClusterId) {
        return getAllStreams().stream()
                .flatMap(kafkaStream -> kafkaStream.getClusterMetadataMap().keySet().stream())
                .anyMatch(cluster -> cluster.equals(kafkaClusterId));
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {}

    /**
     * A utility method for writing metadata in the expected yaml format.
     *
     * @param streamMetadata list of {@link StreamMetadata}
     * @param metadataFile the metadata {@link File}
     */
    public static void saveToYaml(List<StreamMetadata> streamMetadata, File metadataFile)
            throws IOException {
        logger.debug("Writing stream infos to file: {}", streamMetadata);
        Yaml yaml = initYamlParser();
        FileWriter fileWriter = new FileWriter(metadataFile, false);
        yaml.dump(streamMetadata, fileWriter);
        fileWriter.close();
    }

    private void refreshIfNeeded() {
        Instant now = Instant.now();
        try {
            if (now.isAfter(lastRefresh.plus(refreshInterval.toMillis(), ChronoUnit.MILLIS))) {
                streamMetadata = parseFile();
                lastRefresh = now;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @VisibleForTesting
    Set<PscStream> parseFile() throws IOException {
        if (yaml == null) {
            yaml = initYamlParser();
        }

        List<StreamMetadata> streamMetadataList =
                yaml.load(Files.newInputStream(Paths.get(metadataFilePath)));
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Input stream of metadata file has size: {}",
                    Files.newInputStream(Paths.get(metadataFilePath)).available());
        }
        Set<PscStream> kafkaStreams = new HashSet<>();

        for (StreamMetadata streamMetadata : streamMetadataList) {
            Map<String, ClusterMetadata> clusterMetadataMap = new HashMap<>();

            for (StreamMetadata.ClusterMetadata clusterMetadata :
                    streamMetadata.getClusterMetadataList()) {
                final String kafkaClusterId;
                if (clusterMetadata.getClusterId() != null) {
                    kafkaClusterId = clusterMetadata.getClusterId();
                } else {
                    kafkaClusterId = clusterMetadata.getClusterUriString();
                }

                Properties properties = new Properties();
                properties.setProperty(
                        PscFlinkConfiguration.CLUSTER_URI_CONFIG,
                        clusterMetadata.getClusterUriString());
                PscTestUtils.putDiscoveryProperties(properties, clusterMetadata.getBootstrapServers(), clusterMetadata.getClusterUriString());
                System.out.println("properties: " + properties);
                clusterMetadataMap.put(
                        kafkaClusterId,
                        new ClusterMetadata(
                                new HashSet<>(clusterMetadata.getTopics()), properties));
            }

            kafkaStreams.add(new PscStream(streamMetadata.getStreamId(), clusterMetadataMap));
        }

        logger.debug("From {} loaded metadata: {}", metadataFilePath, kafkaStreams);
        return kafkaStreams;
    }

    private static Yaml initYamlParser() {
        DumperOptions dumperOptions = new DumperOptions();
        Representer representer = new Representer(dumperOptions);
        representer.addClassTag(StreamMetadata.class, Tag.MAP);
        TypeDescription typeDescription = new TypeDescription(StreamMetadata.class);
        representer.addTypeDescription(typeDescription);
        representer.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        LoaderOptions loaderOptions = new LoaderOptions();
        // Allow global tag for StreamMetadata
        loaderOptions.setTagInspector(
                tag -> tag.getClassName().equals(StreamMetadata.class.getName()));
        return new Yaml(new ListConstructor<>(StreamMetadata.class, loaderOptions), representer);
    }

    /** A custom constructor is required to read yaml lists at the root. */
    private static class ListConstructor<T> extends Constructor {
        private final Class<T> clazz;

        public ListConstructor(final Class<T> clazz, final LoaderOptions loaderOptions) {
            super(loaderOptions);
            this.clazz = clazz;
        }

        @Override
        protected Object constructObject(final Node node) {
            if (node instanceof SequenceNode && isRootNode(node)) {
                ((SequenceNode) node).setListType(clazz);
            }
            return super.constructObject(node);
        }

        private boolean isRootNode(final Node node) {
            return node.getStartMark().getIndex() == 0;
        }
    }

    /** Internal class for snake yaml parsing. A mutable, no arg, public class is required. */
    public static class StreamMetadata {

        private String streamId;
        private List<ClusterMetadata> clusterMetadataList;

        public StreamMetadata() {}

        public StreamMetadata(String streamId, List<ClusterMetadata> clusterMetadataList) {
            this.streamId = streamId;
            this.clusterMetadataList = clusterMetadataList;
        }

        public String getStreamId() {
            return streamId;
        }

        public void setStreamId(String streamId) {
            this.streamId = streamId;
        }

        public List<ClusterMetadata> getClusterMetadataList() {
            return clusterMetadataList;
        }

        public void setClusterMetadataList(List<ClusterMetadata> clusterMetadata) {
            this.clusterMetadataList = clusterMetadata;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("streamId", streamId)
                    .add("clusterMetadataList", clusterMetadataList)
                    .toString();
        }

        /** Information to connect to a particular cluster. */
        public static class ClusterMetadata {
            private String clusterId;
            private String clusterUriString;
            private List<String> topics;
            private String bootstrapServers;

            public ClusterMetadata() {}

            public ClusterMetadata(String clusterId, String clusterUriString, List<String> topics, String bootstrapServers) {
                this.clusterId = clusterId;
                this.clusterUriString = clusterUriString;
                this.topics = topics;
                this.bootstrapServers = bootstrapServers;
            }

            public String getClusterId() {
                return clusterId;
            }

            public void setClusterId(String clusterId) {
                this.clusterId = clusterId;
            }

            public String getClusterUriString() {
                return clusterUriString;
            }

            public void setClusterUriString(String clusterUriString) {
                this.clusterUriString = clusterUriString;
            }

            public List<String> getTopics() {
                return topics;
            }

            public void setTopics(List<String> topics) {
                this.topics = topics;
            }

            public String getBootstrapServers() {
                return bootstrapServers;
            }

            public void setBootstrapServers(String bootstrapServers) {
                this.bootstrapServers = bootstrapServers;
            }
        }
    }
}
