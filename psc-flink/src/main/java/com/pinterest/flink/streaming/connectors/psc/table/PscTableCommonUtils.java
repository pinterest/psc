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

package com.pinterest.flink.streaming.connectors.psc.table;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.metadata.TopicUriMetadata;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.SCAN_ENABLE_RESCALE;

/**
 * Common utility functions for PSC dynamic table source and sink.
 * 
 * <p>This class provides shared utility methods used across PSC table factories,
 * including smart rescale decision logic, metadata queries, and other common operations.
 */
public class PscTableCommonUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PscTableCommonUtils.class);

    /**
     * Functional interface for providing partition count.
     * Package-private for testing purposes only.
     */
    @FunctionalInterface
    interface PartitionCountProvider {
        /**
         * Retrieves the partition count for the given topic URIs.
         * 
         * @param topicUris List of topic URIs to query
         * @param pscProperties PSC properties for metadata client connection
         * @return Minimum partition count across all topics, or -1 if count cannot be determined
         */
        int getPartitionCount(List<String> topicUris, Properties pscProperties);
    }

    /**
     * Default partition count provider that uses real PSC metadata client.
     * Can be overridden for testing purposes.
     */
    private static PartitionCountProvider partitionCountProvider = 
        PscTableCommonUtils::getTopicPartitionCount;

    /**
     * Sets a custom partition count provider for testing.
     * Must be reset after test using {@link #resetProvider()}.
     * 
     * @param provider Custom partition count provider
     */
    @VisibleForTesting
    static synchronized void setProviderForTest(PartitionCountProvider provider) {
        partitionCountProvider = provider;
    }

    /**
     * Resets the partition count provider to the default implementation.
     * Should be called in test teardown to prevent test pollution.
     */
    @VisibleForTesting
    static synchronized void resetProvider() {
        partitionCountProvider = PscTableCommonUtils::getTopicPartitionCount;
    }

    /**
     * Determines the effective parallelism for the source by walking the following
     * fallback chain and returning the first source that yields a positive value:
     * <ol>
     *   <li>{@code scan.parallelism} (table-level override) &mdash; used if non-null and not -1</li>
     *   <li>{@code table.exec.resource.default-parallelism} &mdash; used if set and not -1</li>
     *   <li>Kafka partition count for the source topics (queried via PSC metadata client)</li>
     * </ol>
     *
     * <p>If none of the above produces a positive value (e.g. the partition-count query
     * fails), this method returns {@code -1} to signal "unknown".
     *
     * @param globalConfig    Global Flink configuration (read for table.exec.resource.default-parallelism)
     * @param topicUris       List of topic URIs used to query partition count
     * @param pscProperties   PSC properties for metadata client connection
     * @param scanParallelism Optional explicit scan.parallelism configuration
     * @return Effective parallelism, or -1 if it cannot be determined
     */
    public static int getEffectiveSourceParallelism(
            ReadableConfig globalConfig,
            List<String> topicUris,
            Properties pscProperties,
            @Nullable Integer scanParallelism) {

        // 1) scan.parallelism
        if (scanParallelism != null && scanParallelism > 0) {
            LOG.info("Effective source parallelism = {} (source: {})",
                    scanParallelism, PscConnectorOptions.SCAN_PARALLELISM.key());
            return scanParallelism;
        }

        // 2) table.exec.resource.default-parallelism
        Integer tableExecParallelism =
                globalConfig.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM);
        if (tableExecParallelism != null && tableExecParallelism > 0) {
            LOG.info("Effective source parallelism = {} (source: {})",
                    tableExecParallelism,
                    ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key());
            return tableExecParallelism;
        }

        // 3) Kafka partition count
        int partitionCount = partitionCountProvider.getPartitionCount(topicUris, pscProperties);
        if (partitionCount > 0) {
            LOG.info("Effective source parallelism = {} (source: kafka partition count)",
                    partitionCount);
            return partitionCount;
        }

        // 4) Unknown
        LOG.warn("Could not determine effective source parallelism: {} is unset/-1, {} is unset/-1, " +
                "and partition count could not be retrieved. Returning -1.",
                PscConnectorOptions.SCAN_PARALLELISM.key(),
                ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key());
        return -1;
    }

    /**
     * Queries the minimum partition count across all specified topic URIs.
     * 
     * <p>For multi-topic sources, returns the minimum partition count as a conservative approach.
     * If any topic has fewer partitions, that becomes the bottleneck.
     * 
     * <p>This method is used as the default implementation for {@link PartitionCountProvider}.
     * In tests, a mock provider can be injected via {@link #setProviderForTest(PartitionCountProvider)}.
     *
     * @param topicUris List of topic URIs to query
     * @param pscProperties PSC properties for metadata client connection
     * @return Minimum partition count across all topics, or -1 if count cannot be determined
     */
    private static int getTopicPartitionCount(List<String> topicUris, Properties pscProperties) {
        if (topicUris == null || topicUris.isEmpty()) {
            LOG.warn("No topic URIs provided for partition count query.");
            return -1;
        }

        PscMetadataClient metadataClient = null;
        try {
            // Create a copy of properties to avoid modifying the original
            Properties metadataClientProps = new Properties();
            metadataClientProps.putAll(pscProperties);
            
            // Set the required psc.metadata.client.id if not already present.
            // This is required by PscMetadataClient validation (see PscConfigurationInternal.validateMetadataClientConfiguration).
            // PscSourceEnumerator also sets this before creating its metadata client.
            if (!metadataClientProps.containsKey(PscConfiguration.PSC_METADATA_CLIENT_ID)) {
                metadataClientProps.setProperty(
                        PscConfiguration.PSC_METADATA_CLIENT_ID, 
                        "psc-table-partition-count-query");
            }
            
            // Convert properties to PSC configuration.
            // When passed to PscMetadataClient, it will be wrapped in PscConfigurationInternal
            // which loads psc.conf defaults and validates the configuration.
            PscConfiguration pscConfig = PscConfigurationUtils.propertiesToPscConfiguration(metadataClientProps);
            
            metadataClient = new PscMetadataClient(pscConfig);
            
            int minPartitionCount = Integer.MAX_VALUE;
            
            for (String topicUriStr : topicUris) {
                try {
                    TopicUri topicUri = TopicUri.validate(topicUriStr);
                    
                    // Query metadata for this topic
                    Map<TopicUri, TopicUriMetadata> metadataMap = metadataClient.describeTopicUris(
                            topicUri, // cluster URI (can use full topic URI)
                            java.util.Collections.singleton(topicUri),
                            Duration.ofSeconds(10));
                    
                    TopicUriMetadata metadata = metadataMap.get(topicUri);
                    if (metadata != null) {
                        int partitionCount = metadata.getTopicUriPartitions().size();
                        LOG.debug("Topic {} has {} partitions", topicUriStr, partitionCount);
                        minPartitionCount = Math.min(minPartitionCount, partitionCount);
                    } else {
                        LOG.warn("No metadata returned for topic {}", topicUriStr);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to query partition count for topic {}: {}", 
                            topicUriStr, e.getMessage());
                }
            }
            
            return (minPartitionCount == Integer.MAX_VALUE) ? -1 : minPartitionCount;
            
        } catch (Exception e) {
            LOG.warn("Failed to create PSC metadata client or query partition count: {}", 
                    e.getMessage());
            return -1;
        } finally {
            if (metadataClient != null) {
                try {
                    metadataClient.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close PSC metadata client: {}", e.getMessage());
                }
            }
        }
    }

    /** Private constructor to prevent instantiation. */
    private PscTableCommonUtils() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }
}


