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
import com.pinterest.psc.metadata.TopicUriMetadata;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.SCAN_ENABLE_RESCALE;

/**
 * Utility class for smart rescale decision logic shared between PSC table factories.
 * 
 * <p>This class provides methods to determine whether rescale() should be applied to a PSC source
 * based on comparing the configured pipeline parallelism with the actual topic partition count.
 */
public class PscRescaleUtil {

    private static final Logger LOG = LoggerFactory.getLogger(PscRescaleUtil.class);

    /**
     * Determines whether rescale() should be applied based on:
     * 1. scan.enable-rescale flag must be true
     * 2. Global parallelism (table.exec.resource.default-parallelism) > partition count
     * 
     * <p>This automatically avoids unnecessary shuffle overhead when partitions >= parallelism.
     *
     * @param tableOptions User's table configuration options
     * @param globalConfig Global Flink configuration
     * @param topicUris List of topic URIs to query for partition counts
     * @param pscProperties PSC properties for metadata client connection
     * @return true if rescale should be applied, false otherwise
     */
    public static boolean shouldApplyRescale(
            ReadableConfig tableOptions,
            ReadableConfig globalConfig,
            List<String> topicUris,
            Properties pscProperties) {
        
        // First check if rescale is enabled by user
        if (!tableOptions.get(SCAN_ENABLE_RESCALE)) {
            return false;
        }

        // Get the global default parallelism
        int defaultParallelism = globalConfig.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM);
        
        // If parallelism is -1 (not set) or <= 0, cannot determine, so don't apply rescale
        if (defaultParallelism <= 0) {
            LOG.info("scan.enable-rescale is true, but table.exec.resource.default-parallelism is {} (not set). " +
                    "Rescale will not be applied. Set a positive parallelism value to enable automatic rescale.", 
                    defaultParallelism);
            return false;
        }

        // Query partition count from PSC metadata
        int partitionCount = getTopicPartitionCount(topicUris, pscProperties);
        
        // If partition count couldn't be determined, don't apply rescale (fail-safe)
        if (partitionCount <= 0) {
            LOG.warn("scan.enable-rescale is true, but partition count could not be determined. " +
                    "Rescale will not be applied.");
            return false;
        }

        // Apply rescale only if user-configured parallelism exceeds partition count
        boolean shouldRescale = defaultParallelism > partitionCount;
        
        if (shouldRescale) {
            LOG.info("Applying rescale(): configured parallelism ({}) > partition count ({}). " +
                    "Data will be redistributed to fully utilize downstream operators.",
                    defaultParallelism, partitionCount);
        } else {
            LOG.info("Skipping rescale(): configured parallelism ({}) <= partition count ({}). " +
                    "No shuffle needed as source will naturally match or exceed desired parallelism.",
                    defaultParallelism, partitionCount);
        }
        
        return shouldRescale;
    }

    /**
     * Queries the minimum partition count across all specified topic URIs.
     * 
     * <p>For multi-topic sources, returns the minimum partition count as a conservative approach.
     * If any topic has fewer partitions, that becomes the bottleneck.
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
            // Create PSC configuration from properties
            PscConfiguration pscConfig = new PscConfiguration();
            for (String key : pscProperties.stringPropertyNames()) {
                pscConfig.setProperty(key, pscProperties.getProperty(key));
            }
            
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
    private PscRescaleUtil() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }
}

