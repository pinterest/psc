/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.streaming.connectors.psc.table;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metadata.TopicUriMetadata;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Utility class for PSC table source operations, particularly parallelism inference.
 * Inspired by Iceberg's SourceUtil pattern.
 */
public class PscTableSourceUtil {
    
    private static final Logger LOG = LoggerFactory.getLogger(PscTableSourceUtil.class);
    private static final Duration METADATA_TIMEOUT = Duration.ofSeconds(30);

    private PscTableSourceUtil() {
        // Utility class, no instantiation
    }

    /**
     * Infer source parallelism based on topic partition count, similar to Iceberg's approach.
     * 
     * <p>The inference logic follows this priority:
     * <ol>
     *   <li>Start with Flink's global default parallelism from {@link ExecutionConfigOptions#TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM}</li>
     *   <li>If {@link PscConnectorOptions#INFER_SCAN_PARALLELISM} is enabled, use partition count</li>
     *   <li>Cap the result at {@link PscConnectorOptions#INFER_SCAN_PARALLELISM_MAX}</li>
     *   <li>Ensure result is at least 1</li>
     * </ol>
     *
     * @param readableConfig Flink configuration
     * @param partitionCountProvider Lazy supplier of partition count (expensive metadata operation)
     * @return Inferred parallelism value
     */
    public static int inferParallelism(
            ReadableConfig readableConfig,
            Supplier<Integer> partitionCountProvider) {
        
        // 1. Start with Flink's global default parallelism
        int parallelism = readableConfig.get(
            ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM);
        
        LOG.debug("Starting parallelism inference with Flink default: {}", parallelism);
        
        // 2. Check if inference is enabled
        if (readableConfig.get(PscConnectorOptions.INFER_SCAN_PARALLELISM)) {
            int maxInferParallelism = readableConfig.get(
                PscConnectorOptions.INFER_SCAN_PARALLELISM_MAX);
            
            // Get actual partition count (lazy evaluation to avoid unnecessary metadata calls)
            int partitionCount = partitionCountProvider.get();
            LOG.debug("Partition count from provider: {}", partitionCount);
            
            // Use minimum of partition count and max cap
            parallelism = Math.min(partitionCount, maxInferParallelism);
            LOG.info("Inferred parallelism from {} partitions: {} (max cap: {})", 
                     partitionCount, parallelism, maxInferParallelism);
        } else {
            LOG.debug("Parallelism inference is disabled, using default: {}", parallelism);
        }
        
        // 3. Ensure parallelism is at least 1
        parallelism = Math.max(1, parallelism);
        LOG.debug("Final inferred parallelism: {}", parallelism);
        
        return parallelism;
    }

    /**
     * Get partition count for given topic URIs by querying PSC metadata.
     * 
     * <p>This method creates a temporary {@link PscMetadataClient} to query topic metadata
     * and counts the total number of partitions across all specified topics.
     *
     * @param properties PSC properties with cluster configuration
     * @param topicUriStrings List of topic URI strings to query
     * @param clusterUriString Cluster URI string for metadata client
     * @return Total partition count across all topics
     * @throws RuntimeException if metadata query fails
     */
    public static int getPartitionCount(
            Properties properties,
            List<String> topicUriStrings,
            String clusterUriString) {
        
        LOG.debug("Fetching partition count for {} topics from cluster {}", 
                  topicUriStrings.size(), clusterUriString);
        
        if (topicUriStrings == null || topicUriStrings.isEmpty()) {
            LOG.warn("No topics specified, returning partition count of 0");
            return 0;
        }
        
        Properties metadataProps = new Properties();
        metadataProps.putAll(properties);
        
        // Create temporary metadata client
        try (PscMetadataClient metadataClient = new PscMetadataClient(
                PscConfigurationUtils.propertiesToPscConfiguration(metadataProps))) {
            
            // Parse cluster URI
            TopicUri baseClusterUri = TopicUri.validate(clusterUriString);
            
            // Parse topic URIs
            Set<TopicUri> topicUriSet = new HashSet<>();
            for (String topicUriStr : topicUriStrings) {
                try {
                    topicUriSet.add(TopicUri.validate(topicUriStr));
                } catch (TopicUriSyntaxException e) {
                    LOG.warn("Failed to parse topic URI: {}, skipping", topicUriStr, e);
                }
            }
            
            if (topicUriSet.isEmpty()) {
                LOG.warn("No valid topic URIs after parsing, returning partition count of 0");
                return 0;
            }
            
            // Query metadata for all topics
            Map<TopicUri, TopicUriMetadata> metadata = metadataClient.describeTopicUris(
                baseClusterUri,
                topicUriSet,
                METADATA_TIMEOUT);
            
            // Sum up partition counts across all topics
            int totalPartitions = metadata.values().stream()
                .mapToInt(m -> m.getTopicUriPartitions().size())
                .sum();
            
            LOG.info("Total partitions across {} topics: {}", topicUriSet.size(), totalPartitions);
            return totalPartitions;
                
        } catch (ConfigurationException e) {
            throw new RuntimeException("Failed to create PscMetadataClient for partition count query", e);
        } catch (TopicUriSyntaxException e) {
            throw new RuntimeException("Failed to parse cluster URI: " + clusterUriString, e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch partition count for parallelism inference", e);
        }
    }
}

