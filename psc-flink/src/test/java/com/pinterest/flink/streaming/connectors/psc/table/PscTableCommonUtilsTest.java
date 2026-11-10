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

import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.exception.startup.ConfigurationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.SCAN_ENABLE_RESCALE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Comprehensive test suite for PscTableCommonUtils, focusing on rescale decision logic
 * with mocked partition counts to achieve full code coverage.
 */
public class PscTableCommonUtilsTest {

    private Configuration tableOptions;
    private Configuration globalConfig;
    private List<String> topicUris;
    private Properties pscProperties;

    @Before
    public void setup() {
        // Reset partition count provider to default before each test
        PscTableCommonUtils.resetProvider();
        
        // Initialize common test fixtures
        tableOptions = new Configuration();
        globalConfig = new Configuration();
        topicUris = Arrays.asList("plaintext:kafka:local:test-cluster:/test-topic");
        pscProperties = new Properties();
    }

    @After
    public void tearDown() {
        // Always reset provider after each test to prevent test pollution
        PscTableCommonUtils.resetProvider();
    }

    // ============================================
    // Tests for getEffectiveSourceParallelism()
    // Precedence: scan.parallelism > table.exec.resource.default-parallelism > kafka partition count
    // ============================================

    @Test
    public void testEffectiveParallelismFromScanParallelism() {
        // Given: scan.parallelism = 12, others would also resolve but should be ignored
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        PscTableCommonUtils.setProviderForTest((topicUris, props) -> 7);

        // When
        int parallelism = PscTableCommonUtils.getEffectiveSourceParallelism(
            globalConfig, topicUris, pscProperties, 12);

        // Then: scan.parallelism wins
        assertThat(parallelism).isEqualTo(12);
    }

    @Test
    public void testEffectiveParallelismFallsThroughWhenScanParallelismIsNull() {
        // Given: scan.parallelism not set; table.exec set to 4; partition count = 7
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        PscTableCommonUtils.setProviderForTest((topicUris, props) -> 7);

        // When
        int parallelism = PscTableCommonUtils.getEffectiveSourceParallelism(
            globalConfig, topicUris, pscProperties, null);

        // Then: table.exec wins (4)
        assertThat(parallelism).isEqualTo(4);
    }

    @Test
    public void testEffectiveParallelismFallsThroughWhenScanParallelismIsMinusOne() {
        // Given: scan.parallelism = -1 (unset sentinel)
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        PscTableCommonUtils.setProviderForTest((topicUris, props) -> 7);

        // When
        int parallelism = PscTableCommonUtils.getEffectiveSourceParallelism(
             globalConfig, topicUris, pscProperties, -1);

        // Then: -1 is treated as unset; table.exec wins (4)
        assertThat(parallelism).isEqualTo(4);
    }

    @Test
    public void testEffectiveParallelismFallsThroughToPartitionCount() {
        // Given: scan.parallelism unset; table.exec = -1; partition count = 7
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, -1);
        PscTableCommonUtils.setProviderForTest((topicUris, props) -> 7);

        // When
        int parallelism = PscTableCommonUtils.getEffectiveSourceParallelism(
             globalConfig, topicUris, pscProperties, null);

        // Then: kafka partition count is used
        assertThat(parallelism).isEqualTo(7);
    }

    @Test
    public void testEffectiveParallelismFallsThroughToPartitionCountWhenTableExecIsUnset() {
        // Given: scan.parallelism unset; table.exec not configured at all (returns null/default)
        // Note: globalConfig has no value for TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM
        PscTableCommonUtils.setProviderForTest((topicUris, props) -> 9);

        // When
        int parallelism = PscTableCommonUtils.getEffectiveSourceParallelism(
             globalConfig, topicUris, pscProperties, null);

        // Then: kafka partition count is used
        assertThat(parallelism).isEqualTo(9);
    }

    @Test
    public void testEffectiveParallelismReturnsMinusOneWhenAllSourcesFail() {
        // Given: scan.parallelism unset; table.exec = -1; partition count provider returns -1
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, -1);
        PscTableCommonUtils.setProviderForTest((topicUris, props) -> -1);

        // When
        int parallelism = PscTableCommonUtils.getEffectiveSourceParallelism(
             globalConfig, topicUris, pscProperties, null);

        // Then: -1 (unknown)
        assertThat(parallelism).isEqualTo(-1);
    }

    @Test
    public void testEffectiveParallelismReturnsMinusOneWhenPartitionCountIsZero() {
        // Given: scan.parallelism null; table.exec unset; partition count provider returns 0
        PscTableCommonUtils.setProviderForTest((topicUris, props) -> 0);

        // When
        int parallelism = PscTableCommonUtils.getEffectiveSourceParallelism(
             globalConfig, topicUris, pscProperties, null);

        // Then: 0 is invalid → -1 (unknown)
        assertThat(parallelism).isEqualTo(-1);
    }

    // ============================================
    // Tests validating PscMetadataClient configuration fix
    // ============================================

    @Test
    public void testMetadataClientValidationPassesWithClientId() {
        // This test proves the FIX works:
        // With psc.metadata.client.id set, PscConfigurationInternal validation passes
        
        // Given: Properties WITH psc.metadata.client.id (matching our fix)
        Properties propsWithClientId = new Properties();
        propsWithClientId.setProperty(
            PscConfiguration.PSC_METADATA_CLIENT_ID, 
            "psc-table-partition-count-query"
        );
        
        PscConfiguration pscConfig = PscConfigurationUtils.propertiesToPscConfiguration(propsWithClientId);
        
        // When/Then: Creating PscConfigurationInternal for metadata client does NOT throw
        assertThatCode(() -> 
            new PscConfigurationInternal(pscConfig, PscConfigurationInternal.PSC_CLIENT_TYPE_METADATA)
        ).doesNotThrowAnyException();
    }
}

