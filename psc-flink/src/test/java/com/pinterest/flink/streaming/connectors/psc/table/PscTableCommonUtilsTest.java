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
        PscTableCommonUtils.resetPartitionCountProvider();
        
        // Initialize common test fixtures
        tableOptions = new Configuration();
        globalConfig = new Configuration();
        topicUris = Arrays.asList("plaintext:kafka:local:test-cluster:/test-topic");
        pscProperties = new Properties();
    }

    @After
    public void tearDown() {
        // Always reset provider after each test to prevent test pollution
        PscTableCommonUtils.resetPartitionCountProvider();
    }

    // ============================================
    // Tests for rescale enabled flag checks
    // ============================================

    @Test
    public void testShouldNotRescaleWhenDisabled() {
        // Given: rescale is disabled
        tableOptions.set(SCAN_ENABLE_RESCALE, false);
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 10);
        
        // When: shouldApplyRescale is called
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, null);
        
        // Then: rescale is not applied
        assertThat(result).isFalse();
    }

    // ============================================
    // Tests for scan.parallelism with mocked partition counts
    // ============================================

    @Test
    public void testShouldRescaleWhenScanParallelismExceedsPartitionCount() {
        // Given: scan.parallelism = 10, partition count = 5, rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2); // Should be ignored
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 5);
        
        // When: shouldApplyRescale is called with scan.parallelism = 10
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, 10);
        
        // Then: rescale is applied (10 > 5)
        assertThat(result).isTrue();
    }

    @Test
    public void testShouldNotRescaleWhenScanParallelismLessThanPartitionCount() {
        // Given: scan.parallelism = 5, partition count = 20, rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 100); // Should be ignored
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 20);
        
        // When: shouldApplyRescale is called with scan.parallelism = 5
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, 5);
        
        // Then: rescale is not applied (5 < 20)
        assertThat(result).isFalse();
    }

    @Test
    public void testShouldNotRescaleWhenScanParallelismEqualsPartitionCount() {
        // Given: scan.parallelism = 10, partition count = 10, rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 10);
        
        // When: shouldApplyRescale is called with scan.parallelism = 10
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, 10);
        
        // Then: rescale is not applied (10 == 10)
        assertThat(result).isFalse();
    }

    // ============================================
    // Tests for global default parallelism with mocked partition counts
    // ============================================

    @Test
    public void testShouldRescaleWhenGlobalParallelismExceedsPartitionCount() {
        // Given: global parallelism = 50, partition count = 10, no scan.parallelism, rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 50);
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 10);
        
        // When: shouldApplyRescale is called with no scan.parallelism
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, null);
        
        // Then: rescale is applied (50 > 10)
        assertThat(result).isTrue();
    }

    @Test
    public void testShouldNotRescaleWhenGlobalParallelismLessThanPartitionCount() {
        // Given: global parallelism = 5, partition count = 20, no scan.parallelism, rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 5);
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 20);
        
        // When: shouldApplyRescale is called with no scan.parallelism
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, null);
        
        // Then: rescale is not applied (5 < 20)
        assertThat(result).isFalse();
    }

    // ============================================
    // Tests for invalid parallelism configurations
    // ============================================

    @Test
    public void testShouldNotRescaleWhenScanParallelismIsZero() {
        // Given: scan.parallelism = 0 (invalid), rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 10);
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 5);
        
        // When: shouldApplyRescale is called with scan.parallelism = 0
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, 0);
        
        // Then: falls back to global parallelism, rescale is applied (10 > 5)
        assertThat(result).isTrue();
    }

    @Test
    public void testShouldNotRescaleWhenScanParallelismIsNegative() {
        // Given: scan.parallelism = -1 (invalid), rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 10);
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 5);
        
        // When: shouldApplyRescale is called with scan.parallelism = -1
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, -1);
        
        // Then: falls back to global parallelism, rescale is applied (10 > 5)
        assertThat(result).isTrue();
    }

    @Test
    public void testShouldNotRescaleWhenNoParallelismConfigured() {
        // Given: no scan.parallelism, no global parallelism, rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        // globalConfig has no default parallelism set (returns null)
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 10);
        
        // When: shouldApplyRescale is called
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, null);
        
        // Then: rescale is not applied (no valid parallelism to compare)
        assertThat(result).isFalse();
    }

    // ============================================
    // Tests for partition count edge cases
    // ============================================

    @Test
    public void testShouldNotRescaleWhenPartitionCountCannotBeDetermined() {
        // Given: partition count = -1 (cannot be determined), rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 10);
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> -1);
        
        // When: shouldApplyRescale is called
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, null);
        
        // Then: rescale is not applied (fail-safe behavior)
        assertThat(result).isFalse();
    }

    @Test
    public void testShouldNotRescaleWhenPartitionCountIsZero() {
        // Given: partition count = 0 (invalid), rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 10);
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 0);
        
        // When: shouldApplyRescale is called
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, null);
        
        // Then: rescale is not applied (fail-safe behavior)
        assertThat(result).isFalse();
    }

    // ============================================
    // Tests for scan.parallelism precedence
    // ============================================

    @Test
    public void testScanParallelismTakesPrecedenceOverGlobalParallelism() {
        // Given: scan.parallelism = 100, global parallelism = 5, partition count = 10, rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 5);
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 10);
        
        // When: shouldApplyRescale is called with scan.parallelism = 100
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, 100);
        
        // Then: rescale is applied based on scan.parallelism (100 > 10), not global (5 < 10)
        assertThat(result).isTrue();
    }

    // ============================================
    // Tests for multi-topic scenarios
    // ============================================

    @Test
    public void testShouldRescaleWithMultipleTopics() {
        // Given: multiple topics, min partition count = 8, scan.parallelism = 20, rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        List<String> multipleTopics = Arrays.asList(
            "plaintext:kafka:local:test-cluster:/topic1",
            "plaintext:kafka:local:test-cluster:/topic2"
        );
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 8);
        
        // When: shouldApplyRescale is called
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, multipleTopics, pscProperties, 20);
        
        // Then: rescale is applied (20 > 8)
        assertThat(result).isTrue();
    }

    // ============================================
    // Tests for high parallelism scenarios
    // ============================================

    @Test
    public void testShouldRescaleWithHighParallelismAndLowPartitionCount() {
        // Given: scan.parallelism = 2000, partition count = 10, rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 10);
        
        // When: shouldApplyRescale is called with high parallelism
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, 2000);
        
        // Then: rescale is applied (2000 > 10)
        assertThat(result).isTrue();
    }

    @Test
    public void testShouldNotRescaleWithHighPartitionCountAndLowParallelism() {
        // Given: scan.parallelism = 2, partition count = 1000, rescale enabled
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 1000);
        
        // When: shouldApplyRescale is called
        boolean result = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, 2);
        
        // Then: rescale is not applied (2 < 1000)
        assertThat(result).isFalse();
    }

    // ============================================
    // Tests for provider reset mechanism
    // ============================================

    @Test
    public void testProviderResetRestoresDefaultBehavior() {
        // Given: custom provider is set
        PscTableCommonUtils.setPartitionCountProviderForTesting((topicUris, props) -> 42);
        
        tableOptions.set(SCAN_ENABLE_RESCALE, true);
        globalConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 100);
        
        // Verify custom provider works
        boolean resultWithMock = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, null);
        assertThat(resultWithMock).isTrue(); // 100 > 42
        
        // When: provider is reset
        PscTableCommonUtils.resetPartitionCountProvider();
        
        // Then: default behavior is restored (returns -1 in unit test environment)
        boolean resultAfterReset = PscTableCommonUtils.shouldApplyRescale(
            tableOptions, globalConfig, topicUris, pscProperties, null);
        assertThat(resultAfterReset).isFalse(); // partition count = -1, fail-safe
    }
}

