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
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PscTableSourceUtil}.
 */
public class PscTableSourceUtilTest {

    @Test
    public void testInferParallelismWithInferenceDisabled() {
        Configuration config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 16);
        config.set(PscConnectorOptions.INFER_SCAN_PARALLELISM, false);
        
        // Supplier should not be called when inference is disabled
        int parallelism = PscTableSourceUtil.inferParallelism(config, () -> {
            throw new RuntimeException("Supplier should not be called");
        });
        
        // Should use Flink's default parallelism
        assertThat(parallelism).isEqualTo(16);
    }

    @Test
    public void testInferParallelismWithInferenceEnabled() {
        Configuration config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 16);
        config.set(PscConnectorOptions.INFER_SCAN_PARALLELISM, true);
        config.set(PscConnectorOptions.INFER_SCAN_PARALLELISM_MAX, 50);
        
        // Supplier returns 30 partitions
        int parallelism = PscTableSourceUtil.inferParallelism(config, () -> 30);
        
        // Should use partition count since it's less than max
        assertThat(parallelism).isEqualTo(30);
    }

    @Test
    public void testInferParallelismRespectsMaxCap() {
        Configuration config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 16);
        config.set(PscConnectorOptions.INFER_SCAN_PARALLELISM, true);
        config.set(PscConnectorOptions.INFER_SCAN_PARALLELISM_MAX, 50);
        
        // Supplier returns 200 partitions (more than max)
        int parallelism = PscTableSourceUtil.inferParallelism(config, () -> 200);
        
        // Should cap at max value
        assertThat(parallelism).isEqualTo(50);
    }

    @Test
    public void testInferParallelismWithZeroPartitions() {
        Configuration config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 16);
        config.set(PscConnectorOptions.INFER_SCAN_PARALLELISM, true);
        config.set(PscConnectorOptions.INFER_SCAN_PARALLELISM_MAX, 50);
        
        // Supplier returns 0 partitions
        int parallelism = PscTableSourceUtil.inferParallelism(config, () -> 0);
        
        // Should ensure minimum of 1
        assertThat(parallelism).isEqualTo(1);
    }

    @Test
    public void testInferParallelismWithDefaultMax() {
        Configuration config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 16);
        config.set(PscConnectorOptions.INFER_SCAN_PARALLELISM, true);
        // Don't set max, use default (128)
        
        // Supplier returns 150 partitions
        int parallelism = PscTableSourceUtil.inferParallelism(config, () -> 150);
        
        // Should cap at default max value (128)
        assertThat(parallelism).isEqualTo(128);
    }

    @Test
    public void testInferParallelismLazyEvaluation() {
        Configuration config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 16);
        config.set(PscConnectorOptions.INFER_SCAN_PARALLELISM, false);
        
        boolean[] supplierCalled = {false};
        
        // Supplier should NOT be called when inference is disabled
        int parallelism = PscTableSourceUtil.inferParallelism(config, () -> {
            supplierCalled[0] = true;
            return 100;
        });
        
        assertThat(parallelism).isEqualTo(16);
        assertThat(supplierCalled[0]).isFalse(); // Verify lazy evaluation
    }
}

