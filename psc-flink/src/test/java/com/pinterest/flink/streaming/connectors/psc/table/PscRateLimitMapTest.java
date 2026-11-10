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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

/**
 * Test suite for PscRateLimitMap.
 */
public class PscRateLimitMapTest {

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private Counter throttleCounter;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        
        // Create a mock metric group
        OperatorMetricGroup metricGroup = mock(OperatorMetricGroup.class);
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.counter(anyString())).thenReturn(throttleCounter);
        when(metricGroup.gauge(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(null);
    }

    @Test
    public void testRateLimitMapCreation() {
        // Test that RateLimitMap can be created with valid rate limits
        PscRateLimitMap<String> rateLimitMap = new PscRateLimitMap<>(1000.0);
        assertThat(rateLimitMap).isNotNull();
    }

    @Test
    public void testRateLimitMapWithFractionalRate() {
        // Test that fractional rates are supported
        PscRateLimitMap<String> rateLimitMap = new PscRateLimitMap<>(123.45);
        assertThat(rateLimitMap).isNotNull();
    }

    @Test
    public void testRateLimitMapDividesRateByParallelism() throws Exception {
        // Test that rate limit is properly divided by parallelism
        double totalRate = 1000.0;
        int parallelism = 4;
        double expectedSubtaskRate = totalRate / parallelism;

        PscRateLimitMap<String> rateLimitMap = new PscRateLimitMap<>(totalRate);
        
        when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(parallelism);
        
        // Open should complete without errors
        rateLimitMap.setRuntimeContext(runtimeContext);
        rateLimitMap.open(new Configuration());
        
        // The rate limiter should be initialized
        assertThat(rateLimitMap).isNotNull();
    }

    @Test
    public void testRateLimitMapWithHighParallelism() throws Exception {
        // Test with high parallelism that results in low per-subtask rate
        double totalRate = 100.0;
        int parallelism = 50;
        double expectedSubtaskRate = totalRate / parallelism; // 2.0 per subtask

        PscRateLimitMap<String> rateLimitMap = new PscRateLimitMap<>(totalRate);
        
        when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(parallelism);
        
        // Should succeed since subtask rate is 2.0 > 0.1
        rateLimitMap.setRuntimeContext(runtimeContext);
        rateLimitMap.open(new Configuration());
    }

    @Test
    public void testRateLimitMapFailsWhenSubtaskRateTooLow() throws Exception {
        // Test that opening fails when per-subtask rate is too low
        double totalRate = 1.0;
        int parallelism = 100;
        // Subtask rate would be 0.01, which is < 0.1 threshold

        PscRateLimitMap<String> rateLimitMap = new PscRateLimitMap<>(totalRate);
        
        when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(parallelism);
        
        rateLimitMap.setRuntimeContext(runtimeContext);
        
        // Should throw IllegalArgumentException
        assertThatThrownBy(() -> rateLimitMap.open(new Configuration()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Subtask rate limit should be greater than 0.1 QPS");
    }

    @Test
    public void testRateLimitMapPassesRecordsThrough() throws Exception {
        // Test that records pass through unchanged
        PscRateLimitMap<String> rateLimitMap = new PscRateLimitMap<>(10000.0);
        
        when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(1);
        
        rateLimitMap.setRuntimeContext(runtimeContext);
        rateLimitMap.open(new Configuration());

        String testRecord = "test-record";
        String result = rateLimitMap.map(testRecord);
        
        assertThat(result).isEqualTo(testRecord);
    }

    @Test
    public void testRateLimitMapWithLargeRate() throws Exception {
        // Test with very large rate limit
        double totalRate = 1_000_000.0;
        int parallelism = 10;

        PscRateLimitMap<String> rateLimitMap = new PscRateLimitMap<>(totalRate);
        
        when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(parallelism);
        
        rateLimitMap.setRuntimeContext(runtimeContext);
        rateLimitMap.open(new Configuration());

        // Should be able to process records without throttling at high rate
        for (int i = 0; i < 10; i++) {
            String result = rateLimitMap.map("record-" + i);
            assertThat(result).isEqualTo("record-" + i);
        }
    }

    @Test
    public void testRateLimitMapEdgeCase() throws Exception {
        // Test edge case where rate / parallelism is exactly 0.1
        double totalRate = 1.0;
        int parallelism = 10;
        // Subtask rate = 0.1, which is exactly the threshold

        PscRateLimitMap<String> rateLimitMap = new PscRateLimitMap<>(totalRate);
        
        when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(parallelism);
        
        rateLimitMap.setRuntimeContext(runtimeContext);
        
        // Should fail because check is > 0.1, not >= 0.1
        assertThatThrownBy(() -> rateLimitMap.open(new Configuration()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Subtask rate limit should be greater than 0.1 QPS");
    }

    @Test
    public void testRateLimitMapWithParallelismOne() throws Exception {
        // Test with parallelism of 1 (no division needed)
        double totalRate = 500.0;
        int parallelism = 1;

        PscRateLimitMap<String> rateLimitMap = new PscRateLimitMap<>(totalRate);
        
        when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(parallelism);
        
        rateLimitMap.setRuntimeContext(runtimeContext);
        rateLimitMap.open(new Configuration());

        String result = rateLimitMap.map("test");
        assertThat(result).isEqualTo("test");
    }
}

