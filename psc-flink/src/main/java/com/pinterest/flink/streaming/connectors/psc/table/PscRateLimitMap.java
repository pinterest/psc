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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.util.Preconditions;

/**
 * RateLimitMap applies rate limiting to PSC table source records.
 *
 * <p>This map function uses Guava's RateLimiter to throttle record processing
 * to a specified rate. The total rate limit is divided evenly across all
 * parallel subtasks, ensuring the aggregate rate does not exceed the configured limit.
 *
 * <p>When the rate limit is exceeded, the function blocks until permits become available,
 * emitting metrics to track throttling behavior.
 *
 *
 *
 * @param <T> The type of records flowing through this map function
 */
public class PscRateLimitMap<T> extends RichMapFunction<T, T> {

    private static final long serialVersionUID = 1L;

    private final double rateLimitRecordsPerSecond;
    private transient Counter throttleOccurredCounter;
    private transient Gauge<Long> maxThrottleDelayGauge;
    private transient Gauge<Long> currentThrottleDelayGauge;
    private transient RateLimiter subtaskRateLimiter;
    private volatile long maxThrottleDelayMs = 0;
    private volatile long currentThrottleDelayMs = 0;

    /**
     * Creates a new rate limiting map function.
     *
     * @param rateLimitRecordsPerSecond 
     */
    public PscRateLimitMap(double rateLimitRecordsPerSecond) {
        this.rateLimitRecordsPerSecond = rateLimitRecordsPerSecond;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);

        // Register metrics
        throttleOccurredCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("throttleOccurred");

        maxThrottleDelayGauge = getRuntimeContext()
                .getMetricGroup()
                .gauge("maxThrottleDelayMs", () -> maxThrottleDelayMs);
        
        currentThrottleDelayGauge = getRuntimeContext()
                .getMetricGroup()
                .gauge("currentThrottleDelayMs", () -> currentThrottleDelayMs);

        // Calculate per-subtask rate limit
        int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        double subtaskRateLimit = rateLimitRecordsPerSecond / numberOfParallelSubtasks;

        Preconditions.checkArgument(
                subtaskRateLimit > 0.1,
                "Subtask rate limit should be greater than 0.1 QPS. " +
                        "Current rate: %s records/second divided by %s subtasks = %s records/second per subtask. " +
                        "Consider increasing the rate limit or decreasing parallelism.",
                rateLimitRecordsPerSecond,
                numberOfParallelSubtasks,
                subtaskRateLimit);

        subtaskRateLimiter = RateLimiter.create(subtaskRateLimit);
    }

    @Override
    public T map(T record) throws Exception {
        blockIfNecessary();
        return record;
    }

    /**
     * Blocks if necessary to enforce the rate limit.
     * Emits metrics when throttling occurs.
     */
    private void blockIfNecessary() {
        // tryAcquire returns false if there are no permits available at this time
        if (!subtaskRateLimiter.tryAcquire()) {
            // Record that throttling occurred
            throttleOccurredCounter.inc();
            long startTime = System.currentTimeMillis();

            // Block until a permit becomes available
            subtaskRateLimiter.acquire();

            long endTime = System.currentTimeMillis();
            long delayMs = endTime - startTime;
            
            // Update both current and max throttle delay
            currentThrottleDelayMs = delayMs;
            if (delayMs > maxThrottleDelayMs) {
                maxThrottleDelayMs = delayMs;
            }
        } else {
            // No throttling occurred, reset current delay
            currentThrottleDelayMs = 0;
        }
    }
}

