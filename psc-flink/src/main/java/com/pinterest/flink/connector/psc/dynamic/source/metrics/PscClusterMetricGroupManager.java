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

package com.pinterest.flink.connector.psc.dynamic.source.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** Manages metric groups for each cluster. */
@Internal
public class PscClusterMetricGroupManager implements AutoCloseable {
    private static final Logger logger =
            LoggerFactory.getLogger(PscClusterMetricGroupManager.class);
    private final Map<String, AbstractMetricGroup> metricGroups;

    public PscClusterMetricGroupManager() {
        this.metricGroups = new HashMap<>();
    }

    public void register(String clusterId, PscClusterMetricGroup clusterMetricGroup) {
        if (clusterMetricGroup.getInternalClusterSpecificMetricGroup()
                instanceof AbstractMetricGroup) {
            metricGroups.put(
                    clusterId,
                    (AbstractMetricGroup)
                            clusterMetricGroup.getInternalClusterSpecificMetricGroup());
        } else {
            logger.warn(
                    "MetricGroup {} is an instance of {}, which is not supported. Please use an implementation of AbstractMetricGroup.",
                    clusterMetricGroup.getInternalClusterSpecificMetricGroup(),
                    clusterMetricGroup
                            .getInternalClusterSpecificMetricGroup()
                            .getClass()
                            .getSimpleName());
        }
    }

    public void close(String clusterId) {
        AbstractMetricGroup metricGroup = metricGroups.remove(clusterId);
        if (metricGroup != null) {
            metricGroup.close();
        } else {
            logger.warn(
                    "Tried to close metric group for {} but it is not registered for lifecycle management",
                    clusterId);
        }
    }

    @Override
    public void close() throws Exception {
        for (AbstractMetricGroup metricGroup : metricGroups.values()) {
            metricGroup.close();
        }
    }
}
