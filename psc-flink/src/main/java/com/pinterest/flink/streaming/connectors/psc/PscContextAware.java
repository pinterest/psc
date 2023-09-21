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

package com.pinterest.flink.streaming.connectors.psc;

import org.apache.flink.annotation.PublicEvolving;

/**
 * An interface for {@link PscSerializationSchema PscSerializationSchemas} that need information
 * about the context where the PSC Producer is running along with information about the available
 * partitions.
 *
 * <p>You only need to override the methods for the information that you need. However, {@link
 * #getTargetTopicUri(Object)} is required because it is used to determine the available partitions.
 */
@PublicEvolving
public interface PscContextAware<T> {


    /**
     * Sets the number of the parallel subtask that the PSC Producer is running on. The numbering
     * starts from 0 and goes up to parallelism-1 (parallelism as returned by {@link
     * #setNumParallelInstances(int)}).
     */
    default void setParallelInstanceId(int parallelInstanceId) {
    }

    /**
     * Sets the parallelism with which the parallel task of the PSC Producer runs.
     */
    default void setNumParallelInstances(int numParallelInstances) {
    }

    /**
     * Sets the available partitions for the topic URI returned from {@link #getTargetTopicUri(Object)}.
     */
    default void setPartitions(int[] partitions) {
    }

    /**
     * Returns the topic URI that the presented element should be sent to. This is not used for setting
     * the topic URI (this is done via the {@link com.pinterest.psc.producer.PscProducerMessage} that
     * is returned from {@link PscSerializationSchema#serialize(Object, Long)}, it is only used
     * for getting the available partitions that are presented to {@link #setPartitions(int[])}.
     */
    String getTargetTopicUri(T element);
}
