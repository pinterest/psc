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

package com.pinterest.flink.connector.psc.dynamic.source.enumerator.subscriber;

import com.pinterest.flink.connector.psc.dynamic.metadata.PscMetadataService;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.util.Set;

/**
 * The subscriber interacts with {@link PscMetadataService} to find which PSC streams the source
 * will subscribe to.
 */
@Experimental
public interface PscStreamSubscriber extends Serializable {

    /**
     * Get the subscribed {@link PscStream}s.
     *
     * @param pscMetadataService the {@link PscMetadataService}.
     * @return the subscribed {@link PscStream}s.
     */
    Set<PscStream> getSubscribedStreams(PscMetadataService pscMetadataService);
}
