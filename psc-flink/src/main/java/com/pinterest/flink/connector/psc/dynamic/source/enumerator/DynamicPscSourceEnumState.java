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

package com.pinterest.flink.connector.psc.dynamic.source.enumerator;

import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumState;
import org.apache.flink.annotation.Internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The enumerator state keeps track of the state of the sub enumerators assigned splits and
 * metadata.
 */
@Internal
public class DynamicPscSourceEnumState {
    private final Set<PscStream> pscStreams;
    private final Map<String, PscSourceEnumState> clusterEnumeratorStates;

    public DynamicPscSourceEnumState() {
        this.pscStreams = new HashSet<>();
        this.clusterEnumeratorStates = new HashMap<>();
    }

    public DynamicPscSourceEnumState(
            Set<PscStream> pscStreams,
            Map<String, PscSourceEnumState> clusterEnumeratorStates) {
        this.pscStreams = pscStreams;
        this.clusterEnumeratorStates = clusterEnumeratorStates;
    }

    public Set<PscStream> getPscStreams() {
        return pscStreams;
    }

    public Map<String, PscSourceEnumState> getClusterEnumeratorStates() {
        return clusterEnumeratorStates;
    }
}
