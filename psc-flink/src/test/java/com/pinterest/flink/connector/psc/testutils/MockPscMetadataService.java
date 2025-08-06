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

package com.pinterest.flink.connector.psc.testutils;

import com.google.common.collect.ImmutableMap;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscMetadataService;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** A mock in-memory implementation of {@link PscMetadataService}. */
public class MockPscMetadataService implements PscMetadataService {

    private Set<PscStream> pscStreams;
    private Set<String> kafkaClusterIds;
    private boolean throwException = false;

    public MockPscMetadataService(boolean throwException) {
        this.throwException = throwException;
    }

    public MockPscMetadataService(Set<PscStream> pscStreams) {
        setPscStreams(pscStreams);
    }

    public void setPscStreams(Set<PscStream> pscStreams) {
        this.pscStreams = pscStreams;
        this.kafkaClusterIds =
                pscStreams.stream()
                        .flatMap(
                                kafkaStream ->
                                        kafkaStream.getClusterMetadataMap().keySet().stream())
                        .collect(Collectors.toSet());
    }

    public void setThrowException(boolean throwException) {
        this.throwException = throwException;
    }

    private void checkAndThrowException() {
        if (throwException) {
            throw new RuntimeException("Mock exception");
        }
    }

    @Override
    public Set<PscStream> getAllStreams() {
        checkAndThrowException();
        return pscStreams;
    }

    @Override
    public Map<String, PscStream> describeStreams(Collection<String> streamIds) {
        checkAndThrowException();
        ImmutableMap.Builder<String, PscStream> builder = ImmutableMap.builder();
        for (PscStream stream : getAllStreams()) {
            if (streamIds.contains(stream.getStreamId())) {
                builder.put(stream.getStreamId(), stream);
            }
        }

        return builder.build();
    }

    @Override
    public boolean isClusterActive(String kafkaClusterId) {
        checkAndThrowException();
        return kafkaClusterIds.contains(kafkaClusterId);
    }

    @Override
    public void close() throws Exception {}
}
