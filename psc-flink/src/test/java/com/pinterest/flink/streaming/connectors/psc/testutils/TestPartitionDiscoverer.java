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

package com.pinterest.flink.streaming.connectors.psc.testutils;

import com.pinterest.flink.streaming.connectors.psc.internals.AbstractTopicUriPartitionDiscoverer;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUrisDescriptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Utility {@link AbstractTopicUriPartitionDiscoverer} for tests that allows
 * mocking the sequence of consecutive metadata fetch calls to Kafka.
 */
public class TestPartitionDiscoverer extends AbstractTopicUriPartitionDiscoverer {

    private final PscTopicUrisDescriptor topicsDescriptor;

    private final List<List<String>> mockGetAllTopicUrisReturnSequence;
    private final List<List<PscTopicUriPartition>> mockGetAllPartitionsForTopicUrisReturnSequence;

    private int getAllTopicsInvokeCount = 0;
    private int getAllPartitionsForTopicsInvokeCount = 0;

    public TestPartitionDiscoverer(
            PscTopicUrisDescriptor topicsDescriptor,
            int indexOfThisSubtask,
            int numParallelSubtasks,
            List<List<String>> mockGetAllTopicUrisReturnSequence,
            List<List<PscTopicUriPartition>> mockGetAllPartitionsForTopicUrisReturnSequence) {

        super(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks);

        this.topicsDescriptor = topicsDescriptor;
        this.mockGetAllTopicUrisReturnSequence = mockGetAllTopicUrisReturnSequence;
        this.mockGetAllPartitionsForTopicUrisReturnSequence = mockGetAllPartitionsForTopicUrisReturnSequence;
    }

    @Override
    protected List<String> getAllTopicUris() {
        assertThat(topicsDescriptor.isTopicUriPattern()).isTrue();
        return mockGetAllTopicUrisReturnSequence.get(getAllTopicsInvokeCount++);
    }

    @Override
    protected List<PscTopicUriPartition> getAllPartitionsForTopicUris(List<String> topics) {
        if (topicsDescriptor.isFixedTopicUris()) {
            assertThat(topics).isEqualTo(topicsDescriptor.getFixedTopicUris());
        } else {
            assertThat(topics)
                    .isEqualTo(
                            mockGetAllTopicUrisReturnSequence.get(
                                    getAllPartitionsForTopicsInvokeCount - 1));
        }
        return mockGetAllPartitionsForTopicUrisReturnSequence.get(getAllPartitionsForTopicsInvokeCount++);
    }

    @Override
    protected void initializeConnections() {
        // nothing to do
    }

    @Override
    protected void wakeupConnections() {
        // nothing to do
    }

    @Override
    protected void closeConnections() {
        // nothing to do
    }

    // ---------------------------------------------------------------------------------
    //  Utilities to create mocked, fixed results for a sequences of metadata fetches
    // ---------------------------------------------------------------------------------

    public static List<List<String>> createMockGetAllTopicsSequenceFromFixedReturn(final List<String> fixed) {
        @SuppressWarnings("unchecked")
        List<List<String>> mockSequence = mock(List.class);
        when(mockSequence.get(anyInt())).thenAnswer(new Answer<List<String>>() {
            @Override
            public List<String> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return new ArrayList<>(fixed);
            }
        });

        return mockSequence;
    }

    public static List<List<PscTopicUriPartition>> createMockGetAllPartitionsFromTopicsSequenceFromFixedReturn(final List<PscTopicUriPartition> fixed) {
        @SuppressWarnings("unchecked")
        List<List<PscTopicUriPartition>> mockSequence = mock(List.class);
        when(mockSequence.get(anyInt())).thenAnswer(new Answer<List<PscTopicUriPartition>>() {
            @Override
            public List<PscTopicUriPartition> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return new ArrayList<>(fixed);
            }
        });

        return mockSequence;
    }
}
