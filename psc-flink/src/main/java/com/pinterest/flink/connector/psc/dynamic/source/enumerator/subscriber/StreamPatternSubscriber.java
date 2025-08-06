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

import com.google.common.collect.ImmutableSet;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscMetadataService;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import org.apache.flink.annotation.Internal;

import java.util.Set;
import java.util.regex.Pattern;

/** To subscribe to streams based on a pattern. */
@Internal
public class StreamPatternSubscriber implements PscStreamSubscriber {

    private final Pattern streamPattern;

    public StreamPatternSubscriber(Pattern streamPattern) {
        this.streamPattern = streamPattern;
    }

    @Override
    public Set<PscStream> getSubscribedStreams(PscMetadataService pscMetadataService) {
        Set<PscStream> allStreams = pscMetadataService.getAllStreams();
        ImmutableSet.Builder<PscStream> builder = ImmutableSet.builder();
        for (PscStream pscStream : allStreams) {
            String streamId = pscStream.getStreamId();
            if (streamPattern.matcher(streamId).find()) {
                builder.add(pscStream);
            }
        }

        return builder.build();
    }
}
