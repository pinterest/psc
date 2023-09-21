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

package com.pinterest.flink.streaming.connectors.psc.internals;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A PSC Topic URIs Descriptor describes how the consumer subscribes to PSC topic URIs -
 * either a fixed list of topic URIs, or a topic URI pattern.
 */
@Internal
public class PscTopicUrisDescriptor implements Serializable {

    private static final long serialVersionUID = -3807227764764900975L;

    private final List<String> fixedTopicUris;
    private final Pattern topicUriPattern;

    public PscTopicUrisDescriptor(@Nullable List<String> fixedTopicUris, @Nullable Pattern topicUriPattern) {
        checkArgument((fixedTopicUris != null && topicUriPattern == null) || (fixedTopicUris == null && topicUriPattern != null),
                "Exactly one of fixedTopicUris or topicUriPattern must be specified.");

        if (fixedTopicUris != null) {
            checkArgument(!fixedTopicUris.isEmpty(), "If subscribing to a fixed topic URIs list, the supplied list cannot be empty.");
        }

        this.fixedTopicUris = fixedTopicUris;
        this.topicUriPattern = topicUriPattern;
    }

    public boolean isFixedTopicUris() {
        return fixedTopicUris != null;
    }

    public boolean isTopicUriPattern() {
        return topicUriPattern != null;
    }

    /**
     * Check if the input topic URI matches the topic URIs described by this PscTopicDescriptor.
     *
     * @return true if found a match.
     */
    public boolean isMatchingTopicUri(String topicUriStr) {
        TopicUri topicUri;
        try {
            topicUri = TopicUri.validate(topicUriStr);
        } catch (TopicUriSyntaxException e) {
            throw new RuntimeException(e);
        }

        if (isFixedTopicUris()) {
            return fixedTopicUris.contains(topicUriStr) || fixedTopicUris.contains(topicUri.getTopicRn().toString());
        } else {
            return topicUriPattern.matcher(topicUriStr).matches();
        }
    }

    public List<String> getFixedTopicUris() {
        return fixedTopicUris;
    }

    @Override
    public String toString() {
        return (fixedTopicUris == null)
                ? "Topic URI Regex Pattern (" + topicUriPattern.pattern() + ")"
                : "Fixed Topic URIs (" + fixedTopicUris + ")";
    }
}
