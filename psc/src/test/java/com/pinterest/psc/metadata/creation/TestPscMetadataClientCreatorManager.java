package com.pinterest.psc.metadata.creation;

import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metadata.TopicUriMetadata;
import com.pinterest.psc.metadata.client.PscBackendMetadataClient;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestPscMetadataClientCreatorManager {

    private static final String TEST_BACKEND = "test-metadata-backend";

    @Test
    void testCreatorsWithSameBackendAreSortedByPriority() {
        PscMetadataClientCreatorManager manager = new PscMetadataClientCreatorManager();

        NavigableMap<String, List<PscBackendMetadataClientCreator>> backendCreators = manager.getBackendCreators();
        List<PscBackendMetadataClientCreator> testCreators = backendCreators.get(TEST_BACKEND);

        assertNotNull(testCreators, "Expected test backend creators to be registered");
        assertEquals(2, testCreators.size(), "Expected two creators registered for the test backend");
        assertEquals(PriorityOneTestMetadataClientCreator.class, testCreators.get(0).getClass());
        assertEquals(PriorityTenTestMetadataClientCreator.class, testCreators.get(1).getClass());
    }

    @Test
    void testBackendKeysAreSortedAlphabetically() {
        PscMetadataClientCreatorManager manager = new PscMetadataClientCreatorManager();

        NavigableMap<String, List<PscBackendMetadataClientCreator>> backendCreators = manager.getBackendCreators();
        List<String> keys = new ArrayList<>(backendCreators.keySet());
        List<String> sortedKeys = new ArrayList<>(keys);
        Collections.sort(sortedKeys);

        assertEquals(sortedKeys, keys, "Expected backend keys to be sorted alphabetically");
    }

    @PscMetadataClientCreatorPlugin(backend = TEST_BACKEND, priority = 10)
    public static class PriorityTenTestMetadataClientCreator extends PscBackendMetadataClientCreator {

        @Override
        public PscBackendMetadataClient create(Environment env, PscConfigurationInternal pscConfigurationInternal, TopicUri clusterUri)
                throws ConfigurationException {
            return new TestMetadataClient();
        }

        @Override
        public TopicUri validateBackendTopicUri(TopicUri topicUri) throws TopicUriSyntaxException {
            return topicUri;
        }
    }

    @PscMetadataClientCreatorPlugin(backend = TEST_BACKEND, priority = 1)
    public static class PriorityOneTestMetadataClientCreator extends PscBackendMetadataClientCreator {

        @Override
        public PscBackendMetadataClient create(Environment env, PscConfigurationInternal pscConfigurationInternal, TopicUri clusterUri)
                throws ConfigurationException {
            return new TestMetadataClient();
        }

        @Override
        public TopicUri validateBackendTopicUri(TopicUri topicUri) throws TopicUriSyntaxException {
            return topicUri;
        }
    }

    static class TestMetadataClient extends PscBackendMetadataClient {

        @Override
        public List<TopicRn> listTopicRns(Duration duration)
                throws ExecutionException, InterruptedException, TimeoutException {
            return Collections.emptyList();
        }

        @Override
        public Map<TopicUri, TopicUriMetadata> describeTopicUris(Collection<TopicUri> topicUris, Duration duration)
                throws ExecutionException, InterruptedException, TimeoutException {
            return Collections.emptyMap();
        }

        @Override
        public Map<TopicUriPartition, Long> listOffsets(
                Map<TopicUriPartition, PscMetadataClient.MetadataClientOption> topicRnsAndOptions, Duration duration)
                throws ExecutionException, InterruptedException, TimeoutException {
            return Collections.emptyMap();
        }

        @Override
        public Map<TopicUriPartition, Long> listOffsetsForTimestamps(
                Map<TopicUriPartition, Long> topicUriPartitionsAndTimes, Duration duration)
                throws ExecutionException, InterruptedException, TimeoutException {
            return Collections.emptyMap();
        }

        @Override
        public Map<TopicUriPartition, Long> listOffsetsForConsumerGroup(
                String consumerGroupId, Collection<TopicUriPartition> topicUriPartitions, Duration duration)
                throws ExecutionException, InterruptedException, TimeoutException {
            return Collections.emptyMap();
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }
}
