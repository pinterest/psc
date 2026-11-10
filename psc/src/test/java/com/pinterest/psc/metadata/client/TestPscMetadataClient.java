package com.pinterest.psc.metadata.client;

import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metadata.MetadataUtils;
import com.pinterest.psc.metadata.creation.PscBackendMetadataClientCreator;
import com.pinterest.psc.metadata.creation.PscMetadataClientCreatorManager;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

public class TestPscMetadataClient {

    protected static final String testKafkaTopic1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:aws_us-west-1::kafkacluster01:topic1";

    /**
     * Ensure that {@link TopicRn} creation is correct, and that equality is implemented correctly
     * @throws TopicUriSyntaxException
     */
    @Test
    void testCreateTopicRn() throws TopicUriSyntaxException {
        TopicRn topic1Rn = TopicUri.validate(testKafkaTopic1).getTopicRn();
        TopicRn topic1RnCreated = MetadataUtils.createTopicRn(TopicUri.validate(testKafkaTopic1), "topic1");
        assertTrue(topic1Rn.equals(topic1RnCreated));   // ensure that equality is implemented correctly

        TopicRn topic2Rn = MetadataUtils.createTopicRn(TopicUri.validate(testKafkaTopic1), "topic2");

        // Ensure that the new topic name is the only difference
        assertEquals(topic1Rn.getStandard(), topic2Rn.getStandard());
        assertEquals(topic1Rn.getService(), topic2Rn.getService());
        assertEquals(topic1Rn.getEnvironment(), topic2Rn.getEnvironment());
        assertEquals(topic1Rn.getCloud(), topic2Rn.getCloud());
        assertEquals(topic1Rn.getRegion(), topic2Rn.getRegion());
        assertEquals(topic1Rn.getClassifier(), topic2Rn.getClassifier());
        assertEquals(topic1Rn.getCluster(), topic2Rn.getCluster());
        assertEquals("topic2", topic2Rn.getTopic());
    }

    @Test
    void testGetBackendMetadataClientCachesByTopicUriPrefix() throws Exception {
        PscMetadataClient metadataClient = new PscMetadataClient(buildMinimalMetadataConfiguration());

        PscMetadataClientCreatorManager creatorManager = Mockito.mock(PscMetadataClientCreatorManager.class);
        PscBackendMetadataClientCreator backendCreator = Mockito.mock(PscBackendMetadataClientCreator.class);
        PscBackendMetadataClient backendClient =
                Mockito.mock(PscBackendMetadataClient.class, Mockito.withSettings().lenient());

        Map<String, List<PscBackendMetadataClientCreator>> creators =
                Collections.singletonMap("kafka", Collections.singletonList(backendCreator));
        Mockito.when(creatorManager.getBackendCreators()).thenReturn(creators);
        Mockito.when(backendCreator.validateBackendTopicUri(any())).thenAnswer(invocation -> invocation.getArgument(0));
        Mockito.when(backendCreator.create(any(), any(), any())).thenReturn(backendClient);

        Environment environment = new Environment();
        setField(metadataClient, "creatorManager", creatorManager);
        setField(metadataClient, "environment", environment);

        TopicUri topicUri = Mockito.mock(TopicUri.class);
        Mockito.when(topicUri.getBackend()).thenReturn("kafka");
        Mockito.when(topicUri.getTopicUriPrefix()).thenReturn("plaintext:/rn:kafka:env:region::cluster");

        PscBackendMetadataClient first = metadataClient.getBackendMetadataClient(topicUri);
        PscBackendMetadataClient second = metadataClient.getBackendMetadataClient(topicUri);

        assertSame(backendClient, first);
        assertSame(first, second, "Expected cached backend client for identical topic URI prefix");
        Mockito.verify(backendCreator, times(1)).create(any(), any(), any());
    }

    @Test
    void testGetBackendMetadataClientThrowsForUnknownBackend() throws Exception {
        PscMetadataClient metadataClient = new PscMetadataClient(buildMinimalMetadataConfiguration());

        PscMetadataClientCreatorManager creatorManager = Mockito.mock(PscMetadataClientCreatorManager.class);
        Mockito.when(creatorManager.getBackendCreators()).thenReturn(new HashMap<>());

        setField(metadataClient, "creatorManager", creatorManager);
        setField(metadataClient, "environment", new Environment());

        TopicUri topicUri = Mockito.mock(TopicUri.class);
        Mockito.when(topicUri.getBackend()).thenReturn("unknown-backend");

        assertThrows(IllegalArgumentException.class, () -> metadataClient.getBackendMetadataClient(topicUri));
    }

    private static PscConfiguration buildMinimalMetadataConfiguration() {
        PscConfiguration configuration = new PscConfiguration();
        configuration.setProperty(PscConfiguration.PSC_METADATA_CLIENT_ID, "test-metadata-client");
        return configuration;
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = PscMetadataClient.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
