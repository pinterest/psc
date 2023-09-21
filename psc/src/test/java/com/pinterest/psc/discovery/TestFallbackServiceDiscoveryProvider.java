package com.pinterest.psc.discovery;

import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
public class TestFallbackServiceDiscoveryProvider {

    @Mock
    Environment environment;

    private static final String testUri1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:aws_us-west-1::kafkacluster01:";
    private static final String nonexistentUri = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:aws_us-west-1::kafkacluster03:";

    @Test
    void testGetConfigSimple() throws TopicUriSyntaxException, IOException {
        ServiceDiscoveryProvider sdp = new FallbackServiceDiscoveryProvider(
                DiscoveryUtil.createTempFallbackFile()
        );
        ServiceDiscoveryConfig sdc = sdp.getConfig(environment, TopicUri.validate(testUri1));
        assertEquals("kafkacluster01001:9092,kafkacluster01002:9092", sdc.getConnect());
        assertEquals("PLAINTEXT", sdc.getSecurityProtocol());
    }

    @Test
    void testGetConfigNonexistentUri() throws TopicUriSyntaxException, IOException {
        ServiceDiscoveryProvider sdp = new FallbackServiceDiscoveryProvider(
                DiscoveryUtil.createTempFallbackFile()
        );
        ServiceDiscoveryConfig sdc = sdp.getConfig(environment, TopicUri.validate(nonexistentUri));
        assertNull(sdc);
    }
}
