package com.pinterest.psc.discovery;

import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.SortedMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
public class TestServiceDiscoveryManager {

    @Mock
    Environment environment;

    private static final String testUriFallback = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region-1::cluster01:";
    private static final String testNonKafkaUriFallback = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":pubsubsys:env:cloud_region-1::cluster02:";

    @Test
    void testFallbackServiceDiscoveryPriorityOrdering()
            throws TopicUriSyntaxException, IOException, ConfigurationException {
        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.addProperty(PscConfigurationInternal.getPscDiscoveryServiceProviderFallbackConfigName(), DiscoveryUtil.createTempFallbackFile());

        ServiceDiscoveryConfig sdc = ServiceDiscoveryManager.getServiceDiscoveryConfig(environment, pscConfiguration, TopicUri.validate(testUriFallback));
        assertNotNull(sdc);
        assertEquals("cluster01001:9092,cluster01002:9092", sdc.getConnect());  // fallback
        assertEquals("PLAINTEXT", sdc.getSecurityProtocol());

        sdc = ServiceDiscoveryManager.getServiceDiscoveryConfig(environment, pscConfiguration, TopicUri.validate(testNonKafkaUriFallback));
        assertNotNull(sdc);
        assertEquals("cluster02001:9092,cluster02002:9092", sdc.getConnect());   // fallback
    }
}
