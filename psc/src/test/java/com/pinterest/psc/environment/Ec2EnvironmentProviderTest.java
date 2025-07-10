package com.pinterest.psc.environment;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Ec2EnvironmentProviderTest {

    @Test
    public void testCachedMetadata() {
        Ec2EnvironmentProvider ec2EnvironmentProvider = new Ec2EnvironmentProvider();
        Environment env = ec2EnvironmentProvider.getEnvironment();
        env.setInstanceId("i-test");
        env.setInstanceType("instancetype1");
        env.setIpAddress("0.0.0.0");
        env.setLocality("us-region-1a");
        env.setRegion("us-region-1");

        // These shouldn't invoke calls to EC2MetadataUtils now that the fields are set
        assertEquals("i-test", ec2EnvironmentProvider.getInstanceId());
        assertEquals("instancetype1", ec2EnvironmentProvider.getInstanceType());
        assertEquals("0.0.0.0", ec2EnvironmentProvider.getIpAddress());
        assertEquals("us-region-1a", ec2EnvironmentProvider.getLocality());
        assertEquals("us-region-1", ec2EnvironmentProvider.getRegion());
    }

    @Test
    public void testRetryLogic() {
        Ec2EnvironmentProvider ec2EnvironmentProvider = new Ec2EnvironmentProvider();
        assertEquals(Environment.INFO_NOT_AVAILABLE, ec2EnvironmentProvider.fetchEC2MetadataWithRetries(() -> {
            throw SdkClientException.create("Simulated failure");
        }, "testField"));
        assertEquals("us-region-1", ec2EnvironmentProvider.fetchEC2MetadataWithRetries(() -> "us-region-1", "region"));
    }
}
