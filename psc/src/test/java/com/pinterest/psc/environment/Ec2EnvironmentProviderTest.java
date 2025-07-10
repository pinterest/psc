package com.pinterest.psc.environment;

import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Ec2EnvironmentProviderTest {

    @Test
    public void testCachedMetadata() throws Exception {
        setStaticField(Ec2EnvironmentProvider.class, "instanceId", "i-test");
        setStaticField(Ec2EnvironmentProvider.class, "instanceType", "instancetype1");
        setStaticField(Ec2EnvironmentProvider.class, "ipAddress", "0.0.0.0");
        setStaticField(Ec2EnvironmentProvider.class, "locality", "us-region-1a");
        setStaticField(Ec2EnvironmentProvider.class, "region", "us-region-1");

        Ec2EnvironmentProvider ec2EnvironmentProvider = new Ec2EnvironmentProvider();
        // These shouldn't invoke calls to EC2MetadataUtils now that the fields are set
        assertEquals("i-test", ec2EnvironmentProvider.getInstanceId());
        assertEquals("instancetype1", ec2EnvironmentProvider.getInstanceType());
        assertEquals("0.0.0.0", ec2EnvironmentProvider.getIpAddress());
        assertEquals("us-region-1a", ec2EnvironmentProvider.getLocality());
        assertEquals("us-region-1", ec2EnvironmentProvider.getRegion());
    }

    @Test
    public void testRetryLogic() throws Exception {
        Ec2EnvironmentProvider ec2EnvironmentProvider = new Ec2EnvironmentProvider();
        assertEquals(Environment.INFO_NOT_AVAILABLE, ec2EnvironmentProvider.fetchEC2MetadataWithRetries(() -> {
            throw SdkClientException.create("test");
        }, "testField"));
        assertEquals("us-region-1", ec2EnvironmentProvider.fetchEC2MetadataWithRetries(() -> "us-region-1", "region"));
    }

    private void setStaticField(Class<?> clazz, String fieldName, Object value) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(null, value); // Set static field
    }
}
