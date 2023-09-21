package com.pinterest.psc.environment;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;

public class Ec2EnvironmentProvider extends EnvironmentProvider {
    private static final String PROJECT_URI = "PROJECT_URI";
    private static final String PROJECT = "PROJECT";
    private static final String DEPLOYMENT_STAGE = "DEPLOYMENT_STAGE";

    @Override
    public String getInstanceId() {
        try {
            return EC2MetadataUtils.getInstanceId();
        } catch (SdkClientException e) {
            return Environment.INFO_NOT_AVAILABLE;
        }
    }

    @Override
    public String getInstanceType() {
        try {
            return EC2MetadataUtils.getInstanceType();
        } catch (SdkClientException e) {
            return Environment.INFO_NOT_AVAILABLE;
        }
    }

    @Override
    public String getIpAddress() {
        try {
            return EC2MetadataUtils.getPrivateIpAddress();
        } catch (SdkClientException e) {
            return Environment.INFO_NOT_AVAILABLE;
        }
    }

    @Override
    public String getLocality() {
        try {
            return EC2MetadataUtils.getAvailabilityZone();
        } catch (SdkClientException e) {
            return Environment.INFO_NOT_AVAILABLE;
        }
    }

    @Override
    public String getRegion() {
        try {
            return EC2MetadataUtils.getEC2InstanceRegion();
        } catch (SdkClientException e) {
            return Environment.INFO_NOT_AVAILABLE;
        }
    }

    @Override
    public String getDeploymentStage() {
        return System.getenv(DEPLOYMENT_STAGE);
    }

    @Override
    public String getProjectUri() {
        return System.getenv(PROJECT_URI);
    }

    @Override
    public String getProject() {
        return System.getenv(PROJECT);
    }
}
