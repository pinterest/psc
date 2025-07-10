package com.pinterest.psc.environment;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.logging.PscLogger;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;

public class Ec2EnvironmentProvider extends EnvironmentProvider {

    private static final PscLogger LOGGER = PscLogger.getLogger(
        Ec2EnvironmentProvider.class);
    private static final String PROJECT_URI = "PROJECT_URI";
    private static final String PROJECT = "PROJECT";
    private static final String DEPLOYMENT_STAGE = "DEPLOYMENT_STAGE";
    private static final int MAX_FETCH_RETRIES = 3;

    @Override
    public String getInstanceId() {
        if (environment.getInstanceId().equals(Environment.INFO_NOT_AVAILABLE)) {
            environment.setInstanceId(fetchEC2MetadataWithRetries(EC2MetadataUtils::getInstanceId, "instanceId"));
        }
        return environment.getInstanceId();
    }

    @Override
    public String getInstanceType() {
        if (environment.getInstanceType().equals(Environment.INFO_NOT_AVAILABLE)) {
            environment.setInstanceType(fetchEC2MetadataWithRetries(EC2MetadataUtils::getInstanceType, "instanceType"));
        }
        return environment.getInstanceType();
    }

    @Override
    public String getIpAddress() {
        if (environment.getIpAddress().equals(Environment.INFO_NOT_AVAILABLE)) {
            environment.setIpAddress(fetchEC2MetadataWithRetries(EC2MetadataUtils::getPrivateIpAddress, "ipAddress"));
        }
        return environment.getIpAddress();
    }

    @Override
    public String getLocality() {
        if (environment.getLocality().equals(Environment.INFO_NOT_AVAILABLE)) {
            environment.setLocality(fetchEC2MetadataWithRetries(EC2MetadataUtils::getAvailabilityZone, "locality"));
        }
        return environment.getLocality();
    }

    @Override
    public String getRegion() {
        if (environment.getRegion().equals(Environment.INFO_NOT_AVAILABLE)) {
            environment.setRegion(fetchEC2MetadataWithRetries(EC2MetadataUtils::getEC2InstanceRegion, "region"));
        }
        return environment.getRegion();
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

    /**
     * Fetches EC2 metadata with retries and exponential backoff.
     *
     * @param ec2MetadataFetcher the function to fetch EC2 metadata
     * @param propertyName       the name of the property being fetched, used for logging
     * @return the fetched metadata or a default value if it fails
     */
    @VisibleForTesting
    public String fetchEC2MetadataWithRetries(EC2MetadataFetcher ec2MetadataFetcher,
        String propertyName) {
        int attempts = 0;
        long backoff = 500;

        while (attempts < MAX_FETCH_RETRIES) {
            try {
                return ec2MetadataFetcher.fetch();
            } catch (SdkClientException e) {
                attempts++;
                LOGGER.error("Failed to fetch {} from EC2 metadata with on attempt {}: {}",
                    propertyName,
                    attempts, e);
                if (attempts >= MAX_FETCH_RETRIES) {
                    break;
                }
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    LOGGER.error(
                        "Interrupted while waiting to retry fetching {} from EC2 metadata: {}",
                        propertyName, ie.getMessage());
                    return Environment.INFO_NOT_AVAILABLE;
                }
                backoff *= 2;
            }
        }
        LOGGER.error(
            "Failed to fetch {} from EC2 metadata after {} attempts, returning default value.",
            propertyName, MAX_FETCH_RETRIES);
        return Environment.INFO_NOT_AVAILABLE;
    }

    /**
     * Functional interface for fetching EC2 metadata.
     * This allows for different metadata fetch methods to be passed in.
     */
    @FunctionalInterface
    public interface EC2MetadataFetcher {

        String fetch() throws SdkClientException;
    }
}
