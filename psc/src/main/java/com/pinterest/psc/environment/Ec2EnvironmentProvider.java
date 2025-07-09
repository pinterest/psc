package com.pinterest.psc.environment;

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

    private static String instanceId;
    private static String instanceType;
    private static String ipAddress;
    private static String locality;
    private static String region;

    @Override
    public String getInstanceId() {
        return instanceId == null || instanceId.equals(Environment.INFO_NOT_AVAILABLE)
            ? instanceId = fetchEC2MetadataWithRetries(
            EC2MetadataUtils::getInstanceId, "instanceId") : instanceId;
    }

    @Override
    public String getInstanceType() {
        return instanceType == null || instanceType.equals(Environment.INFO_NOT_AVAILABLE)
            ? instanceType = fetchEC2MetadataWithRetries(
            EC2MetadataUtils::getInstanceType, "instanceType") : instanceType;
    }

    @Override
    public String getIpAddress() {
        return ipAddress == null || ipAddress.equals(Environment.INFO_NOT_AVAILABLE)
            ? ipAddress = fetchEC2MetadataWithRetries(
            EC2MetadataUtils::getPrivateIpAddress, "ipAddress") : ipAddress;
    }

    @Override
    public String getLocality() {
        return locality == null || locality.equals(Environment.INFO_NOT_AVAILABLE)
            ? locality = fetchEC2MetadataWithRetries(
            EC2MetadataUtils::getAvailabilityZone, "locality") : locality;
    }

    @Override
    public String getRegion() {
        return region == null ? region = fetchEC2MetadataWithRetries(
            EC2MetadataUtils::getEC2InstanceRegion, "region") : region;
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
    private String fetchEC2MetadataWithRetries(EC2MetadataFetcher ec2MetadataFetcher,
        String propertyName) {
        int attempts = 0;
        long backoff = 500;

        while (attempts < MAX_FETCH_RETRIES) {
            try {
                return ec2MetadataFetcher.fetch();
            } catch (SdkClientException e) {
                LOGGER.error("Failed to fetch {} from EC2 metadata with {} on attempt {}: {}",
                    propertyName,
                    attempts + 1, e.getMessage());
                attempts++;
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

    @FunctionalInterface
    private interface EC2MetadataFetcher {

        String fetch() throws SdkClientException;
    }
}
