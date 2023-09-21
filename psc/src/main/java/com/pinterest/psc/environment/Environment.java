package com.pinterest.psc.environment;

import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.PscUtils;

/**
 * This indicates what environment PSC is running in.
 * <p>
 * PSC Environment indicator can subsequently be used by any component that
 * needs to switch functionality based on the environment it is running in.
 * <p>
 * NOTE: all variable MUST have default initialized in case the loader doesn't
 * work, all getters must return a NON-NULL value unless NULLs are expected.
 */
public class Environment {

    public static final String INFO_NOT_AVAILABLE = "n/a";
    public static final String DEFAULT_HOSTNAME = PscCommon.getHostname();

    private String hostname = INFO_NOT_AVAILABLE;
    private String ipAddress = INFO_NOT_AVAILABLE;
    private String instanceId = INFO_NOT_AVAILABLE;
    private String instanceType = INFO_NOT_AVAILABLE;
    private String locality = INFO_NOT_AVAILABLE;
    private String region = INFO_NOT_AVAILABLE;
    private String deploymentStage = INFO_NOT_AVAILABLE;
    private String projectUri = INFO_NOT_AVAILABLE;
    private String project = INFO_NOT_AVAILABLE;

    /**
     * @return the locality
     */
    public String getLocality() {
        return locality;
    }

    /**
     * @param locality the locality to set
     */
    public void setLocality(String locality) {
        this.locality = locality;
    }

    /**
     * @return the deploymentStage
     */
    public String getDeploymentStage() {
        return deploymentStage;
    }

    /**
     * @param deploymentStage the deploymentStage to set
     */
    public void setDeploymentStage(String deploymentStage) {
        this.deploymentStage = deploymentStage;
    }

    /**
     * @return the hostname
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * @param hostname the hostname to set
     */
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public void setInstanceType(String instanceType) {
        this.instanceType = instanceType;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getProjectUri() {
        return projectUri;
    }

    public void setProjectUri(String projectUri) {
        this.projectUri = projectUri;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return String.format(
                "Environment [hostname=%s, ip_address=%s, instance_id=%s, instance_type=%s, locality=%s, deployment_stage=%s",
                hostname, ipAddress, instanceId, instanceType, locality, deploymentStage
        );
    }
}