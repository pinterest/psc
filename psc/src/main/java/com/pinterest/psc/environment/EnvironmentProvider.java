package com.pinterest.psc.environment;

import com.pinterest.psc.common.PscPlugin;

/**
 * Environment provider for PSC. This class can be
 * extended to change the behavior of environment provider.
 */
public abstract class EnvironmentProvider implements PscPlugin {

    protected Environment environment = new Environment();

    public EnvironmentProvider() {
        environment.setHostname(getHostname());
        environment.setIpAddress(getIpAddress());
        environment.setInstanceId(getInstanceId());
        environment.setInstanceType(getInstanceType());
        environment.setLocality(getLocality());
        environment.setRegion(getRegion());
        environment.setDeploymentStage(getDeploymentStage());
    }

    public String getHostname() {
        return Environment.DEFAULT_HOSTNAME;
    }

    public abstract String getIpAddress();

    public abstract String getInstanceId();

    public abstract String getInstanceType();

    public abstract String getLocality();

    public abstract String getRegion();

    public abstract String getDeploymentStage();

    public abstract String getProjectUri();

    public abstract String getProject();

    public Environment getEnvironment() {
        return environment;
    }
}