package com.pinterest.psc.environment;

import com.pinterest.psc.common.PscCommon;

public class LocalEnvironmentProvider extends EnvironmentProvider {
    public static final String LOCAL_ENV = "local-dev";

    @Override
    public String getInstanceId() {
        return LOCAL_ENV;
    }

    @Override
    public String getInstanceType() {
        return LOCAL_ENV;
    }

    @Override
    public String getIpAddress() {
        return PscCommon.getHostIp();
    }

    @Override
    public String getLocality() {
        return LOCAL_ENV;
    }

    @Override
    public String getRegion() {
        return LOCAL_ENV;
    }

    @Override
    public String getDeploymentStage() {
        return LOCAL_ENV;
    }

    @Override
    public String getProjectUri() {
        return LOCAL_ENV;
    }

    @Override
    public String getProject() {
        return LOCAL_ENV;
    }
}
