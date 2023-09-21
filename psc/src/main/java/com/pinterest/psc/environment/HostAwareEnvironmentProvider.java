package com.pinterest.psc.environment;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.logging.PscLogger;

public class HostAwareEnvironmentProvider extends EnvironmentProvider {
    private static final PscLogger logger = PscLogger.getLogger(HostAwareEnvironmentProvider.class);

    private static class LazyHolder {
        private final static EnvironmentProvider delegate;

        static {
            if (PscUtils.isEc2Host()) {
                logger.info("EC2 host detected; using EC2 environment provider.");
                delegate = new Ec2EnvironmentProvider();
            } else {
                logger.info("No EC2 host detected; using local environment provider.");
                delegate = new LocalEnvironmentProvider();
            }
        }
    }

    private EnvironmentProvider getDelegate() {
        return LazyHolder.delegate;
    }

    @Override
    public String getIpAddress() {
        return getDelegate().getIpAddress();
    }

    @Override
    public String getInstanceId() {
        return getDelegate().getInstanceId();
    }

    @Override
    public String getInstanceType() {
        return getDelegate().getInstanceType();
    }

    @Override
    public String getLocality() {
        return getDelegate().getLocality();
    }

    @Override
    public String getRegion() {
        return getDelegate().getRegion();
    }

    @Override
    public String getDeploymentStage() {
        return getDelegate().getDeploymentStage();
    }

    @Override
    public String getProjectUri() {
        return getDelegate().getProjectUri();
    }

    @Override
    public String getProject() {
        return getDelegate().getProject();
    }
}
