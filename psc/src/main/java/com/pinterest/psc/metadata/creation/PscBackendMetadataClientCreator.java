package com.pinterest.psc.metadata.creation;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metadata.client.PscBackendMetadataClient;

public abstract class PscBackendMetadataClientCreator {

    public abstract PscBackendMetadataClient create(Environment env, PscConfigurationInternal pscConfigurationInternal, TopicUri clusterUri) throws ConfigurationException;

}
