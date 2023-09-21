package com.pinterest.psc.discovery;

import com.pinterest.psc.common.PscPlugin;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.environment.Environment;

public interface ServiceDiscoveryProvider extends PscPlugin {
    ServiceDiscoveryConfig getConfig(Environment env, TopicUri topicUri);
}
