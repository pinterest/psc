package com.pinterest.psc.config;

import java.util.HashMap;
import java.util.Map;

public class PscConsumerToMemqConsumerConfigConverter
        extends PscConsumerToBackendConsumerConfigConverter {
    @Override
    protected Map<String, String> getConfigConverterMap() {
        return new HashMap<String, String>() {
            private static final long serialVersionUID = 1L;

            {
                put(PscConfiguration.POLL_MESSAGES_MAX, "notification.max.poll.records");
                put(PscConfiguration.OFFSET_AUTO_RESET, "notification.auto.offset.reset");
                put(PscConfiguration.METADATA_AGE_MAX_MS, "notification.metadata.max.age.ms");
                put(PscConfiguration.COMMIT_AUTO_ENABLED, "notification.enable.auto.commit");
            }
        };
    }
}
