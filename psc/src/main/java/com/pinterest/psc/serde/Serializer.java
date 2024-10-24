package com.pinterest.psc.serde;

import com.pinterest.psc.common.PscPlugin;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.producer.SerializerException;

import java.io.Closeable;
import java.util.Map;

public interface Serializer<T> extends PscPlugin, Closeable {
    default void configure(PscConfiguration pscConfiguration, boolean isKey) {
    }

    default void configure(Map<String, String> config, boolean isKey) {
        PscConfiguration pscConfiguration = new PscConfiguration();
        for (Map.Entry<String, String> entry: config.entrySet()) {
            pscConfiguration.setProperty(entry.getKey(), entry.getValue());
        }
        configure(pscConfiguration, isKey);
    }

    default void close() {
    }

    byte[] serialize(T data) throws SerializerException;

    default byte[] serialize(String topicUri, T data) throws SerializerException {
        return serialize(data);
    }
}
