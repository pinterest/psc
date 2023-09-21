package com.pinterest.psc.serde;

import com.pinterest.psc.common.PscPlugin;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.consumer.DeserializerException;

import java.io.Closeable;

public interface Deserializer<T> extends PscPlugin, Closeable {
    default void configure(PscConfiguration pscConfiguration, boolean isKey) {
    }

    default void close() {
    }

    T deserialize(byte[] bytes) throws DeserializerException;
}
