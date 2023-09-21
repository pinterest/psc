package com.pinterest.psc.serde;

import com.pinterest.psc.common.PscPlugin;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.producer.SerializerException;

import java.io.Closeable;

public interface Serializer<T> extends PscPlugin, Closeable {
    default void configure(PscConfiguration pscConfiguration, boolean isKey) {
    }

    default void close() {
    }

    byte[] serialize(T data) throws SerializerException;
}
