package com.pinterest.psc.exception.consumer;

/**
 * This exception is thrown if the consumed message key cannot be deserialized using the configured key deserializer.
 * This normally means a mismatch between the type of stored data and the expected type by
 * {@link com.pinterest.psc.consumer.PscConsumer}.
 */
public class KeyDeserializerException extends DeserializerException {
    private static final long serialVersionUID = 1L;

    public KeyDeserializerException(Throwable cause) {
        super(cause);
    }
}
