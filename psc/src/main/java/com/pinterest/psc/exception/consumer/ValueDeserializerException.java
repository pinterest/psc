package com.pinterest.psc.exception.consumer;

/**
 * This exception is thrown if the consumed message value cannot be deserialized using the configured value
 * deserializer. This normally means a mismatch between the type of stored data and the expected type by
 * {@link com.pinterest.psc.consumer.PscConsumer}.
 */
public class ValueDeserializerException extends DeserializerException {
    private static final long serialVersionUID = 1L;

    public ValueDeserializerException(Throwable cause) {
        super(cause);
    }
}
