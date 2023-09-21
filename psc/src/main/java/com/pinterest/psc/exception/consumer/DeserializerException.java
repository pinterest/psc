package com.pinterest.psc.exception.consumer;

/**
 * This exception is thrown if the consumed message key or value cannot be deserialized using the configured key or
 * value deserializer. This normally means a mismatch between the type of stored data and the expected type by
 * {@link com.pinterest.psc.consumer.PscConsumer}.
 */
public class DeserializerException extends ConsumerException {
    private static final long serialVersionUID = 1L;

    public DeserializerException(String message) {
        super(message);
    }

    public DeserializerException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeserializerException(Throwable cause) {
        super(cause);
    }
}
