package com.pinterest.psc.exception.producer;

/**
 * This exception is thrown if the message key or value that is being produced cannot be serialized using the configured
 * key or value serializer.
 */
public class SerializerException extends ProducerException {
    private static final long serialVersionUID = 1L;

    public SerializerException(String message) {
        super(message);
    }

    public SerializerException(String message, Throwable cause) {
        super(message, cause);
    }

    public SerializerException(Throwable cause) {
        super(cause);
    }
}
