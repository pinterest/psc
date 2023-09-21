package com.pinterest.psc.exception.producer;

/**
 * This exception is thrown if the message value that is being produced cannot be serialized using the configured
 * value serializer.
 */
public class ValueSerializerException extends SerializerException {
    private static final long serialVersionUID = 1L;

    public ValueSerializerException(Throwable cause) {
        super(cause);
    }
}
