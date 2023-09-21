package com.pinterest.psc.exception.producer;

/**
 * This exception is thrown if the message key that is being produced cannot be serialized using the configured
 * key serializer.
 */
public class KeySerializerException extends SerializerException {
    private static final long serialVersionUID = 1L;

    public KeySerializerException(Throwable cause) {
        super(cause);
    }
}
