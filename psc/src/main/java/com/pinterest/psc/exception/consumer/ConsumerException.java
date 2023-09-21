package com.pinterest.psc.exception.consumer;

import com.pinterest.psc.exception.ClientException;

/**
 * Most errors that are thrown when processing {@link com.pinterest.psc.consumer.PscConsumer} APIs are wrapped in an
 * exception of type ConsumerException.
 */
public class ConsumerException extends ClientException {
    private static final long serialVersionUID = 1L;

    public ConsumerException(String message) {
        super(message);
    }

    public ConsumerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConsumerException(Throwable cause) {
        super(cause);
    }
}
