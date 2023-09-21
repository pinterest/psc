package com.pinterest.psc.exception.producer;

import com.pinterest.psc.exception.ClientException;

/**
 * Most errors that are thrown when processing {@link com.pinterest.psc.producer.PscProducer} APIs are wrapped in an
 * exception of type ProducerException.
 */
public class ProducerException extends ClientException {
    private static final long serialVersionUID = 1L;

    public ProducerException(String message) {
        super(message);
    }

    public ProducerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProducerException(Throwable cause) {
        super(cause);
    }
}
