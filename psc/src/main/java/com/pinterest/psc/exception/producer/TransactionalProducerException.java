package com.pinterest.psc.exception.producer;

public class TransactionalProducerException extends ProducerException {
    public TransactionalProducerException(String message) {
        super(message);
    }

    public TransactionalProducerException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransactionalProducerException(Throwable cause) {
        super(cause);
    }
}
