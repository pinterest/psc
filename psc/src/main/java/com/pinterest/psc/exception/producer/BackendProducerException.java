package com.pinterest.psc.exception.producer;

import com.pinterest.psc.exception.BackendException;

public class BackendProducerException extends ProducerException implements BackendException {

    private static final long serialVersionUID = 1L;
    private static final String ERROR_METRIC_PREFIX = "error.producer.";

    private final String backend;

    public BackendProducerException(String message, Throwable cause, String backend) {
        super(message, cause);
        this.backend = backend;
    }

    public BackendProducerException(Throwable cause, String backend) {
        super(cause);
        this.backend = backend;
    }

    public BackendProducerException(String message, String backend) {
        super(message);
        this.backend = backend;
    }

    @Override
    public String getBackend() {
        return backend;
    }

    @Override
    public String getMetricName() {
        return ERROR_METRIC_PREFIX + this.backend + "." +
                (this.getCause() == null ? this.getClass().getName() : this.getCause().getClass().getName());
    }
}
