package com.pinterest.psc.exception.consumer;

import com.pinterest.psc.exception.BackendException;

public class BackendConsumerException extends ConsumerException implements BackendException {

    private static final long serialVersionUID = 1L;
    private static final String ERROR_METRIC_PREFIX = "error.consumer.";

    private final String backend;

    public BackendConsumerException(String message, Throwable cause, String backend) {
        super(message, cause);
        this.backend = backend;
    }

    public BackendConsumerException(Throwable cause, String backend) {
        super(cause);
        this.backend = backend;

    }

    public BackendConsumerException(String message, String backend) {
        super(message);
        this.backend = backend;
    }

    @Override
    public String getBackend() {
        return this.backend;
    }

    @Override
    public String getMetricName() {
        return ERROR_METRIC_PREFIX + this.backend + "." +
                (this.getCause() == null ? this.getClass().getName() : this.getCause().getClass().getName());
    }
}
