package com.pinterest.psc.exception;

public class PscException extends Exception {

    private static final long serialVersionUID = 1L;

    public PscException(String message) {
        super(message);
    }

    public PscException(String message, Throwable cause) {
        super(message, cause);
    }

    public PscException(Throwable cause) {
        super(cause);
    }

    public String getMetricName() {
        return "error." + this.getClass().getName();
    }
}
