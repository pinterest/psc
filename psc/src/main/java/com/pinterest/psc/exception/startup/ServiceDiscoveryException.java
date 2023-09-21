package com.pinterest.psc.exception.startup;

public class ServiceDiscoveryException extends PscStartupException {
    private static final long serialVersionUID = 1L;

    public ServiceDiscoveryException(String message) {
        super(message);
    }

    public ServiceDiscoveryException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServiceDiscoveryException(Throwable cause) {
        super(cause);
    }
}
