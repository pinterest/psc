package com.pinterest.psc.exception.startup;

/**
 * This exception is thrown if there are errors with the provided configuration.
 */
public class ConfigurationException extends PscStartupException {
    private static final long serialVersionUID = 1L;

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigurationException(Throwable cause) {
        super(cause);
    }
}
