package com.pinterest.psc.exception;

/**
 * This is a superclass for PSC consumer or producer exceptions.
 */
public class ClientException extends PscException {
    private static final long serialVersionUID = 1L;

    public ClientException(String message) {
        super(message);
    }

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClientException(Throwable cause) {
        super(cause);
    }
}
