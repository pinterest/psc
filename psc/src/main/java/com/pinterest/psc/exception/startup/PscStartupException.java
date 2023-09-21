package com.pinterest.psc.exception.startup;

import com.pinterest.psc.exception.PscException;

public class PscStartupException extends PscException {

    private static final long serialVersionUID = 1L;

    public PscStartupException(String message) {
        super(message);
    }

    public PscStartupException(String message, Throwable cause) {
        super(message, cause);
    }

    public PscStartupException(Throwable cause) {
        super(cause);
    }

}
