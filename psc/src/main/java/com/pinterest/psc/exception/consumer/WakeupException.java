package com.pinterest.psc.exception.consumer;

import com.pinterest.psc.consumer.PscConsumer;

/**
 * This exception is thrown as a result of calling {@link PscConsumer#wakeup()}, for example when the consumer is in
 * a long running {@link PscConsumer#poll()} call.
 */
public class WakeupException extends ConsumerException {
    private static final long serialVersionUID = 1L;

    public WakeupException() {
        super("");
    }

    public WakeupException(String message) {
        super(message);
    }

    public WakeupException(String message, Throwable cause) {
        super(message, cause);
    }

    public WakeupException(Throwable cause) {
        super(cause);
    }
}
