package com.pinterest.psc.exception.startup;

public class TopicRnSyntaxException extends PscStartupException {
    private static final long serialVersionUID = 1L;

    public TopicRnSyntaxException(String input, String reason) {
        super("Topic RN '" + input + "' is invalid due to: " + reason);
    }

    public TopicRnSyntaxException(String message) {
        super(message);
    }

    public TopicRnSyntaxException(String message, Throwable cause) {
        super(message, cause);
    }

    public TopicRnSyntaxException(Throwable cause) {
        super(cause);
    }
}
