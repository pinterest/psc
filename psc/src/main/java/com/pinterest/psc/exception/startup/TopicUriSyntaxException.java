package com.pinterest.psc.exception.startup;

public class TopicUriSyntaxException extends PscStartupException {
    private static final long serialVersionUID = 1L;

    public TopicUriSyntaxException(String input, String reason) {
        super("Topic URI '" + input + "' is invalid due to: " + reason);
    }

    public TopicUriSyntaxException(String input, String reason, Throwable cause) {
        super("Topic URI '" + input + "' is invalid due to: " + reason, cause);
    }

    public TopicUriSyntaxException(String message) {
        super(message);
    }

    public TopicUriSyntaxException(String message, Throwable cause) {
        super(message, cause);
    }

    public TopicUriSyntaxException(Throwable cause) {
        super(cause);
    }
}
