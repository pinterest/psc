package com.pinterest.psc.logging;

import com.pinterest.psc.logging.log4j1.PscLog4j1Logger;

public interface PscLogger {

    static PscLogger getLogger() {
        switch (PscLoggerManager.pscLoggerType) {
            case LOG4J:
                return PscLog4j1Logger.getLogger();
            case LOG4J2:
            case LOGBACK:
                throw new UnsupportedOperationException(
                        String.format("Logger '%s' is not supported", PscLoggerManager.pscLoggerType)
                );
            default:
                throw new UnsupportedOperationException("Logger is not set");
        }
    }

    static PscLogger getLogger(String name) {
        switch (PscLoggerManager.pscLoggerType) {
            case LOG4J:
                return PscLog4j1Logger.getLogger(name);
            case LOG4J2:
            case LOGBACK:
                throw new UnsupportedOperationException(
                        String.format("Logger '%s' is not supported", PscLoggerManager.pscLoggerType)
                );
            default:
                throw new UnsupportedOperationException("Logger is not set");
        }
    }

    static PscLogger getLogger(Class clazz) {
        switch (PscLoggerManager.pscLoggerType) {
            case LOG4J:
                return PscLog4j1Logger.getLogger(clazz);
            case LOG4J2:
            case LOGBACK:
                throw new UnsupportedOperationException(
                        String.format("Logger '%s' is not supported", PscLoggerManager.pscLoggerType)
                );
            default:
                throw new UnsupportedOperationException("Logger is not set");
        }
    }

    void all(String message);
    void all(String message, Object... params);
    void all(String message, Throwable t);

    void trace(String message);
    void trace(String message, Object... params);
    void trace(String message, Throwable t);

    void debug(String message);
    void debug(String message, Object... params);
    void debug(String message, Throwable t);

    void info(String message);
    void info(String message, Object... params);
    void info(String message, Throwable t);

    void warn(String message);
    void warn(String message, Object... params);
    void warn(String message, Throwable t);

    void error(String message);
    void error(String message, Object... params);
    void error(String message, Throwable t);

    void fatal(String message);
    void fatal(String message, Object... params);
    void fatal(String message, Throwable t);

    void addContext(String key, String value);
    void clearContext();
}
