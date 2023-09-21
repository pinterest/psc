package com.pinterest.psc.logging.slf4j;

import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.logging.PscLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.text.MessageFormat;
import java.util.regex.Pattern;

public class PscSlf4jLogger implements PscLogger {
    private Logger logger;

    public static PscSlf4jLogger getLogger() {
        return new PscSlf4jLogger();
    }

    public static PscSlf4jLogger getLogger(String name) {
        return new PscSlf4jLogger(name);
    }

    public static PscSlf4jLogger getLogger(Class clazz) {
        return new PscSlf4jLogger(clazz);
    }

    private PscSlf4jLogger() {
        //TODO: implement caller class lookup
        logger = LoggerFactory.getLogger(PscSlf4jLogger.class);
    }

    private PscSlf4jLogger(String name) {
        logger = LoggerFactory.getLogger(name);
    }

    private PscSlf4jLogger(Class clazz) {
        logger = LoggerFactory.getLogger(clazz);
    }

    public String reformatMessage(String message) {
        int i = 0;
        while (message.contains("{}"))
            message = message.replaceFirst(Pattern.quote("{}"), "{" + (i++) + "}");
        return message;
    }

    @Override
    public void all(String message) {
        all(message, (Throwable) null);
    }

    @Override
    public void all(String message, Object... params) {
        if (params[params.length - 1] instanceof Throwable)
            all(MessageFormat.format(reformatMessage(message), params), (Throwable) params[params.length - 1]);
        else
            all(MessageFormat.format(reformatMessage(message), params), (Throwable) null);
    }

    @Override
    public void all(String message, Throwable t) {
        verifyLogger();
        logger.trace(message, t);
    }


    @Override
    public void trace(String message) {
        trace(message, (Throwable) null);
    }

    @Override
    public void trace(String message, Object... params) {
        if (params[params.length - 1] instanceof Throwable)
            trace(MessageFormat.format(reformatMessage(message), params), (Throwable) params[params.length - 1]);
        else
            trace(MessageFormat.format(reformatMessage(message), params), (Throwable) null);
    }

    @Override
    public void trace(String message, Throwable t) {
        verifyLogger();
        logger.trace(message, t);
    }


    @Override
    public void debug(String message) {
        debug(message, (Throwable) null);
    }

    @Override
    public void debug(String message, Object... params) {
        if (params[params.length - 1] instanceof Throwable)
            debug(MessageFormat.format(reformatMessage(message), params), (Throwable) params[params.length - 1]);
        else
            debug(MessageFormat.format(reformatMessage(message), params), (Throwable) null);
    }

    @Override
    public void debug(String message, Throwable t) {
        verifyLogger();
        logger.debug(message, t);
    }


    @Override
    public void info(String message) {
        info(message, (Throwable) null);
    }

    @Override
    public void info(String message, Object... params) {
        if (params[params.length - 1] instanceof Throwable)
            info(MessageFormat.format(reformatMessage(message), params), (Throwable) params[params.length - 1]);
        else
            info(MessageFormat.format(reformatMessage(message), params), (Throwable) null);
    }

    @Override
    public void info(String message, Throwable t) {
        verifyLogger();
        logger.info(message, t);
    }


    @Override
    public void warn(String message) {
        warn(message, (Throwable) null);
    }

    @Override
    public void warn(String message, Object... params) {
        if (params[params.length - 1] instanceof Throwable)
            warn(MessageFormat.format(reformatMessage(message), params), (Throwable) params[params.length - 1]);
        else
            warn(MessageFormat.format(reformatMessage(message), params), (Throwable) null);
    }

    @Override
    public void warn(String message, Throwable t) {
        verifyLogger();
        logger.warn(message, t);
    }


    @Override
    public void error(String message) {
        error(message, (Throwable) null);
    }

    @Override
    public void error(String message, Object... params) {
        if (params[params.length - 1] instanceof Throwable)
            error(MessageFormat.format(reformatMessage(message), params), (Throwable) params[params.length - 1]);
        else
            error(MessageFormat.format(reformatMessage(message), params), (Throwable) null);
    }

    @Override
    public void error(String message, Throwable t) {
        verifyLogger();
        logger.error(message, t);
    }


    @Override
    public void fatal(String message) {
        fatal(message, (Throwable) null);
    }

    @Override
    public void fatal(String message, Object... params) {
        if (params[params.length - 1] instanceof Throwable)
            fatal(MessageFormat.format(reformatMessage(message), params), (Throwable) params[params.length - 1]);
        else
            fatal(MessageFormat.format(reformatMessage(message), params), (Throwable) null);
    }

    @Override
    public void fatal(String message, Throwable t) {
        verifyLogger();
        logger.error(message, t);
    }

    @Override
    public void addContext(String key, String value) {
        // check if log4j2 thread context is loaded
        try {
            Class<?> threadContextClass = Class.forName("org.apache.logging.log4j.ThreadContext");
            PscCommon.invoke(
                    threadContextClass,
                    "put",
                    new Class[]{String.class, String.class},
                    new String[]{key, value}
            );
        } catch (ClassNotFoundException e) {
            // use log4j1 mdc
            MDC.put(key, value);
        }
    }

    @Override
    public void clearContext() {
        // check if log4j2 thread context is loaded
        try {
            Class<?> threadContextClass = Class.forName("org.apache.logging.log4j.ThreadContext");
            PscCommon.invoke(threadContextClass, "clearAll");
        } catch (ClassNotFoundException e) {
            // use log4j1 mdc
            MDC.clear();
        }
    }

    private void verifyLogger() {
        if (logger == null)
            throw new IllegalStateException("PscLogger is not initialized");
    }
}
