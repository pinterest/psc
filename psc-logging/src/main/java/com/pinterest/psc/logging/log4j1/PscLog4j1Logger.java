package com.pinterest.psc.logging.log4j1;

import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.logging.PscLogger;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import java.text.MessageFormat;
import java.util.regex.Pattern;

public class PscLog4j1Logger implements PscLogger {
    private Logger logger;
    private static String classFqdn = PscLog4j1Logger.class.getCanonicalName();

    public static PscLog4j1Logger getLogger() {
        return new PscLog4j1Logger();
    }

    public static PscLog4j1Logger getLogger(String name) {
        return new PscLog4j1Logger(name);
    }

    public static PscLog4j1Logger getLogger(Class clazz) {
        return new PscLog4j1Logger(clazz);
    }

    private PscLog4j1Logger() {
        //TODO: implement caller class lookup
        logger = Logger.getLogger(PscLog4j1Logger.class);
    }

    private PscLog4j1Logger(String name) {
        logger = Logger.getLogger(name);
    }

    private PscLog4j1Logger(Class clazz) {
        logger = Logger.getLogger(clazz);
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
        log(Level.ALL, message, t);
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
        log(Level.TRACE, message, t);
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
        log(Level.DEBUG, message, t);
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
        log(Level.INFO, message, t);
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
        log(Level.WARN, message, t);
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
        log(Level.ERROR, message, t);
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
        log(Level.FATAL, message, t);
    }

    private void log(Level level, String message, Throwable t) {
        verifyLogger();
        //Granular thread tracking as MDC context is inherited - skipping this to avoid performance penalty
        //addContext("pscTid", String.valueOf(Thread.currentThread().getId()));
        logger.log(classFqdn, level, message, t);
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
