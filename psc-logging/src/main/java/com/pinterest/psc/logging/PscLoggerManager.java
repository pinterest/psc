package com.pinterest.psc.logging;

public class PscLoggerManager {
    protected static PscLoggerType pscLoggerType = PscLoggerType.LOG4J;

    public static void setPscLoggerType(PscLoggerType pscLoggerType) {
        PscLoggerManager.pscLoggerType = pscLoggerType;
    }
}
