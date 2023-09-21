package com.pinterest.psc.logging;

public class PscLoggerManager {
    protected static PscLoggerType pscLoggerType = PscLoggerType.SLF4J;

    public static void setPscLoggerType(PscLoggerType pscLoggerType) {
        PscLoggerManager.pscLoggerType = pscLoggerType;
    }
}
