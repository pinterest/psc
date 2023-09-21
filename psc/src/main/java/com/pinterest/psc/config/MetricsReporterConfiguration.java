package com.pinterest.psc.config;

public class MetricsReporterConfiguration {
    private final boolean reportingEnabled;
    private final String reporterClass;
    private final int reporterParallelism;
    private final String host;
    private final int port;
    private final int frequencyMs;

    public MetricsReporterConfiguration(
            boolean reportingEnabled, String reporterClass, int reporterParallelism, String host, int port, int frequencyMs
    ) {
        this.reportingEnabled = reportingEnabled;
        this.reporterClass = reporterClass;
        this.reporterParallelism = reporterParallelism;
        this.host = host;
        this.port = port;
        this.frequencyMs = frequencyMs;
    }

    public boolean isReportingEnabled() {
        return reportingEnabled;
    }

    public String getReporterClass() {
        return reporterClass;
    }

    public int getReporterParallelism() {
        return reporterParallelism;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getFrequencyMs() {
        return frequencyMs;
    }

    @Override
    public String toString() {
        return String.format("reportingEnabled=%b, reporterClass=%s, reporterParallelism=%d, host=%s, port=%d, frequencyMs=%d",
                             reportingEnabled, reporterClass, reporterParallelism, host, port, frequencyMs);
    }
}
