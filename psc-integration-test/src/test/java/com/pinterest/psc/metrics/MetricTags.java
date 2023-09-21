package com.pinterest.psc.metrics;

import java.util.Map;

public class MetricTags {
    private final String hostname;
    private final String hostIp;
    private final String locality;
    private final String instanceType;
    private final String processId;
    private final long threadId;
    private final String uri;
    private final String project;
    private final String version;
    private final Map<String, String> others;

    public MetricTags(String hostname, String hostIp, String locality, String instanceType, String processId, long threadId, String uri, String project, String version, Map<String, String> others) {
        this.hostname = hostname;
        this.hostIp = hostIp;
        this.locality = locality;
        this.instanceType = instanceType;
        this.processId = processId;
        this.threadId = threadId;
        this.uri = uri;
        this.project = project;
        this.version = version;
        this.others = others;
    }

    public String getHostname() {
        return hostname;
    }

    public String getHostIp() {
        return hostIp;
    }

    public String getLocality() {
        return locality;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public String getProcessId() {
        return processId;
    }

    public long getThreadId() {
        return threadId;
    }

    public String getUri() {
        return uri;
    }

    public String getProject() {
        return project;
    }

    public String getVersion() {
        return version;
    }

    public Map<String, String> getOthers() {
        return others;
    }

    @Override
    public String toString() {
        return String.format("Proj-%s, Th-%d", project, threadId);
    }
}
