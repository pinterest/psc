package com.pinterest.psc.metrics;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class PscMetricTag {
    public final static String PSC_TAG_GROUP = "group";
    public final static String PSC_TAG_CLIENT_ID = "client_id";
    public final static String PSC_TAG_PARTITION = "partition";
    public final static String PSC_CLIENT_TYPE = "client_type";
    public final static Map<String, PscMetricTag> pscMetricTagsMap = new HashMap<>();

    private String serializedTag;

    public static void addPscMetricTag(PscMetricTag pscMetricTag) {
        pscMetricTagsMap.put(pscMetricTag.getId(), pscMetricTag);
    }

    public static PscMetricTag getPscMetricTag(String tagId) {
        return pscMetricTagsMap.get(tagId);
    }

    private String id;
    private String hostname;
    private String hostIp;
    private String locality;
    private String instanceType;
    private String processId;
    private long threadId = -1;
    private String project;
    private String version;
    private Map<String, String> additionalTags;

    public String getId() {
        return id;
    }

    protected void setId(String id) {
        this.id = id;
    }

    public String getHostname() {
        return hostname;
    }

    protected void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getHostIp() {
        return hostIp;
    }

    protected void setHostIp(String hostIp) {
        this.hostIp = hostIp;
    }

    public String getLocality() {
        return locality;
    }

    protected void setLocality(String locality) {
        this.locality = locality;
    }

    public String getInstanceType() {
        return instanceType;
    }

    protected void setInstanceType(String instanceType) {
        this.instanceType = instanceType;
    }

    public String getProcessId() {
        return processId;
    }

    protected void setProcessId(String processId) {
        this.processId = processId;
    }

    public long getThreadId() {
        return threadId;
    }

    protected void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public String getProject() {
        return project;
    }

    protected void setProject(String project) {
        this.project = project;
    }

    public String getVersion() {
        return version;
    }

    protected void setVersion(String version) {
        this.version = version;
    }

    public Map<String, String> getAdditionalTags() {
        return additionalTags;
    }

    protected void addAdditionalTag(String name, String value) {
        if (additionalTags == null)
            additionalTags = new HashMap<>();
        additionalTags.put(name, value);
    }

    protected void addAdditionalTags(Map<String, String> keyValues) {
        if (keyValues == null)
            return;

        if (additionalTags == null)
            additionalTags = new HashMap<>();

        additionalTags.putAll(keyValues);
    }

    public static class Builder {
        private final PscMetricTag pscMetricTag = new PscMetricTag();

        public Builder id(String id) {
            pscMetricTag.setId(id);
            return this;
        }

        public Builder hostname(String hostname) {
            pscMetricTag.setHostname(hostname);
            return this;
        }

        public Builder hostIp(String hostIp) {
            pscMetricTag.setHostIp(hostIp);
            return this;
        }

        public Builder locality(String locality) {
            pscMetricTag.setLocality(locality);
            return this;
        }

        public Builder instanceType(String instanceType) {
            pscMetricTag.setInstanceType(instanceType);
            return this;
        }

        public Builder processId(String processId) {
            pscMetricTag.setProcessId(processId);
            return this;
        }

        public Builder threadId(long threadId) {
            pscMetricTag.setThreadId(threadId);
            return this;
        }

        public Builder project(String project) {
            pscMetricTag.setProject(project);
            return this;
        }

        public Builder version(String version) {
            pscMetricTag.setVersion(version);
            return this;
        }

        public Builder tag(String name, String value) {
            pscMetricTag.addAdditionalTag(name, value);
            return this;
        }

        public Builder tags(Map<String, String> keyValues) {
            pscMetricTag.addAdditionalTags(keyValues);
            return this;
        }

        public PscMetricTag build() {
            return pscMetricTag;
        }

    }
    public String getSerializedString() {
        if (serializedTag == null) {
            synchronized (this) {
                if (serializedTag == null) {
                    StringBuilder sb = new StringBuilder();
                    if (id != null) {
                        sb.append("id=").append(id).append(" ");
                    }
                    if (hostname != null) {
                        sb.append("hostname=").append(hostname).append(" ");
                    }
                    if (hostIp != null) {
                        sb.append("hostIp=").append(hostIp).append(" ");
                    }
                    if (locality != null) {
                        sb.append("locality=").append(locality).append(" ");
                    }
                    if (instanceType != null) {
                        sb.append("instanceType=").append(instanceType).append(" ");
                    }
                    if (processId != null) {
                        sb.append("processId=").append(processId).append(" ");
                    }
                    if (threadId != -1) {
                        sb.append("threadId=").append(threadId).append(" ");
                    }
                    if (project != null) {
                        sb.append("project=").append(project).append(" ");
                    }
                    if (version != null) {
                        sb.append("version=").append(version).append(" ");
                    }
                    if (additionalTags != null && !additionalTags.isEmpty()) {
                        additionalTags.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEachOrdered((e) -> sb.append(e.getKey()).append("=").append(e.getValue()).append(" "));
                    }
                    // remove last space
                    serializedTag = sb.delete(sb.length() - 1, sb.length()).toString();
                }
                return serializedTag;
            }
        }
        return serializedTag;
    }
}
