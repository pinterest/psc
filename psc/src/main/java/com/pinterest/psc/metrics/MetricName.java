package com.pinterest.psc.metrics;

import com.pinterest.psc.common.PscCommon;

import java.util.Map;

public final class MetricName {
    private final String name;
    private final String group;
    private final String description;
    private Map<String, String> tags;
    private int hash = 0;

    public MetricName(String name, String group, String description, Map<String, String> tags) {
        this.name = PscCommon.verifyNotNull(name);
        this.group = PscCommon.verifyNotNull(group);
        this.description = PscCommon.verifyNotNull(description);
        this.tags = PscCommon.verifyNotNull(tags);
    }

    public String name() {
        return this.name;
    }

    public String group() {
        return this.group;
    }

    public Map<String, String> tags() {
        return this.tags;
    }

    public String description() {
        return this.description;
    }

    @Override
    public int hashCode() {
        if (hash != 0)
            return hash;
        final int prime = 31;
        int result = 1;
        result = prime * result + group.hashCode();
        result = prime * result + name.hashCode();
        result = prime * result + tags.hashCode();
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MetricName other = (MetricName) obj;
        return group.equals(other.group) && name.equals(other.name) && tags.equals(other.tags);
    }

    @Override
    public String toString() {
        return String.format(
                "MetricName [name=%s, group=%s, description=%s, tags=%s]",
                name, group, description, tags
        );
    }
}
