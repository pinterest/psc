package com.pinterest.psc.producer;

import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.PscMessageTags;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUriPartition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PscProducerMessage<K, V> {
    private String topicUriAsString;
    private int partition;
    private TopicUriPartition topicUriPartition;
    private K key;
    private V value;
    private long publishTimestamp;
    private Map<String, byte[]> headers;
    private Set<PscMessageTags> tags;

    public PscProducerMessage() {
    }

    public PscProducerMessage(
            String topicUri,
            Integer partition,
            K key,
            V value,
            Long publishTimestamp) {
        this();
        this.topicUriAsString = topicUri;
        this.partition = partition == null ? PscUtils.NO_PARTITION : partition;
        this.key = key;
        this.value = value;
        this.publishTimestamp = publishTimestamp == null ? System.currentTimeMillis() : publishTimestamp;
    }

    public PscProducerMessage(
            String topicUri,
            Integer partition,
            K key,
            V value) {
        this(topicUri, partition, key, value, System.currentTimeMillis());
    }

    public PscProducerMessage(
            String topicUri,
            K key,
            V value,
            Long publishTimestamp) {
        this(topicUri, PscUtils.NO_PARTITION, key, value, publishTimestamp);
    }

    public PscProducerMessage(
            String topicUri,
            K key,
            V value) {
        this(topicUri, key, value, System.currentTimeMillis());
    }

    public PscProducerMessage(
            String topicUri,
            V value) {
        this(topicUri, null, value);
    }

    public PscProducerMessage(PscProducerMessage<K, V> other) {
        if (other == null)
            return;

        this.topicUriAsString = other.topicUriAsString;
        this.partition = other.partition;
        this.key = other.key;
        this.value = other.value;
        this.publishTimestamp = other.publishTimestamp;
        this.headers = other.headers == null ? null : new HashMap<>(other.headers);
        this.tags = other.tags == null ? null : new HashSet<>(other.tags);
    }

    public PscProducerMessage(PscProducerMessage other, K key, V value) {
        if (other == null)
            return;

        this.topicUriAsString = other.topicUriAsString;
        this.topicUriPartition = other.topicUriPartition;
        this.partition = other.partition;
        this.key = key;
        this.value = value;
        this.publishTimestamp = other.publishTimestamp;
        this.headers = other.headers == null ? null : new HashMap<>(other.headers);
        this.tags = other.tags == null ? null : new HashSet<>(other.tags);
    }

    public String getTopicUriAsString() {
        return topicUriAsString;
    }

    public int getPartition() {
        return partition;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public long getPublishTimestamp() {
        return this.publishTimestamp;
    }

    public int getHeadersSize() {
        return (this.headers == null) ? -1 : this.headers.size();
    }

    public PscProducerMessage<K, V> setHeader(String key, byte[] val) {
        if (this.headers == null)
            this.headers = new HashMap<>();
        this.headers.put(key, val);
        return this;
    }

    public byte[] getHeader(String key) {
        return this.headers.get(key);
    }

    public Map<String, byte[]> getHeaders() {
        return this.headers;
    }

    public PscProducerMessage<K, V> setHeaders(Map<String, byte[]> headers) {
        this.headers = headers;
        return this;
    }

    public PscProducerMessage<K, V> addHeaders(Map<String, byte[]> headers) {
        if (this.headers == null)
            this.headers = new HashMap<>();
        this.headers.putAll(headers);
        return this;
    }

    public Set<PscMessageTags> getTags() {
        return this.tags;
    }

    public PscProducerMessage<K, V> setTags(Set<PscMessageTags> tags) {
        this.tags = tags;
        return this;
    }

    public void addTag(PscMessageTags tag) {
        if (this.tags == null)
            this.tags = new HashSet<>();
        tags.add(tag);
    }

    protected PscProducerMessage<K, V> setTopicUriPartition(TopicUriPartition topicUriPartition) {
        this.topicUriPartition = topicUriPartition;
        return this;
    }

    public TopicUriPartition getTopicUriPartition() {
        return topicUriPartition;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object that) {
        if (that == null)
            return false;
        if (that instanceof PscProducerMessage)
            return this.equals((PscProducerMessage<K, V>) that);
        return false;
    }

    public boolean equals(PscProducerMessage<K, V> that) {
        if (that == null)
            return false;
        if (this == that)
            return true;

        if (!PscCommon.equals(this.topicUriAsString, that.topicUriAsString))
            return false;
        if (this.partition != that.partition)
            return false;
        if (!PscCommon.equals(this.key, that.key))
            return false;
        if (!PscCommon.equals(this.value, that.value))
            return false;
        if (this.publishTimestamp != that.publishTimestamp)
            return false;
        //if (!equals(this.headers, that.headers))
        //    return false;
        return PscCommon.equals(this.tags, that.tags);
    }

    @Override
    public String toString() {
        return toString(true);
    }

    public String toString(boolean includeKeyValue) {
        String keyVal = includeKeyValue ? String.format("Key: %s, Value: %s", this.key, this.value) : "";
        return partition == PscUtils.NO_PARTITION ?
                String.format("[Topic: %s, %s, PublishTimestamp: %d]",
                        this.topicUriAsString, keyVal, this.publishTimestamp) :
                String.format("[Topic: %s, Partition: %d, %s, PublishTimestamp: %d]",
                        this.topicUriAsString, this.partition, keyVal, this.publishTimestamp);
    }

    public enum DefaultPscProducerMessageTags implements PscMessageTags {
        PAYLOAD_CORRUPTED,
        KEY_SERIALIZATION_FAILED,
        VALUE_SERIALIZATION_FAILED,
        HEADER_TIMESTAMP_NOT_FOUND,
        HEADER_TIMESTAMP_CORRUPTED,
        HEADER_TIMESTAMP_RETRIEVAL_FAILED
    }
}

