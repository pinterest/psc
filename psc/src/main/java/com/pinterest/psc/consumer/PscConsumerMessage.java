package com.pinterest.psc.consumer;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.PscMessageTags;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PscConsumerMessage<K, V> {
    private MessageId messageId;
    private K key;
    private V value;
    private long publishTimestamp;
    private Map<String, byte[]> headers;
    private Set<PscMessageTags> tags;

    public PscConsumerMessage() {
    }

    public PscConsumerMessage(
            MessageId messageId,
            K key,
            V value,
            long publishTimestamp) {
        this();
        this.messageId = messageId;
        this.key = key;
        this.value = value;
        this.publishTimestamp = publishTimestamp;
    }

    public PscConsumerMessage(PscConsumerMessage<K, V> other) {
        if (other == null)
            return;

        this.messageId = other.messageId;
        this.key = other.key;
        this.value = other.value;
        this.publishTimestamp = other.publishTimestamp;
        this.headers = other.headers == null ? null : new HashMap<>(other.headers);
        this.tags = other.tags == null ? null : new HashSet<>(other.tags);
    }

    /**
     * Returns the message id associated with this message. Message id includes information such as topic URI partition
     * hosting the message
     * @return a message id object
     */
    public MessageId getMessageId() {
        return messageId;
    }

    /**
     * sets the message id associated with this message.
     * @param messageId the message id to assign
     * @return the updated consumer message object
     */
    public PscConsumerMessage<K, V> setMessageId(MessageId messageId) {
        this.messageId = messageId;
        return this;
    }

    /**
     * returns the message key.
     * @return message key
     */
    public K getKey() {
        return key;
    }

    /**
     * sets the message key.
     * @param key the key object to assign to this message
     * @return the updated consumer message object
     */
    public PscConsumerMessage<K, V> setKey(K key) {
        this.key = key;
        return this;
    }

    /**
     * returns the message value.
     * @return message value
     */
    public V getValue() {
        return value;
    }

    /**
     * sets the message value.
     * @param value the value object to assign to this message
     * @return the updated consumer message object
     */
    public PscConsumerMessage<K, V> setValue(V value) {
        this.value = value;
        return this;
    }

    /**
     * returns the publish timestamp of this message; i.e. the time it was sent to the pub/sub.
     * @return message timestamp
     */
    public long getPublishTimestamp() {
        return this.publishTimestamp;
    }

    /**
     * sets the publish timestamp of message.
     * @param publishTimestamp the publish timestamp to assign to this message
     * @return the updated consumer message object
     */
    public PscConsumerMessage<K, V> setPublishTimestamp(long publishTimestamp) {
        this.publishTimestamp = publishTimestamp;
        return this;
    }

    /**
     * returns the size of headers of the message.
     * @return headers size
     */
    public int getHeadersSize() {
        return (this.headers == null) ? 0 : this.headers.size();
    }

    /**
     * sets a header key value for this message.
     * @param key the header key to set
     * @param val the header value to set
     * @return the updated consumer message object
     */
    public PscConsumerMessage<K, V> setHeader(String key, byte[] val) {
        if (this.headers == null)
            this.headers = new HashMap<>();
        this.headers.put(key, val);
        return this;
    }

    /**
     * returns the value of a given header key
     * @param key the header key to retrieve
     * @return the header value associated with the given key
     */
    public byte[] getHeader(String key) {
        return this.headers.get(key);
    }

    /**
     * retrieved all headers associated with this message in a map
     * @return a map of this message's header key-value pairs
     */
    public Map<String, byte[]> getHeaders() {
        return this.headers;
    }

    /**
     * sets headers of this message using a key-value pair map. Any previously set header
     * will be overwritten.
     * @param headers a map of key-value pairs to assign to this message.
     * @return the updated consumer message object
     */
    public PscConsumerMessage<K, V> setHeaders(Map<String, byte[]> headers) {
        this.headers = headers;
        return this;
    }

    /**
     * adds given headers to the existing message headers
     * @param headers a map of key-value pairs to add to this message's header.
     * @return the updated consumer message object
     */
    public PscConsumerMessage<K, V> addHeaders(Map<String, byte[]> headers) {
        if (this.headers == null)
            this.headers = new HashMap<>();
        this.headers.putAll(headers);
        return this;
    }

    /**
     * retrieved the set of tags associated with this message
     * @return set of message tags
     */
    public Set<PscMessageTags> getTags() {
        return this.tags;
    }

    /**
     * sets message tags using the given tag set. Any previously set tag will be overwritted.
     * @param tags a set of tags to assign to this message.
     * @return the updated consumer message object
     */
    public PscConsumerMessage<K, V> setTags(Set<PscMessageTags> tags) {
        this.tags = tags;
        return this;
    }

    /**
     * adds the given tag to the existing message tags
     * @param tag a tag to assign to this message
     * @return the updated consumer message object
     */
    public PscConsumerMessage<K, V> addTag(PscMessageTags tag) {
        if (this.tags == null)
            this.tags = new HashSet<>();
        tags.add(tag);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(java.lang.Object that) {
        if (that == null)
            return false;
        if (that instanceof PscConsumerMessage)
            return this.equals((PscConsumerMessage<K, V>) that);
        return false;
    }

    public boolean equals(PscConsumerMessage<K, V> that) {
        if (that == null)
            return false;
        if (this == that)
            return true;

        if (!PscCommon.equals(this.messageId, that.messageId))
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
        return String.format("[Key: %s, Value: %s, PublishTimestamp: %d]", this.key, this.value, this.publishTimestamp);
    }

    public enum DefaultPscConsumerMessageTags implements PscMessageTags {
        PAYLOAD_CORRUPTED,
        KEY_DESERIALIZATION_FAILED,
        VALUE_DESERIALIZATION_FAILED,
        HEADER_TIMESTAMP_NOT_FOUND,
        HEADER_TIMESTAMP_CORRUPTED,
        HEADER_TIMESTAMP_RETRIEVAL_FAILED,
        KEY_SIZE_MISMATCH_WITH_KEY_SIZE_HEADER,
        VALUE_SIZE_MISMATCH_WITH_VALUE_SIZE_HEADER
    }
}

