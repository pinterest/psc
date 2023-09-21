package com.pinterest.psc.common;

import com.pinterest.psc.exception.startup.TopicUriSyntaxException;

import java.util.Map;

/*
  topic uri = <protocol>:<separator><topic rn>
 */

/**
 * Topic URI or unified resource identifier is used to specify how a PubSub topic can be accessed. It relies on TopicRn
 * and adds a {@code <protocol>:/</topicRN>} prefix to identify the protocol that should be used to access the topic:
 * <pre>{@code
 * <protocol>:/<topicRN>
 * }</pre>
 * An example would be <code>plaintext:/rn:kafka:env:aws_us-west-1::kafkacluster01:topic01</code>
 */
public interface TopicUri {
    String DEFAULT_PROTOCOL = "plaintext";
    String STANDARD = TopicRn.STANDARD;
    String SEPARATOR = "/";

    String getBackend();

    /**
     * @deprecated Use <code>getProtocol()</code> instead.
     * @return the transmission Protocol associated with this topic URI
     */
    @Deprecated
    default String getVariant() {
        return getProtocol();
    }

    String getProtocol();

    /**
     * @deprecated Use <code>getTopicRn()</code> instead.
     * @return the topic resource name associated with this topic URI
     */
    String getPath();

    String getTopicUriAsString();

    String getCloud();

    /**
     * @deprecated Use <code>getRegion()</code> instead.
     * @return the cloud region associated with this topic URI
     */
    default String getLocality() {
        return getRegion();
    }

    String getRegion();

    String getCluster();

    String getTopic();

    TopicRn getTopicRn();

    String getTopicUriPrefix();

    default byte getVersion() {
        return getTopicRn().getVersion();
    }

    static TopicUri deserialize(byte[] bytes) throws TopicUriSyntaxException {
        return BaseTopicUri.validate(bytes, null);
    }

    static TopicUri deserialize(byte[] bytes, String protocol) throws TopicUriSyntaxException {
        return BaseTopicUri.validate(bytes, protocol);
    }

    byte[] serialize();

    static TopicUri validate(String topicUriStr) throws TopicUriSyntaxException {
        return BaseTopicUri.validate(topicUriStr);
    }

    Map<String, String> getComponents();
}
