package com.pinterest.psc.common;

import com.pinterest.psc.exception.startup.TopicRnSyntaxException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.logging.PscLogger;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// base topic URI only parses the URI down to topic RN; <protocol>:<separator>[topicRN]

public class BaseTopicUri implements TopicUri {
    private static final PscLogger logger = PscLogger.getLogger(BaseTopicUri.class);
    private static final String DEFAULT_PROTOCOL = "plaintext";
    private static final String[] SCHEMAS = {
            "<protocol>:<separator><topicRN>"
    };
    private static final Pattern[] REGEXES = {
            Pattern.compile(String.format(
                    "((?<protocol>[a-zA-Z0-9-_.]+):%s)?(?<topicRn>[a-zA-Z0-9-_.:]+)", SEPARATOR
            ))
    };

    private final String topicUriAsString;
    protected final String protocol;
    private final String topicUriPrefix;
    private final TopicRn topicRn;
    private final byte version;

    public BaseTopicUri(String protocol, TopicRn topicRn) {
        // add the protocol piece if missing (e.g. when deserializing)
        this.protocol = protocol == null ? DEFAULT_PROTOCOL : protocol;
        this.topicUriAsString = this.protocol + ":" + SEPARATOR + topicRn.toString();
        this.topicUriPrefix = this.protocol + ":" + SEPARATOR + topicRn.getTopicRnPrefixString();
        this.topicRn = topicRn;
        this.version = TopicRn.CURRENT_VERSION;
    }

    protected BaseTopicUri(TopicUri topicUri) {
        this(
                topicUri.getProtocol(),
                topicUri.getTopicRn()
        );
    }

    @Override
    public final String getTopicUriAsString() {
        return topicUriAsString;
    }

    @Deprecated
    @Override
    public final String getPath() {
        return SEPARATOR + getTopicRn();
    }

    @Override
    public TopicRn getTopicRn() {
        return topicRn;
    }

    @Override
    public String getTopic() {
        return topicRn.getTopic();
    }

    @Override
    public String getCloud() {
        return topicRn.getCloud();
    }

    @Override
    public String getRegion() {
        return topicRn.getRegion();
    }

    @Override
    public String getCluster() {
        return topicRn.getCluster();
    }

    @Override
    public Map<String, String> getComponents() {
        Map<String, String> components = new HashMap<>();
        if (protocol != null)
            components.put("protocol", protocol);

        if (topicRn != null)
            components.put(TopicRn.STANDARD, topicRn.toString());

        String backend = getBackend();
        if (backend != null)
            components.put("backend", backend);

        String region = getRegion();
        if (region != null)
            components.put("backend_region", region);

        String cluster = getCluster();
        if (cluster != null)
            components.put("backend_cluster", cluster);

        String topic = getTopic();
        if (topic != null)
            components.put("backend_topic", topic);

        return components;
    }

    @Override
    public String getBackend() {
        return topicRn.getService();
    }

    @Override
    public String getProtocol() {
        return protocol;
    }

    @Override
    public final String getTopicUriPrefix() {
        return topicUriPrefix;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (getClass() != other.getClass()) {
            if (!(other instanceof TopicUri))   // this allows for comparison with other implementations of TopicUri
                return false;
        }
        BaseTopicUri otherBaseTopicUri = (BaseTopicUri) other;
        return PscCommon.equals(topicUriAsString, otherBaseTopicUri.topicUriAsString) &&
                PscCommon.equals(protocol, otherBaseTopicUri.protocol) &&
                PscCommon.equals(topicUriPrefix, otherBaseTopicUri.topicUriPrefix) &&
                PscCommon.equals(topicRn, otherBaseTopicUri.topicRn);
    }

    @Override
    public String toString() {
        return topicUriAsString;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicUriAsString);
    }

    @Override
    public byte[] serialize() {
        // do not serialize the protocol piece
        return ByteArrayHandler.serialize(topicRn.toString(), version);
    }

    private static String upgradeTopicUriToCurrentVersion(String topicUriAsStr, byte serializedVersion) throws TopicUriSyntaxException {
        if (serializedVersion == TopicRn.CURRENT_VERSION)
            return topicUriAsStr;

        /*
        switch (serializedVersion) {
            case 0:
                // convert the topic uri of version 0 to the current version
                // 1. extract topinRn of version 0
                // 2. do TopicRn.upgradeTopicRnToCurrentVersion(topicRn, serializedVersion)
                // 3. rebuild and return the topic uri of current version
            case 1:
                // convert the topic uri of version 1 to the current version
                // 1. extract topinRn of version 1
                // 2. do TopicRn.upgradeTopicRnToCurrentVersion(topicRn, serializedVersion)
                // 3. rebuild and return the topic uri of current version
            default:
                throw new TopicUriSyntaxException(String.format("Unsupported topic URI version %d", serializedVersion));
        }
         */

        throw new TopicUriSyntaxException(String.format("Unsupported topic URI version %d", serializedVersion));
    }

    public static TopicUri validate(byte[] serializedTopicUri, String protocol) throws TopicUriSyntaxException {
        ByteArrayHandler byteArrayHandler = new ByteArrayHandler(serializedTopicUri);
        String topicUriAsStr = byteArrayHandler.readString();
        if (!byteArrayHandler.hasRemaining()) {
            throw new TopicUriSyntaxException(
                    String.format("The version is missing in serialized bytes of topic URI '%s'", topicUriAsStr)
            );
        }

        byte version = byteArrayHandler.readByte();
        topicUriAsStr = upgradeTopicUriToCurrentVersion(topicUriAsStr, version);
        return validate(topicUriAsStr, protocol);
    }

    public static TopicUri validate(String topicUriAsString) throws TopicUriSyntaxException {
        return validate(topicUriAsString, null);
    }

    public static TopicUri validate(String topicUriAsString, String defaultProtocol) throws TopicUriSyntaxException {
        Matcher matcher = REGEXES[TopicRn.CURRENT_VERSION].matcher(topicUriAsString);
        if (matcher.matches()) {
            String protocol = matcher.group("protocol");
            if (protocol == null)
                protocol = defaultProtocol;
            String topicRnStr = matcher.group("topicRn");
            TopicRn topicRn;
            try {
                topicRn = TopicRn.validate(topicRnStr);
            } catch (TopicRnSyntaxException exception) {
                throw new TopicUriSyntaxException(
                        topicUriAsString,
                        String.format("Failed to match expected schema '%s'", SCHEMAS[TopicRn.CURRENT_VERSION]),
                        exception
                );
            }

            return new BaseTopicUri(
                    protocol,
                    topicRn
            );
        } else {
            throw new TopicUriSyntaxException(
                    topicUriAsString, String.format("Failed to match expected schema '%s'", SCHEMAS[TopicRn.CURRENT_VERSION])
            );
        }
    }
    public static void finalizeTopicUriPartition(
            TopicUriPartition topicUriPartition, TopicUri topicUri
    ) {
        if (topicUri == null)
            logger.warn("TopicUriPartition finalization for {} with a null TopicUri.", topicUriPartition);
        else
            topicUriPartition.setTopicUri(topicUri);
    }
}
