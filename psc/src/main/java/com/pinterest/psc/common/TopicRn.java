package com.pinterest.psc.common;

import com.pinterest.psc.exception.startup.TopicRnSyntaxException;
import com.pinterest.psc.logging.PscLogger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TopicRn or topic resource name is an identifier for a PubSub topic. It uses an AWS ARN like format, which is
 * <pre>{@code
 * <standard>:<service>:<environment>:<cloud_region>:<classifier>:<cluster>:<topic>
 * }</pre>
 * In the scope of PubSub, here's how different components of this RN are mapped to PubSub related concepts:
 * <ul>
 *     <li><code>standard</code>: the standard name for this rn; e.g. <code>rn</code></li>
 *     <li><code>service</code>: the type of PubSub service; e.g. <code>kafka</code></li>
 *     <li><code>environment</code>: represents the corresponding stage; e.g. <code>prod</code>, <code>dev</code></li>
 *     <li><code>cloud_region</code>: the associated cloud provider and region; e.g. <code>aws_us-west-1</code></li>
 *     <li><code>classifier</code>: unused</li>
 *     <li><code>cluster</code>: the name of PubSub cluster; e.g. <code>kafkacluster01</code></li>
 *     <li><code>topic</code>: the topic name in the PubSub cluster; e.g. <code>topic01</code></li>
 * </ul>
 * An example of topic RN would be <code>rn:kafka:env:aws_us-west-1::kafkacluster01:topic01</code>.
 */
public class TopicRn implements Serializable {
    private static final PscLogger logger = PscLogger.getLogger(BaseTopicUri.class);
    private static final long serialVersionUID = -6081489815985829052L;
    protected static byte CURRENT_VERSION = 0;
    public static final String STANDARD = getTopicRnStandard();

    static final String[] SCHEMAS = {
            "<standard>:<service>:<environment>:<cloud_region>:<classifier>:<cluster>:<topic>"
    };
    private static final Pattern[] REGEXES = {
            Pattern.compile(
                    "(?<standard>[a-zA-Z0-9-_.]+):(?<service>[a-zA-Z0-9-_.]+):(?<environment>[a-zA-Z0-9-_.]+):(?<cloud>[a-zA-Z0-9-.]+)_(?<region>[a-zA-Z0-9-.]+):(?<classifier>[a-zA-Z0-9-_.]*):(?<cluster>[a-zA-Z0-9-_.]+):(?<topic>[a-zA-Z0-9-_.]+)?"
            )
    };

    private String topicRnString;
    /**
     * topicRnPrefixString is the same as topicRnString with the last portion (resource id, if present) removed. It ends with a ":".
     */
    private String topicRnPrefixString;
    private String standard;
    private String service;
    private String environment;
    private String cloud;
    private String region;
    private String classifier;
    private String cluster;
    private String topic;
    private byte version;

    public TopicRn(
            String topicRnString,
            String topicRnStringPrefix,
            String standard,
            String service,
            String environment,
            String cloud,
            String region,
            String classifier,
            String cluster,
            String topic,
            byte version
    ) {
        this.topicRnString = topicRnString;
        this.topicRnPrefixString = topicRnStringPrefix;
        this.standard = standard;
        this.service = service;
        this.environment = environment;
        this.cloud = cloud;
        this.region = region;
        this.classifier = classifier;
        this.cluster = cluster;
        this.topic = topic;
        this.version = version;
    }

    public TopicRn(
            String topicRnString,
            String topicRnStringPrefix,
            String standard,
            String service,
            String environment,
            String cloud,
            String region,
            String classifier,
            String cluster,
            String topic
    ) {
        this(
                topicRnString,
                topicRnStringPrefix,
                standard,
                service,
                environment,
                cloud,
                region,
                classifier,
                cluster,
                topic,
                CURRENT_VERSION
        );
    }

    public String getTopicRnPrefixString() {
        return topicRnPrefixString;
    }

    public String getStandard() {
        return standard;
    }

    public String getService() {
        return service;
    }

    public String getEnvironment() {
        return environment;
    }

    public String getCloud() {
        return cloud;
    }

    public String getRegion() {
        return region;
    }

    public String getClassifier() {
        return classifier;
    }

    public String getCluster() {
        return cluster;
    }

    public String getTopic() {
        return topic;
    }

    public byte getVersion() {
        return version;
    }

    public byte[] serialize() {
        return ByteArrayHandler.serialize(CURRENT_VERSION, topicRnString);
    }

    private static String upgradeTopicRnToCurrentVersion(String topicRnAsStr, byte serializedVersion) throws TopicRnSyntaxException {
        if (serializedVersion == TopicRn.CURRENT_VERSION)
            return topicRnAsStr;

        /*
        switch (serializedVersion) {
            case 0:
                // convert the topic RN of version 0 to the current version
                // return the converted topic RN
            case 1:
                // convert the topic RN of version 1 to the current version
                // return the converted topic RN
            default:
                throw new TopicUriSyntaxException(String.format("Unsupported topic URI version %d", serializedVersion));
        }
         */

        throw new TopicRnSyntaxException(String.format("Unsupported topic RN version %d", serializedVersion));
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                topicRnString,
                topicRnPrefixString,
                standard,
                service,
                environment,
                cloud,
                region,
                classifier,
                cluster,
                topic
        );
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        TopicRn otherTopicRn = (TopicRn) other;
        return PscCommon.equals(topicRnString, otherTopicRn.topicRnString) &&
                PscCommon.equals(topicRnPrefixString, otherTopicRn.topicRnPrefixString) &&
                PscCommon.equals(standard, otherTopicRn.standard) &&
                PscCommon.equals(service, otherTopicRn.service) &&
                PscCommon.equals(environment, otherTopicRn.environment) &&
                PscCommon.equals(cloud, otherTopicRn.cloud) &&
                PscCommon.equals(region, otherTopicRn.region) &&
                PscCommon.equals(classifier, otherTopicRn.classifier) &&
                PscCommon.equals(cluster, otherTopicRn.cluster) &&
                PscCommon.equals(topic, otherTopicRn.topic);
    }

    @Override
    public String toString() {
        return topicRnPrefixString + (topic == null ? "" : topic);
    }

    private static String getTopicRnStandard() {
        Properties properties = new Properties();
        try {
            properties.load(TopicRn.class.getClassLoader().getResourceAsStream("psc.properties"));
        } catch (IOException ioe) {
            logger.warn("Failed to get Topic URI standard", ioe);
        }
        return properties.getProperty("topic.rn.standard", "rn");
    }

    static TopicRn validate(String topicRnString) throws TopicRnSyntaxException {
        Matcher matcher = REGEXES[CURRENT_VERSION].matcher(topicRnString);
        if (matcher.matches()) {
            String standard = matcher.group("standard");
            if (!PscCommon.equals(standard, STANDARD)) {
                throw new TopicRnSyntaxException(
                        topicRnString,
                        String.format("Expected '%s' as topic RN standard, found '%s'", STANDARD, standard)
                );
            }

            String topic = matcher.group("topic");
            return new TopicRn(
                    topicRnString,
                    topic == null ?  topicRnString : topicRnString.substring(0, topicRnString.lastIndexOf(":") + 1),
                    standard,
                    matcher.group("service"),
                    matcher.group("environment"),
                    matcher.group("cloud"),
                    matcher.group("region"),
                    matcher.group("classifier"),
                    matcher.group("cluster"),
                    topic
            );
        } else {
            throw new TopicRnSyntaxException(
                    topicRnString, String.format("Failed to match the expected schema '%s'", SCHEMAS[CURRENT_VERSION])
            );
        }
    }
}
