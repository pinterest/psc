package com.pinterest.psc.consumer.kafka;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TestKafkaTopicUri {

    TestCase[] badCases = new TestCase[]{
            new TestCase("plaintext:badpathwithoutfirstslash"),
            new TestCase("plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":badpathwithtoofewsegments"),
            new TestCase("plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:aws_us-west-1::kafkacluster01:topic01:more")
    };

    TestCase[] goodCases = new TestCase[]{
            new TestCase("plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:topic", "region", "cluster", "topic", KafkaTopicUri.PLAINTEXT_PROTOCOL),
            new TestCase("plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:", "region", "cluster", KafkaTopicUri.PLAINTEXT_PROTOCOL),
            new TestCase("secure:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:topic", "region", "cluster", "topic", KafkaTopicUri.SECURE_PROTOCOL),
            new TestCase("plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:topic")
    };

    @Test
    void testKafkaTopicUri() {
        for (TestCase c : badCases) {
            try {
                KafkaTopicUri.validate(TopicUri.validate(c.input));
                fail();
            } catch (TopicUriSyntaxException e) {
                // OK
            }
        }

        for (TestCase c : goodCases) {
            try {
                KafkaTopicUri.validate(TopicUri.validate(c.input));
            } catch (TopicUriSyntaxException e) {
                fail(e);
            }
        }
    }

    @Test
    void testTopicValueInUri() throws TopicUriSyntaxException {
        TopicUri topicUri = TopicUri.validate(goodCases[0].input);
        KafkaTopicUri kafkaTopicUri = KafkaTopicUri.validate(topicUri);
        assertEquals(goodCases[0].expectedTopic, kafkaTopicUri.getTopic(), "Unexpected topic value");
    }


    private static class TestCase {
        String input;
        boolean isError;
        String expectedRegion;
        String expectedCluster;
        String expectedTopic;
        boolean expectedIsClusterUri;
        String expectedProtocol;

        // cluster uri
        public TestCase(String input, String expectedRegion, String expectedCluster, String expectedProtocol) {
            this.input = input;
            this.isError = false;
            this.expectedRegion = expectedRegion;
            this.expectedCluster = expectedCluster;
            this.expectedIsClusterUri = true;
            this.expectedProtocol = expectedProtocol;
        }

        // topic uri
        public TestCase(String input, String expectedRegion,
                        String expectedCluster, String expectedTopic, String expectedProtocol) {
            this.input = input;
            this.isError = false;
            this.expectedRegion = expectedRegion;
            this.expectedCluster = expectedCluster;
            this.expectedTopic = expectedTopic;
            this.expectedIsClusterUri = false;
            this.expectedProtocol = expectedProtocol;
        }

        // bad uri
        public TestCase(String input) {
            this.input = input;
            this.isError = true;
        }
    }
}