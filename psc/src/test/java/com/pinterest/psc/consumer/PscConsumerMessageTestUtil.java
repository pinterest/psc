package com.pinterest.psc.consumer;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.PscMessageTags;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.junit.jupiter.api.Assertions;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public abstract class PscConsumerMessageTestUtil<K, V> {
    protected static final Random random = new Random();

    public abstract List<PscConsumerMessage<K, V>> getRandomPscConsumerMessages(int messageCount) throws URISyntaxException,
            TopicUriSyntaxException;

    public void verifyIdenticalLists(List<PscConsumerMessage<K, V>> list1, List<PscConsumerMessage<K, V>> list2) {
        Assertions.assertTrue(PscCommon.equals(list1, list2));
    }

    protected static Set<PscMessageTags> getRandomPscMessageTags(double chanceOfNull) {
        if (random.nextDouble() < chanceOfNull)
            return null;

        Set<PscMessageTags> randomTags = new HashSet<>();
        List<PscConsumerMessage.DefaultPscConsumerMessageTags> allTags =
                Arrays.asList(PscConsumerMessage.DefaultPscConsumerMessageTags.values());
        Collections.shuffle(allTags);
        int toRemove = random.nextInt(allTags.size());
        for (int i = 0; i < toRemove; ++i)
            randomTags.add(allTags.get(i));
        return randomTags;
    }

    protected static Map<String, byte[]> getRandomHeaders(int headerCount) {
        Map<String, byte[]> headers = new HashMap<>();
        for (int i = 0; i < headerCount; ++i) {
            byte[] bytes = new byte[random.nextInt(256)];
            random.nextBytes(bytes);
            headers.put(TestUtils.getRandomString(64), TestUtils.getRandomBytes(256));
        }
        return headers;
    }

    protected static MessageId getRandomMessageId(int keySize, int valueSize) throws TopicUriSyntaxException {
        return new MessageId(
                TestUtils.getFinalizedTopicUriPartition(getRandomTopicUri(), random.nextInt(60)),
                random.nextInt(256),
                keySize,
                valueSize
        );
    }

    protected static TopicUri getRandomTopicUri() throws TopicUriSyntaxException {
        // pattern: <protocol>:/<topicRN>
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(String.format("%s:%s%s:%s",
                TestUtils.getRandomString(10, true),
                TopicUri.SEPARATOR,
                TopicUri.STANDARD,
                TestUtils.getRandomString(10, true)) // topicRnString
        );

        // add RN subparts to cloud
        String subpart;
        for (int i = 0; i < 2; ++i) {
            while ((subpart = TestUtils.getRandomString(10, true)).isEmpty()) ;
            stringBuilder.append(":").append(subpart);
        }

        stringBuilder.append("_"); // cloud/region separator

        // add rest of RN subparts
        for (int i = 0; i < 4; ++i) {
            while ((subpart = TestUtils.getRandomString(10, true)).isEmpty()) ;
            stringBuilder.append(subpart);
            if (i < 3)
                stringBuilder.append(":");
        }

        return TopicUri.validate(stringBuilder.toString());
    }
}
