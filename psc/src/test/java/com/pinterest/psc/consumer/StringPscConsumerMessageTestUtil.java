package com.pinterest.psc.consumer;

import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;

import java.util.ArrayList;
import java.util.List;

public class StringPscConsumerMessageTestUtil extends PscConsumerMessageTestUtil<String, String> {
    @Override
    public List<PscConsumerMessage<String, String>> getRandomPscConsumerMessages(int messageCount) throws
            TopicUriSyntaxException {
        List<PscConsumerMessage<String, String>> pscConsumerMessageList = new ArrayList<>();
        for (int i = 0; i < messageCount; ++i) {
            pscConsumerMessageList.add(new PscConsumerMessage<String, String>()
                    .setKey(TestUtils.getRandomString(16, 0.1))
                    .setValue(TestUtils.getRandomString(128, 0.1))
                    .setPublishTimestamp(System.currentTimeMillis())
                    .setMessageId(getRandomMessageId(random.nextInt(64), random.nextInt(102400)))
                    .addHeaders(getRandomHeaders(10))
                    .setTags(getRandomPscMessageTags(0.25))
            );
        }

        return pscConsumerMessageList;
    }
}
