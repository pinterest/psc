package com.pinterest.psc.consumer;

import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;

import java.util.ArrayList;
import java.util.List;

public class BytesPscConsumerMessageTestUtil extends PscConsumerMessageTestUtil<byte[], byte[]> {
    @Override
    public List<PscConsumerMessage<byte[], byte[]>> getRandomPscConsumerMessages(int messageCount) throws
            TopicUriSyntaxException {
        List<PscConsumerMessage<byte[], byte[]>> pscConsumerMessageList = new ArrayList<>();
        for (int i = 0; i < messageCount; ++i) {
            byte[] key = TestUtils.getRandomBytes(16, 0.5);
            byte[] value = TestUtils.getRandomBytes(128, 0.1);
            pscConsumerMessageList.add(new PscConsumerMessage<byte[], byte[]>()
                    .setKey(key)
                    .setValue(value)
                    .setPublishTimestamp(System.currentTimeMillis())
                    .setMessageId(getRandomMessageId(key == null ? -1 : key.length, value == null ? -1 : value.length))
                    .addHeaders(getRandomHeaders(6))
                    .setTags(getRandomPscMessageTags(0.25))
            );
        }

        return pscConsumerMessageList;
    }
}
