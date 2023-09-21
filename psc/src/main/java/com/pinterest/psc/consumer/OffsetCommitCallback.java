package com.pinterest.psc.consumer;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;

import java.util.Map;

public interface OffsetCommitCallback {
    void onCompletion(Map<TopicUriPartition, MessageId> offsets, Exception exception);
}
