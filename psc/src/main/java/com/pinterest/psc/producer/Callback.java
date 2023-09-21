package com.pinterest.psc.producer;

import com.pinterest.psc.common.MessageId;

public interface Callback {
    void onCompletion(MessageId messageId, Exception exception);
}
