package com.pinterest.psc.example.common;

import com.pinterest.psc.exception.consumer.DeserializerException;
import com.pinterest.psc.exception.producer.SerializerException;

public class TestMessageSerDe {
    public static void main(String[] args) throws SerializerException, DeserializerException {
        Message message = new Message(1, "My first message", System.currentTimeMillis());
        byte[] serialized = new MessageSerializer().serialize(message);
        Message messageDeserialized = new MessageDeserializer().deserialize(serialized);
        assert message.getId() == messageDeserialized.getId();
        assert message.getText() == messageDeserialized.getText();
        assert message.getTimestamp() == messageDeserialized.getTimestamp();
    }
}
