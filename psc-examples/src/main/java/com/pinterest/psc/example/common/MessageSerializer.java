package com.pinterest.psc.example.common;

import com.pinterest.psc.exception.producer.SerializerException;
import com.pinterest.psc.serde.Serializer;

import java.io.IOException;

public class MessageSerializer implements Serializer<Message> {
    @Override
    public byte[] serialize(Message message) throws SerializerException {
        try {
            return Commons.convertToByteArray(message);
        } catch (IOException e) {
            throw new SerializerException("Failed to serialize message object to byte array", e);
        }
    }
}
