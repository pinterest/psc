package com.pinterest.psc.example.common;

import com.pinterest.psc.exception.consumer.DeserializerException;
import com.pinterest.psc.serde.Deserializer;

import java.io.IOException;

public class MessageDeserializer implements Deserializer<Message> {
    @Override
    public Message deserialize(byte[] bytes) throws DeserializerException {
        try {
            return (Message) Commons.convertToObject(bytes);
        } catch (IOException | ClassNotFoundException e) {
            throw new DeserializerException("Failed to deserialize byte array to Message object.", e);
        }
    }
}
