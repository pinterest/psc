package com.pinterest.psc.serde;

import com.pinterest.psc.exception.consumer.DeserializerException;

public class ShortDeserializer implements Deserializer<Short> {
    @Override
    public Short deserialize(byte[] bytes) throws DeserializerException {
        if (bytes == null)
            return null;
        if (bytes.length != 2) {
            throw new DeserializerException("Size of data received by ShortDeserializer is not 2");
        }

        short value = 0;
        for (byte b : bytes) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }
}
