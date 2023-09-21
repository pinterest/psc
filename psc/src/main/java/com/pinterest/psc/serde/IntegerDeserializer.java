package com.pinterest.psc.serde;

import com.pinterest.psc.exception.consumer.DeserializerException;

public class IntegerDeserializer implements Deserializer<Integer> {

    @Override
    public Integer deserialize(byte[] bytes) throws DeserializerException {
        if (bytes == null)
            return null;
        if (bytes.length != 4) {
            throw new DeserializerException("Size of data received by IntegerDeserializer is not 4");
        }

        int value = 0;
        for (byte b : bytes) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }
}
