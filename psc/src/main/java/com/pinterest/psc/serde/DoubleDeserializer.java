package com.pinterest.psc.serde;

import com.pinterest.psc.exception.consumer.DeserializerException;

public class DoubleDeserializer implements Deserializer<Double> {
    @Override
    public Double deserialize(byte[] bytes) throws DeserializerException {
        if (bytes == null)
            return null;
        if (bytes.length != 8) {
            throw new DeserializerException("Size of data received by Deserializer is not 8");
        }

        long value = 0;
        for (byte b : bytes) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return Double.longBitsToDouble(value);
    }
}
