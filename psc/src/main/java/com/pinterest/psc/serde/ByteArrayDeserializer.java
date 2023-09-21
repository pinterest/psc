package com.pinterest.psc.serde;

public class ByteArrayDeserializer implements Deserializer<byte[]> {

    @Override
    public byte[] deserialize(byte[] bytes) {
        return bytes;
    }
}
