package com.pinterest.psc.serde;

public class ByteArraySerializer implements Serializer<byte[]> {

    @Override
    public byte[] serialize(byte[] data) {
        return data;
    }
}
