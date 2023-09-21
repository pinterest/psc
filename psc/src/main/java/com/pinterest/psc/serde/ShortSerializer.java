package com.pinterest.psc.serde;

public class ShortSerializer implements Serializer<Short> {
    @Override
    public byte[] serialize(Short data) {
        if (data == null)
            return null;

        return new byte[]{
                (byte) (data >>> 8),
                data.byteValue()
        };
    }
}
