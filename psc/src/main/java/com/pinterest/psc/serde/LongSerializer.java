package com.pinterest.psc.serde;

public class LongSerializer implements Serializer<Long> {
    @Override
    public byte[] serialize(Long data) {
        if (data == null)
            return null;

        return new byte[]{
                (byte) (data >>> 56),
                (byte) (data >>> 48),
                (byte) (data >>> 40),
                (byte) (data >>> 32),
                (byte) (data >>> 24),
                (byte) (data >>> 16),
                (byte) (data >>> 8),
                data.byteValue()
        };
    }
}
