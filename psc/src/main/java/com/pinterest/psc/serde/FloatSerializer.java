package com.pinterest.psc.serde;

public class FloatSerializer implements Serializer<Float> {
    @Override
    public byte[] serialize(Float data) {
        if (data == null)
            return null;

        long bits = Float.floatToRawIntBits(data);
        return new byte[]{
                (byte) (bits >>> 24),
                (byte) (bits >>> 16),
                (byte) (bits >>> 8),
                (byte) bits
        };
    }
}
