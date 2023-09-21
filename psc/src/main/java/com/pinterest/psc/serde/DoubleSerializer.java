package com.pinterest.psc.serde;

public class DoubleSerializer implements Serializer<Double> {
    @Override
    public byte[] serialize(Double data) {
        if (data == null)
            return null;

        long bits = Double.doubleToLongBits(data);
        return new byte[]{
                (byte) (bits >>> 56),
                (byte) (bits >>> 48),
                (byte) (bits >>> 40),
                (byte) (bits >>> 32),
                (byte) (bits >>> 24),
                (byte) (bits >>> 16),
                (byte) (bits >>> 8),
                (byte) bits
        };
    }
}
