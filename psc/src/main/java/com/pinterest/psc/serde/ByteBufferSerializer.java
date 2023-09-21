package com.pinterest.psc.serde;

import java.nio.ByteBuffer;

public class ByteBufferSerializer implements Serializer<ByteBuffer> {
    @Override
    public byte[] serialize(ByteBuffer data) {
        if (data == null)
            return null;

        data.rewind();

        if (data.hasArray()) {
            byte[] arr = data.array();
            if (data.arrayOffset() == 0 && arr.length == data.remaining()) {
                return arr;
            }
        }

        byte[] ret = new byte[data.remaining()];
        data.get(ret, 0, ret.length);
        data.rewind();
        return ret;
    }
}
