package com.pinterest.psc.serde;

import java.nio.ByteBuffer;

public class ByteBufferDeserializer implements Deserializer<ByteBuffer> {
    @Override
    public ByteBuffer deserialize(byte[] bytes) {
        if (bytes == null)
            return null;

        return ByteBuffer.wrap(bytes);
    }
}
