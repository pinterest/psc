package com.pinterest.psc.common;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteArrayHandler {
    private final ByteBuffer byteBuffer;

    public byte[] asBytes() {
        return byteBuffer.array();
    }
    
    public ByteArrayHandler(byte[] byteArray) {
        this.byteBuffer = ByteBuffer.wrap(byteArray);
    }

    public ByteArrayHandler(int size) {
        this.byteBuffer = ByteBuffer.allocate(size);
    }

    public byte readByte() {
        return byteBuffer.get();
    }

    public short readShort() {
        return byteBuffer.getShort();
    }

    public int readInt() {
        return byteBuffer.getInt();
    }

    public long readLong() {
        return byteBuffer.getLong();
    }

    public byte[] readArray() {
        int len = readInt();
        byte[] bytes = new byte[len];
        byteBuffer.get(bytes);
        return bytes;
    }

    public String readString() {
        return PscCommon.byteArrayToString(readArray());
    }

    public void writeByte(byte val) {
        byteBuffer.put(val);
    }

    public void writeShort(short val) {
        byteBuffer.putShort(val);
    }

    public void writeInt(int val) {
        byteBuffer.putInt(val);
    }

    public void writeLong(long val) {
        byteBuffer.putLong(val);
    }

    public void writeArray(byte[] arr) {
        writeInt(arr.length);
        byteBuffer.put(arr);
    }

    public void writeString(String s) {
        writeArray(s.getBytes(StandardCharsets.UTF_8));
    }

    public boolean hasRemaining() {
        return byteBuffer.hasRemaining();
    }

    public static byte[] serialize(Object ... objects) {
        int size = 0;
        for (Object object : objects) {
            if (object instanceof Byte)
                size += Byte.SIZE;
            else if (object instanceof Short)
                size += Short.SIZE;
            else if (object instanceof Integer)
                size += Integer.SIZE;
            else if (object instanceof Long)
                size += Long.SIZE;
            else if (object instanceof String)
                size += Integer.SIZE + ((String) object).length();
            else if (object instanceof byte[])
                size += Integer.SIZE + ((byte[]) object).length;
        }

        ByteArrayHandler byteArrayHandler = new ByteArrayHandler(size);
        for (Object object : objects) {
            if (object instanceof Byte)
                byteArrayHandler.writeByte((byte) object);
            else if (object instanceof Short)
                byteArrayHandler.writeShort((short) object);
            else if (object instanceof Integer)
                byteArrayHandler.writeInt((int) object);
            else if (object instanceof Long)
                byteArrayHandler.writeLong((long) object);
            else if (object instanceof String)
                byteArrayHandler.writeString((String) object);
            else if (object instanceof byte[])
                byteArrayHandler.writeArray((byte[]) object);
        }

        return byteArrayHandler.asBytes();
    }
}
