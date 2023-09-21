package com.pinterest.psc.example.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Commons {
    public static byte[] convertToByteArray(Object object) throws IOException {
        if (object == null) return null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(object);
        out.flush();
        byte[] bytes = bos.toByteArray();
        bos.close();
        return bytes;
    }

    public static Object convertToObject(byte[] bytes) throws IOException, ClassNotFoundException {
        if (bytes == null) return null;
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = new ObjectInputStream(bis);
        Object object = in.readObject();
        in.close();
        return object;
    }
}
