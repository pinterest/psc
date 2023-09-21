package com.pinterest.psc.common;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class PscCommon {
    private static final String HOSTNAME;
    private static final String HOST_IP;

    static {
        String holder = "";
        try {
            holder = InetAddress.getLocalHost().getHostName();
            int firstDotPos = holder.indexOf('.');
            if (firstDotPos > 0) {
                holder = holder.substring(0, firstDotPos);
            }
        } catch (Exception e) {
            holder = System.getenv("HOSTNAME");
        } finally {
            HOSTNAME = holder;
        }

        try {
            holder = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            holder = "";
        } finally {
            HOST_IP = holder;
        }
    }

    public static String getHostname() {
        return HOSTNAME;
    }

    public static String getHostIp() {
        return HOST_IP;
    }

    public static <T> boolean equals(List<T> list1, List<T> list2) {
        if (list1 == null && list2 == null)
            return true;
        if (list1 == null || list2 == null)
            return false;
        if (list1 == list2)
            return true;
        return list1.equals(list2);
    }

    public static <K, V> boolean equals(Map<K, V> map1, Map<K, V> map2) {
        if (map1 == null && map2 == null)
            return true;
        if (map1 == null || map2 == null)
            return false;
        if (map1 == map2)
            return true;
        return map1.equals(map2);
    }

    public static boolean equals(byte[] bytes1, byte[] bytes2) {
        if (bytes1 == null && bytes2 == null)
            return true;
        if (bytes1 == null || bytes2 == null)
            return false;
        if (bytes1 == bytes2)
            return true;
        if (bytes1.length != bytes2.length)
            return false;

        for (int i = 0; i < bytes1.length; ++i) {
            if (bytes1[i] != bytes2[i])
                return false;
        }

        return true;
    }

    public static int compare(byte[] bytes1, byte[] bytes2) {
        if (bytes1 == bytes2)
            return 0;
        if (bytes1 == null)
            return -1;
        if (bytes2 == null)
            return 1;

        int result;
        for (int i = 0; i < Math.min(bytes1.length, bytes2.length); ++i) {
            if ((result = Byte.compare(bytes1[i], bytes2[i])) != 0)
                return result;
        }

        return Integer.compare(bytes1.length, bytes2.length);
    }

    public static <T> boolean equals(T obj1, T obj2) {
        if (obj1 == null && obj2 == null)
            return true;
        if (obj1 == null || obj2 == null)
            return false;
        return obj1.equals(obj2);
    }

    public static byte[] intToByteArray(int n) {
        return new byte[]{
                (byte) ((n >> 24) & 0xff),
                (byte) ((n >> 16) & 0xff),
                (byte) ((n >> 8) & 0xff),
                (byte) ((n) & 0xff),
        };
    }

    public static byte[] longToByteArray(long n) {
        return new byte[]{
                (byte) ((n >> 56) & 0xff),
                (byte) ((n >> 48) & 0xff),
                (byte) ((n >> 40) & 0xff),
                (byte) ((n >> 32) & 0xff),
                (byte) ((n >> 24) & 0xff),
                (byte) ((n >> 16) & 0xff),
                (byte) ((n >> 8) & 0xff),
                (byte) ((n) & 0xff),
        };
    }

    public static byte[] stringToByteArray(String s) {
        if (s == null)
            return null;
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static int byteArrayToInt(byte[] bytes) {
        if (bytes == null || bytes.length != 4)
            return -1; // unexpected

        return (0xff & bytes[0]) << 24 |
                (0xff & bytes[1]) << 16 |
                (0xff & bytes[2]) << 8 |
                (0xff & bytes[3]);
    }

    public static long byteArrayToLong(byte[] bytes) {
        if (bytes == null || bytes.length != 8)
            return -1; // unexpected

        return ((long) 0xff & bytes[0]) << 56 |
                ((long) 0xff & bytes[1]) << 48 |
                ((long) 0xff & bytes[2]) << 40 |
                ((long) 0xff & bytes[3]) << 32 |
                ((long) 0xff & bytes[4]) << 24 |
                ((long) 0xff & bytes[5]) << 16 |
                ((long) 0xff & bytes[6]) << 8 |
                ((long) 0xff & bytes[7]);
    }

    public static String byteArrayToString(byte[] bytes) {
        if (bytes == null)
            return null;

        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static Object getField(Object object, String fieldName) {
        return getField(object, object.getClass(), fieldName);
    }

    public static Object getField(Object object, Class<?> clazz, String fieldName) {
        try {
            Field field = getDeclaredField(clazz, fieldName);
            if (field == null)
                throw new NoSuchFieldException(clazz.getName() + ":" + fieldName);
            field.setAccessible(true);
            return field.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible Object version", e);
        }
    }

    private static Field getDeclaredField(Class<?> clazz, String fieldName) {
        try {
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException exception) {
            if (clazz.getSuperclass() != null)
                return getDeclaredField(clazz.getSuperclass(), fieldName);
            else
                return null;
        }
    }

    public static Object getField(Class<?> clazz, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(null);
    }

    public static Object invoke(Object object, String methodName, Object... args) {
        Class<?>[] argTypes = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argTypes[i] = args[i].getClass();
        }
        return invoke(object, methodName, argTypes, args);
    }

    public static Object invoke(Object object, String methodName, Class<?>[] argTypes, Object[] args) {
        try {
            Method method = object.getClass().getDeclaredMethod(methodName, argTypes);
            method.setAccessible(true);
            return method.invoke(object, args);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible Object version", e);
        }
    }

    public static Object invoke(Class<?> clazz, String methodName, Class<?>[] argTypes, Object[] args) {
        try {
            Method method = clazz.getDeclaredMethod(methodName, argTypes);
            method.setAccessible(true);
            return method.invoke(null, args);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(
                    String.format("Method '%s' was not found in class '%s'", methodName, clazz.getName()), e
            );
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(
                    String.format("Could not call static method '%s' of class '%s'", methodName, clazz.getName()), e
            );
        }
    }

    public static Object invoke(Class<?> clazz, String methodName) {
        try {
            Method method = clazz.getDeclaredMethod(methodName);
            method.setAccessible(true);
            return method.invoke(null);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(
                    String.format("Method '%s' was not found in class '%s'", methodName, clazz.getName()), e
            );
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(
                    String.format("Could not call static method '%s' of class '%s'", methodName, clazz.getName()), e
            );
        }
    }

    public static Enum<?> getEnum(String enumFullName) {
        String[] x = enumFullName.split("\\.(?=[^\\.]+$)");
        if (x.length == 2) {
            String enumClassName = x[0];
            String enumName = x[1];
            try {
                Class<Enum> cl = (Class<Enum>) Class.forName(enumClassName);
                return Enum.valueOf(cl, enumName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Incompatible Object version", e);
            }
        }
        return null;
    }

    public static void setField(Object object, String fieldName, Object value) {
        try {
            Field field = getDeclaredField(object.getClass(), fieldName);
            if (field == null)
                throw new NoSuchFieldException(object.getClass().getName() + ":" + fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible Object version", e);
        }
    }

    public static <T> T verifyNotNull(T obj) {
        if (obj == null)
            throw new NullPointerException();
        else
            return obj;
    }
}