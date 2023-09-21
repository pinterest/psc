package com.pinterest.psc.serde;

import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.consumer.DeserializerException;
import com.pinterest.psc.exception.producer.SerializerException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSerde {

    final private List<List<Object>> testData = Arrays.asList(
            Arrays.asList("my string".getBytes()),
            Arrays.asList(ByteBuffer.allocate(10).put("my string".getBytes())),
            Arrays.asList(5678567.12312d, -5678567.12341d),
            Arrays.asList(5678567.12312f, -5678567.12341f),
            Arrays.asList(423412424, -41243432),
            Arrays.asList(922337203685477580L, -922337203685477581L),
            Arrays.asList((short) 32767, (short) -32768),
            Arrays.asList("my string"),
            Arrays.asList(UUID.randomUUID())
    );
    final private List<Class> deserializerClasses = Arrays.asList(
            ByteArrayDeserializer.class,
            ByteBufferDeserializer.class,
            DoubleDeserializer.class,
            FloatDeserializer.class,
            IntegerDeserializer.class,
            LongDeserializer.class,
            ShortDeserializer.class,
            StringDeserializer.class,
            UuidDeserializer.class
    );
    final private List<Class> serializerClasses = Arrays.asList(
            ByteArraySerializer.class,
            ByteBufferSerializer.class,
            DoubleSerializer.class,
            FloatSerializer.class,
            IntegerSerializer.class,
            LongSerializer.class,
            ShortSerializer.class,
            StringSerializer.class,
            UuidSerializer.class
    );

    @Test
    public void deserializingNullBytes() throws IllegalAccessException, InstantiationException, DeserializerException {
        for (Class clazz : deserializerClasses) {
            assertNull(((Deserializer) clazz.newInstance()).deserialize(null));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void serializingNullBytes() throws IllegalAccessException, InstantiationException, SerializerException {
        for (Class clazz : serializerClasses) {
            assertNull(((Serializer) clazz.newInstance()).serialize(null));
        }
    }

    @Test
    public void deserializerAndBytesLength() {
        DoubleDeserializer doubleDeserializer = new DoubleDeserializer();
        assertThrows(DeserializerException.class, () -> doubleDeserializer.deserialize(new byte[9]));
        assertDoesNotThrow(() -> doubleDeserializer.deserialize(new byte[8]));

        FloatDeserializer floatDeserializer = new FloatDeserializer();
        assertThrows(DeserializerException.class, () -> floatDeserializer.deserialize(new byte[3]));
        assertDoesNotThrow(() -> floatDeserializer.deserialize(new byte[4]));

        IntegerDeserializer integerDeserializer = new IntegerDeserializer();
        assertThrows(DeserializerException.class, () -> integerDeserializer.deserialize(new byte[5]));
        assertDoesNotThrow(() -> integerDeserializer.deserialize(new byte[4]));

        LongDeserializer longDeserializer = new LongDeserializer();
        assertThrows(DeserializerException.class, () -> longDeserializer.deserialize(new byte[7]));
        assertDoesNotThrow(() -> longDeserializer.deserialize(new byte[8]));

        ShortDeserializer shortDeserializer = new ShortDeserializer();
        assertThrows(DeserializerException.class, () -> shortDeserializer.deserialize(new byte[1]));
        assertDoesNotThrow(() -> shortDeserializer.deserialize(new byte[2]));

    }

    @SuppressWarnings("unchecked")
    @Test
    public void allSerdesShouldRoundtripInput()
            throws IllegalAccessException, InstantiationException, SerializerException, DeserializerException {
        for (int i = 0; i < testData.size(); ++i) {
            List<Object> dataList = testData.get(i);
            Serializer serializer = (Serializer) serializerClasses.get(i).newInstance();
            Deserializer deserializer = (Deserializer) deserializerClasses.get(i).newInstance();

            for (Object data : dataList) {
                assertEquals(
                        data,
                        deserializer.deserialize(serializer.serialize(data)),
                        "Should get the original after deserialization for " + data);
            }
        }
    }

    @Test
    public void stringSerdeShouldSupportDifferentEncodings() throws SerializerException, DeserializerException {
        String str = "my string";
        List<String> encodings = Arrays.asList("UTF8", "UTF-16");

        for (String encoding : encodings) {
            PscConfiguration serializerConfiguration = new PscConfiguration();
            serializerConfiguration.setProperty("serializer.key.encoding", encoding);
            StringSerializer serializer = new StringSerializer();
            serializer.configure(serializerConfiguration, true);

            PscConfiguration deserializerConfiguration = new PscConfiguration();
            deserializerConfiguration.setProperty("deserializer.key.encoding", encoding);
            StringDeserializer deserializer = new StringDeserializer();
            deserializer.configure(deserializerConfiguration, true);

            assertEquals(
                    str,
                    deserializer.deserialize(serializer.serialize(str)),
                    "Should get the original string after serialization and deserialization with encoding " + encoding);
        }
    }

    @Test
    public void floatSerdeShouldPreserveNaNValues() throws DeserializerException {
        int someNaNAsIntBits = 0x7f800001;
        float someNaN = Float.intBitsToFloat(someNaNAsIntBits);
        int anotherNaNAsIntBits = 0x7f800002;
        float anotherNaN = Float.intBitsToFloat(anotherNaNAsIntBits);

        FloatDeserializer floatDeserializer = new FloatDeserializer();
        FloatSerializer floatSerializer = new FloatSerializer();
        Float roundtrip = floatDeserializer.deserialize(floatSerializer.serialize(someNaN));
        assertEquals(someNaNAsIntBits, Float.floatToRawIntBits(roundtrip));
        Float otherRoundtrip = floatDeserializer.deserialize(floatSerializer.serialize(anotherNaN));
        assertEquals(anotherNaNAsIntBits, Float.floatToRawIntBits(otherRoundtrip));
    }
}
