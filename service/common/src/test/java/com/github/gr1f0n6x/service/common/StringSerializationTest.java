package com.github.gr1f0n6x.service.common;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class StringSerializationTest {
    private static Serializer<String> serializer;
    private static Deserializer<String> deserializer;

    @BeforeClass
    public static void init() {
        serializer = new StringSerializer();
        deserializer = new StringDeserializer();
    }

    @Test
    public void serializeAndDeserialize() throws IOException {
        String str = "test";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.serialize(str, out);
        byte[] bytes = out.toByteArray();
        String result = deserializer.deserialize(bytes);

        assertEquals(str, result);
    }
}
