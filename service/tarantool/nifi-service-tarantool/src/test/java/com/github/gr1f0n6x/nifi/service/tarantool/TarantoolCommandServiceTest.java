package com.github.gr1f0n6x.nifi.service.tarantool;

import com.github.gr1f0n6x.nifi.service.common.Deserializer;
import com.github.gr1f0n6x.nifi.service.common.Serializer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TarantoolCommandServiceTest {
    private TestRunner runner;

    @Before
    public void init() {
        TestProcessor processor = new TestProcessor();
        runner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testCommands() throws InitializationException, IOException {
        Serializer<String> serializer = new TestSerializer();
        Deserializer<String> deserializer = new TestDeserializer();

        TarantoolConnection connection = new TarantoolConnectionService();
        TarantoolCommand command = new TarantoolCommandService();

        runner.addControllerService("connection", connection);
        runner.addControllerService("command", command);
        runner.setProperty(command, TarantoolCommandService.CONNECTION, "connection");
        runner.setProperty(command, TarantoolCommandService.SPACE_ID, "512"); // already created
        runner.setProperty(command, TarantoolCommandService.INDEX_ID, "0");
        runner.setProperty(command, TarantoolCommandService.ITERATOR_TYPE, TarantoolCommandService.EQ);

        runner.enableControllerService(connection);
        runner.enableControllerService(command);

        try {
            assertFalse(command.exists("key", serializer));
            command.set("key", "value", serializer, serializer);
            assertTrue(command.exists("key", serializer));
            String value = command.get("key", serializer, deserializer);
            assertEquals(value, "value");
            command.delete("key", serializer);
            assertFalse(command.exists("key", serializer));
        } finally {
            command.delete("key", serializer);
        }

    }

    public class TestSerializer implements Serializer<String> {
        @Override
        public byte[] serialize(String o) throws IOException {
            return o.getBytes(StandardCharsets.UTF_8);
        }
    }

    public class TestDeserializer implements Deserializer<String> {
        @Override
        public String deserialize(byte[] bytes) throws IOException {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
}
