package com.github.gr1f0n6x.processor.cache;

import com.github.gr1f0n6x.processor.cache.processor.CachePut;
import com.github.gr1f0n6x.processor.cache.utils.Relationships;
import com.github.gr1f0n6x.service.common.Cache;
import com.github.gr1f0n6x.service.common.Deserializer;
import com.github.gr1f0n6x.service.common.Serializer;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CachePutTest {
    private TestRunner runner;
    private CachePut cachePut;

    @Before
    public void init() {
        cachePut = new CachePut();
        runner = TestRunners.newTestRunner(cachePut);
    }

    @Test
    public void putSuccess() throws InitializationException {
        Cache cache = new TestCache();
        Serializer serializer = new TestSerializer();
        runner.addControllerService("cache-provider", cache);
//        runner.addControllerService("serializer", serializer);
        runner.enableControllerService(cache);
//        runner.enableControllerService(serializer);
        runner.setProperty(Properties.CACHE, "cache-provider");
        runner.setProperty(Properties.KEY_FIELD, "key");
        runner.setProperty(Properties.SERIALIZER, "serializer");
        runner.enqueue("{\"key\":1, \"value\":2}");
        runner.run();

        runner.assertTransferCount(Relationships.SUCCESS, 1);
    }

    @Test
    public void putFailure() throws InitializationException {
        Cache cache = new TestCache();
        Serializer serializer = new TestSerializer();
        runner.addControllerService("cache-provider", cache);
//        runner.addControllerService("serializer", serializer);
        runner.enableControllerService(cache);
//        runner.enableControllerService(serializer);
        runner.setProperty(Properties.CACHE, "cache-provider");
        runner.setProperty(Properties.KEY_FIELD, "incorrect-key");
        runner.setProperty(Properties.SERIALIZER, "serializer");
        runner.enqueue("{\"key\":1, \"value\":2}");
        runner.run();

        runner.assertTransferCount(Relationships.FAILURE, 0);
    }

    public static class TestCache extends AbstractControllerService implements Cache {
        Map<Object, Object> inMemory = new HashMap<>();

        @Override
        public <K> boolean exists(K key, Serializer<K> serializer) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            serializer.serialize(key);
            return inMemory.containsKey(out.toByteArray());
        }

        @Override
        public <K, V> String set(K key, V value, Serializer<K> kSerializer, Serializer<V> vSerializer) throws IOException {
            ByteArrayOutputStream kOut = new ByteArrayOutputStream();
            ByteArrayOutputStream vOut = new ByteArrayOutputStream();
            kSerializer.serialize(key);
            vSerializer.serialize(value);
            inMemory.put(kOut.toByteArray(), vOut.toByteArray());
            return "ok";
        }

        @Override
        public <K> Long delete(K key, Serializer<K> serializer) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            serializer.serialize(key);
            inMemory.remove(out.toByteArray());
            return 1L;
        }

        @Override
        public <K, V> V get(K key, Serializer<K> kSerializer, Deserializer<V> vDeserializer) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            kSerializer.serialize(key);
            return vDeserializer.deserialize((byte[]) inMemory.get(out.toByteArray()));
        }
    }

    public static class TestSerializer implements Serializer<String> {
        @Override
        public byte[] serialize(String o) throws IOException {
            return new byte[0];
        }
    }

    public static class TestDeserializer implements Deserializer<String> {
        @Override
        public String deserialize(byte[] bytes) {
            return new String(bytes);
        }
    }
}
