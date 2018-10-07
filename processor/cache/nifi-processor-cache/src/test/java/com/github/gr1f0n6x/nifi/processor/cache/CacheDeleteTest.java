package com.github.gr1f0n6x.nifi.processor.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.gr1f0n6x.nifi.processor.cache.processor.CacheBase;
import com.github.gr1f0n6x.nifi.processor.cache.processor.CacheDelete;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;


public class CacheDeleteTest {
    private TestRunner runner;
    private CacheDelete cacheDelete;

    @Before
    public void init() {
        cacheDelete = new CacheDelete();
        runner = TestRunners.newTestRunner(cacheDelete);
    }

    @Test
    public void deleteSuccess() throws InitializationException, IOException {
        InMemoryCache cache = new InMemoryCache();
        ObjectMapper mapper = new ObjectMapper();
        cache.map.put(mapper.createObjectNode().put("key", 1).get("key"), null);
        assertTrue(!cache.map.isEmpty());

        runner.addControllerService("cache-provider", cache);
        runner.enableControllerService(cache);
        runner.setProperty(CacheBase.CACHE, "cache-provider");
        runner.setProperty(CacheBase.KEY_FIELD, "key");
        runner.setProperty(CacheBase.SERIALIZER, CacheBase.JSON_SERIALIZER);
        runner.setProperty(CacheBase.DESERIALIZER, CacheBase.JSON_DESERIALIZER);
        runner.enqueue("{\"key\":1, \"value\":2}");
        runner.run();

        runner.assertTransferCount(CacheBase.SUCCESS, 1);
        runner.assertQueueEmpty();
        assertTrue(cache.map.isEmpty());
    }

    @Test
    public void deleteError() throws InitializationException, JsonProcessingException {
        InMemoryCache cache = new InMemoryCache();
        ObjectMapper mapper = new ObjectMapper();
        cache.map.put(mapper.createObjectNode().put("key", 1).get("key"), null);
        assertTrue(!cache.map.isEmpty());

        runner.addControllerService("cache-provider", cache);
        runner.enableControllerService(cache);
        runner.setProperty(CacheBase.SERIALIZER, CacheBase.JSON_SERIALIZER);
        runner.setProperty(CacheBase.DESERIALIZER, CacheBase.JSON_DESERIALIZER);
        runner.setProperty(CacheBase.CACHE, "cache-provider");
        runner.setProperty(CacheBase.KEY_FIELD, "incorrect-key");
        runner.enqueue("{\"key\":1, \"value\":2}");
        runner.run();

        runner.assertTransferCount(CacheBase.FAILURE, 1);
        runner.assertQueueEmpty();
        assertTrue(!cache.map.isEmpty());
    }
}
