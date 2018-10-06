package com.github.gr1f0n6x.processor.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.gr1f0n6x.processor.cache.processor.CacheBase;
import com.github.gr1f0n6x.processor.cache.processor.CacheLookup;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class CacheLookupTest {
    private TestRunner runner;
    private CacheLookup cacheLookup;

    @Before
    public void init() {
        cacheLookup = new CacheLookup();
        runner = TestRunners.newTestRunner(cacheLookup);
    }

    @Test
    public void exist() throws InitializationException {
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

        runner.assertTransferCount(CacheBase.EXIST, 1);
        runner.assertQueueEmpty();
    }

    @Test
    public void notExist() throws InitializationException {
        InMemoryCache cache = new InMemoryCache();
        ObjectMapper mapper = new ObjectMapper();
        cache.map.put(mapper.createObjectNode().put("key", 1).get("key"), null);
        assertTrue(!cache.map.isEmpty());

        runner.addControllerService("cache-provider", cache);
        runner.enableControllerService(cache);
        runner.setProperty(CacheBase.SERIALIZER, CacheBase.JSON_SERIALIZER);
        runner.setProperty(CacheBase.DESERIALIZER, CacheBase.JSON_DESERIALIZER);
        runner.setProperty(CacheBase.CACHE, "cache-provider");
        runner.setProperty(CacheBase.KEY_FIELD, "key");
        runner.enqueue("{\"key\":3, \"value\":2}");
        runner.run();

        runner.assertTransferCount(CacheBase.NOT_EXIST, 1);
        runner.assertQueueEmpty();
    }

    @Test
    public void failure() throws InitializationException {
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
    }
}
