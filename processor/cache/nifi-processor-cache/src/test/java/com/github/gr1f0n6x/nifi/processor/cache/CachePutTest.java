package com.github.gr1f0n6x.nifi.processor.cache;

import com.github.gr1f0n6x.nifi.processor.cache.processor.CacheBase;
import com.github.gr1f0n6x.nifi.processor.cache.processor.CachePut;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

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
        InMemoryCache cache = new InMemoryCache();
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
        assertTrue(!cache.map.isEmpty());
    }

    @Test
    public void putFailure() throws InitializationException {
        InMemoryCache cache = new InMemoryCache();
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
        assertTrue(cache.map.isEmpty());
    }
}
