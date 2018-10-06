package com.github.gr1f0n6x.processor.cache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.gr1f0n6x.processor.cache.processor.CacheBase;
import com.github.gr1f0n6x.processor.cache.processor.CacheGet;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class CacheGetTest {
    private TestRunner runner;
    private CacheGet cacheGet;

    @Before
    public void init() {
        cacheGet = new CacheGet();
        runner = TestRunners.newTestRunner(cacheGet);
    }

    @Test
    public void getSuccess() throws InitializationException {
        InMemoryCache cache = new InMemoryCache();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.createObjectNode().put("key", 1).put("value", 2);
        cache.map.put(node.get("key"), node);
        assertTrue(!cache.map.isEmpty());

        runner.addControllerService("cache-provider", cache);
        runner.enableControllerService(cache);
        runner.setProperty(CacheBase.CACHE, "cache-provider");
        runner.setProperty(CacheBase.KEY_FIELD, "key");
        runner.setProperty(CacheBase.SERIALIZER, CacheBase.JSON_SERIALIZER);
        runner.setProperty(CacheBase.DESERIALIZER, CacheBase.JSON_DESERIALIZER);
        runner.enqueue("{\"key\":1}");
        runner.run();

        runner.assertTransferCount(CacheBase.SUCCESS, 1);
        runner.assertTransferCount(CacheBase.ORIGINAL, 1);
        runner.assertQueueEmpty();
        List<MockFlowFile> fileList = runner.getFlowFilesForRelationship(CacheBase.SUCCESS);
        fileList.forEach(f -> {
            try {
                assertEquals(node, mapper.readTree(f.toByteArray()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void getError() throws InitializationException {
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
        runner.enqueue("{\"key\":1}");
        runner.run();

        runner.assertTransferCount(CacheBase.FAILURE, 1);
        runner.assertTransferCount(CacheBase.ORIGINAL, 0);
        runner.assertQueueEmpty();
    }

    @Test
    public void noValue() throws InitializationException {
        InMemoryCache cache = new InMemoryCache();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.createObjectNode().put("key", 1).put("value", 2);
        cache.map.put(node.get("key"), node);
        assertTrue(!cache.map.isEmpty());

        runner.addControllerService("cache-provider", cache);
        runner.enableControllerService(cache);
        runner.setProperty(CacheBase.SERIALIZER, CacheBase.JSON_SERIALIZER);
        runner.setProperty(CacheBase.DESERIALIZER, CacheBase.JSON_DESERIALIZER);
        runner.setProperty(CacheBase.CACHE, "cache-provider");
        runner.setProperty(CacheBase.KEY_FIELD, "key");
        runner.enqueue("{\"key\":2}");
        runner.run();

        runner.assertTransferCount(CacheBase.SUCCESS, 1);
        runner.assertTransferCount(CacheBase.ORIGINAL, 1);
        runner.assertQueueEmpty();
        List<MockFlowFile> fileList = runner.getFlowFilesForRelationship(CacheBase.SUCCESS);
        fileList.forEach(f -> {
            try {
                assertNotEquals(node, mapper.readTree(f.toByteArray()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
