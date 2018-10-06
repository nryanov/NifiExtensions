package com.github.gr1f0n6x.processor.cache;

import com.github.gr1f0n6x.service.common.Deserializer;
import com.github.gr1f0n6x.service.common.ExpirableCache;
import com.github.gr1f0n6x.service.common.Serializer;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Use simple <Object, Object> map instead of byte[], byte[] to simplify testing
 */
public class InMemoryCache extends AbstractControllerService implements ExpirableCache {
    Map<Object, Object> map = new HashMap<>();; // package-access for testing

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        super.init(config);
    }

    @Override
    public <K, V> void set(K key, V value, int ttl, Serializer<K> kSerializer, Serializer<V> vSerializer) throws IOException {
        map.put(key, value);
    }

    @Override
    public <K> boolean exists(K key, Serializer<K> serializer) throws IOException {
        return map.containsKey(key);
    }

    @Override
    public <K, V> void set(K key, V value, Serializer<K> kSerializer, Serializer<V> vSerializer) throws IOException {
        map.put(key, value);
    }

    @Override
    public <K> void delete(K key, Serializer<K> serializer) throws IOException {
        map.remove(key);
    }

    @Override
    public <K, V> V get(K key, Serializer<K> kSerializer, Deserializer<V> vDeserializer) throws IOException {
        return (V) map.get(key);
    }
}
