package com.github.gr1f0n6x.service.redispool.service;

import com.github.gr1f0n6x.service.common.Deserializer;
import com.github.gr1f0n6x.service.common.Serializer;
import com.github.gr1f0n6x.service.redispool.RedisCommand;
import com.github.gr1f0n6x.service.redispool.RedisPool;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Tags({"redis", "pool", "cache", "ttl"})
@CapabilityDescription("Provides a controller service to execute operations on redis cache.")
public class RedisCommandService extends AbstractControllerService implements RedisCommand {
    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("Redis connection pool")
            .identifiesControllerService(RedisPool.class)
            .required(true)
            .build();

    private static List<PropertyDescriptor> descriptors;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(REDIS_CONNECTION_POOL);

        descriptors = Collections.unmodifiableList(props);
    }

    private volatile RedisPool pool;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnEnabled
    public void enable(final ConfigurationContext context) {
        pool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisPool.class);
    }

    @OnDisabled
    public void disable() {
        pool = null;
    }

    @Override
    public <K> boolean exists(K key, Serializer<K> serializer) throws IOException {
        return execute(client -> {
            byte[] keyBytes = serializer.serialize(key);
            return client.exists(keyBytes);

        });
    }

    @Override
    public <K, V> void set(K key, V value, Serializer<K> kSerializer, Serializer<V> vSerializer) throws IOException {
        execute(client -> {
            byte[] keyBytes = kSerializer.serialize(key);
            byte[] valueBytes = vSerializer.serialize(value);
            client.set(keyBytes, valueBytes);

            return Void.TYPE;
        });
    }

    @Override
    public <K> void delete(K key, Serializer<K> serializer) throws IOException {
        execute(client -> {
            byte[] keyBytes = serializer.serialize(key);
            client.del(keyBytes);

            return Void.TYPE;
        });
    }

    @Override
    public <K, V> V get(K key, Serializer<K> kSerializer, Deserializer<V> vDeserializer) throws IOException {
        return execute(client -> {
            byte[] keyBytes = kSerializer.serialize(key);
            byte[] bytes = client.get(keyBytes);

            return vDeserializer.deserialize(bytes);
        });
    }

    @Override
    public <K, V> void set(K key, V value, int ttl, Serializer<K> kSerializer, Serializer<V> vSerializer) throws IOException {
        execute(client -> {
            byte[] keyBytes = kSerializer.serialize(key);
            byte[] valueBytes = vSerializer.serialize(value);
            client.set(keyBytes, valueBytes);
            client.expire(keyBytes, ttl);

            return Void.TYPE;
        });
    }

    private <T> T execute(Action<T> action) throws IOException {
        try (Jedis client = pool.getConnection()) {
            return action.execute(client);
        }
    }

    private interface Action<T> {
        T execute(Jedis client) throws IOException;
    }
}
