package com.github.gr1f0n6x.service.redispool.service;

import com.github.gr1f0n6x.service.common.Deserializer;
import com.github.gr1f0n6x.service.common.Serializer;
import com.github.gr1f0n6x.service.redispool.RedisCommands;
import com.github.gr1f0n6x.service.redispool.RedisPool;
import com.github.gr1f0n6x.service.redispool.util.RedisAction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Tags({"redis", "pool", "cache"})
@CapabilityDescription("Provides a controller service to execute operations on redis cache.")
public class RedisCommandsService extends AbstractControllerService implements RedisCommands {
    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("Redis connection pool")
            .identifiesControllerService(RedisPool.class)
            .required(true)
            .build();

    public static final PropertyDescriptor TTL = new PropertyDescriptor.Builder()
            .name("TTL")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .defaultValue("0 secs")
            .build();

    private static List<PropertyDescriptor> descriptors;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(REDIS_CONNECTION_POOL);
        props.add(TTL);

        descriptors = Collections.unmodifiableList(props);
    }

    private volatile RedisPool pool;
    private Long ttl;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnEnabled
    public void enable(final ConfigurationContext context) {
        pool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisPool.class);
        ttl = context.getProperty(TTL).asTimePeriod(TimeUnit.SECONDS);

        if (ttl == 0L) {
            ttl = -1L;
        }
    }

    @OnDisabled
    public void disable() {
        pool = null;
    }

    @Override
    public <K> boolean exists(K key, Serializer<K> serializer) throws IOException {
        return execute(client -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            serializer.serialize(key, out);
            return client.exists(out.toByteArray());

        });
    }

    @Override
    public <K, V> String set(K key, V value, Serializer<K> kSerializer, Serializer<V> vSerializer) throws IOException {
        return execute(client -> {
            ByteArrayOutputStream keyBytes = new ByteArrayOutputStream();
            ByteArrayOutputStream valueBytes = new ByteArrayOutputStream();

            kSerializer.serialize(key, keyBytes);
            vSerializer.serialize(value, valueBytes);

            if (ttl != -1L) {
                client.expire(keyBytes.toByteArray(), Math.toIntExact(ttl));
            }

            return client.set(keyBytes.toByteArray(), valueBytes.toByteArray());
        });
    }

    @Override
    public <K> Long delete(K key, Serializer<K> serializer) throws IOException {
        return execute(client -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            serializer.serialize(key, out);

            return client.del(out.toByteArray());
        });
    }

    @Override
    public <K, V> V get(K key, Serializer<K> kSerializer, Deserializer<V> vDeserializer) throws IOException {
        return execute(client -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            kSerializer.serialize(key, out);

            byte[] bytes = client.get(out.toByteArray());

            return vDeserializer.deserialize(bytes);
        });
    }

    private <T> T execute(RedisAction<T> action) throws IOException {
        try (Jedis client = pool.getConnection()) {
            return action.execute(client);
        }
    }
}
