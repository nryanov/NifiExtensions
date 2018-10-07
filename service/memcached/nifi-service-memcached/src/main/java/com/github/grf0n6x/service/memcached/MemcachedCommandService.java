package com.github.grf0n6x.service.memcached;

import com.github.gr1f0n6x.service.common.Deserializer;
import com.github.gr1f0n6x.service.common.Serializer;
import com.github.gr1f0n6x.service.memcached.MemcachedCommand;
import com.github.gr1f0n6x.service.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedClient;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Because of using the single connection, we have to use synchronized methods
 */
@Tags({"memcached", "cache", "ttl"})
public class MemcachedCommandService extends AbstractControllerService implements MemcachedCommand {
    public static final PropertyDescriptor MEMCACHED_CONNECTION = new PropertyDescriptor.Builder()
            .name("Memcached connection")
            .identifiesControllerService(MemcachedConnection.class)
            .required(true)
            .build();

    private static List<PropertyDescriptor> descriptors;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(MEMCACHED_CONNECTION);

        descriptors = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    private volatile MemcachedConnection connection;

    @OnEnabled
    public void enable(final ConfigurationContext context) {
        connection = context.getProperty(MEMCACHED_CONNECTION).asControllerService(MemcachedConnection.class);
    }

    @OnDisabled
    public void disable() {
        connection = null;
    }

    @Override
    public synchronized  <K, V> void set(K key, V value, int ttl, Serializer<K> kSerializer, Serializer<V> vSerializer) throws IOException {
        execute(client -> {
            client.set(new String(kSerializer.serialize(key), StandardCharsets.UTF_8), ttl, vSerializer.serialize(value));
            return Void.TYPE;
        });
    }

    @Override
    public synchronized <K> boolean exists(K key, Serializer<K> serializer) throws IOException {
        return execute(client -> client.get(new String(serializer.serialize(key), StandardCharsets.UTF_8))) != null;
    }

    @Override
    public synchronized <K, V> void set(K key, V value, Serializer<K> kSerializer, Serializer<V> vSerializer) throws IOException {
        execute(client -> {
            client.set(new String(kSerializer.serialize(key), StandardCharsets.UTF_8), 0, vSerializer.serialize(value));
            return Void.TYPE;
        });
    }

    @Override
    public synchronized <K> void delete(K key, Serializer<K> serializer) throws IOException {
        execute(client -> {
            client.delete(new String(serializer.serialize(key), StandardCharsets.UTF_8));
            return Void.TYPE;
        });
    }

    @Override
    public synchronized <K, V> V get(K key, Serializer<K> kSerializer, Deserializer<V> vDeserializer) throws IOException {
        return execute(client -> vDeserializer.deserialize((byte[]) client.get(new String(kSerializer.serialize(key), StandardCharsets.UTF_8))));
    }

    private <T> T execute(Action<T> action) {
        try {
            return action.execyte(connection.getClient());
        } catch (IOException e) {
            getLogger().error(e.getLocalizedMessage());
            return null;
        }
    }

    private interface Action<T> {
        T execyte(MemcachedClient client) throws IOException;
    }
}
