package com.github.gr1f0n6x.nifi.service.tarantool;

import com.github.gr1f0n6x.nifi.service.common.Deserializer;
import com.github.gr1f0n6x.nifi.service.common.Serializer;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.tarantool.TarantoolClientOps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * All operations are executed on single space in format: [key, value] where
 * key and value - bytes array
 */
@Tags({"tarantool", "cache"})
public class TarantoolCommandService extends AbstractControllerService implements TarantoolCommand {
    public static final AllowableValue EQ = new AllowableValue("0", "EQ");
    public static final AllowableValue REQ = new AllowableValue("1", "REQ");
    public static final AllowableValue GT = new AllowableValue("2", "GT");
    public static final AllowableValue GE = new AllowableValue("3", "GE");
    public static final AllowableValue ALL = new AllowableValue("4", "ALL");
    public static final AllowableValue LT = new AllowableValue("5", "LT");
    public static final AllowableValue LE = new AllowableValue("6", "LE");

    public static final PropertyDescriptor CONNECTION = new PropertyDescriptor.Builder()
            .name("TarantoolConnection service")
            .required(true)
            .identifiesControllerService(TarantoolConnection.class)
            .build();

    public static final PropertyDescriptor SPACE_ID = new PropertyDescriptor.Builder()
            .name("Space id")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX_ID = new PropertyDescriptor.Builder()
            .name("Index id")
            .defaultValue("0")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor ITERATOR_TYPE = new PropertyDescriptor.Builder()
            .name("Iterator type")
            .required(false)
            .defaultValue("0")
            .allowableValues(EQ, REQ, GT, GE, ALL, LT, LE)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static List<PropertyDescriptor> descriptors;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CONNECTION);
        props.add(SPACE_ID);

        descriptors = Collections.unmodifiableList(props);
    }

    private TarantoolConnection connection;
    private int spaceId;
    private int indexId;
    private int iteratorType;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnEnabled
    public void enable(ConfigurationContext context) {
        connection = context.getProperty(CONNECTION).asControllerService(TarantoolConnection.class);
        spaceId = context.getProperty(SPACE_ID).asInteger();
        indexId = context.getProperty(INDEX_ID).asInteger();
        iteratorType = context.getProperty(ITERATOR_TYPE).asInteger();
    }

    @OnDisabled
    public void disable() {
        close();
    }

    @OnShutdown
    public void shutdown() {
        close();
    }

    private void close() {
        connection = null;
    }

    @Override
    public <K> boolean exists(K key, Serializer<K> serializer) throws IOException {
        return execute(ops -> {
            List<?> result = ops.select(spaceId, indexId, Arrays.asList(serializer.serialize(key)), 0, 1, iteratorType);
            return !result.isEmpty();
        });
    }

    @Override
    public <K, V> void set(K key, V value, Serializer<K> kSerializer, Serializer<V> vSerializer) throws IOException {
        execute(ops -> {
            ops.insert(spaceId, Arrays.asList(kSerializer.serialize(key), vSerializer.serialize(value)));
            return Void.TYPE;
        });
    }

    @Override
    public <K> void delete(K key, Serializer<K> serializer) throws IOException {
        execute(ops -> {
            ops.delete(spaceId, Arrays.asList(serializer.serialize(key)));
            return Void.TYPE;
        });
    }

    @Override
    public <K, V> V get(K key, Serializer<K> kSerializer, Deserializer<V> vDeserializer) throws IOException {
        /**
         * Result - List<List<?>>
         */
        return execute(ops -> {
            List<?> result = ops.select(spaceId, indexId, Arrays.asList(kSerializer.serialize(key)), 0, 1, iteratorType);
            List<?> row = (List<?>) result.get(0);
            V value = vDeserializer.deserialize((byte[]) row.get(1));

            return value;
        });
    }

    private <T> T execute(Action<T> action) throws IOException {
        return action.execute(connection.getClient().syncOps());
    }

    private interface Action<T> {
        T execute(TarantoolClientOps<Integer, List<?>, Object, List<?>> ops) throws IOException;
    }
}
