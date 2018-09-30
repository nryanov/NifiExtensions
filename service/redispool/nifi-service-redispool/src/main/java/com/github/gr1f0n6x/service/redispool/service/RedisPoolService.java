package com.github.gr1f0n6x.service.redispool.service;

import com.github.gr1f0n6x.service.redispool.RedisPool;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Tags({"redis", "pool", "cache"})
@CapabilityDescription("Provides a controller service to connect to redis node using connection pool.")
public class RedisPoolService extends AbstractControllerService implements RedisPool {
    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("Host")
            .defaultValue("localhost")
            .required(true)
            .description("Host where redis node is located")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .defaultValue("6379")
            .required(true)
            .description("Port listened by redis node")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_IDLE_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("Max idle connections")
            .defaultValue("1")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("Max total connections")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor MIN_IDLE_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("Min idle connections")
            .required(false)
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .required(false)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .required(false)
            .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("Database")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Timeout")
            .required(false)
            .defaultValue("2000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static List<PropertyDescriptor> descriptors;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HOST);
        props.add(PORT);
        props.add(MAX_IDLE_CONNECTIONS);
        props.add(MIN_IDLE_CONNECTIONS);
        props.add(MAX_TOTAL_CONNECTIONS);
        props.add(PASSWORD);
        props.add(USERNAME);
        props.add(DATABASE);
        props.add(TIMEOUT);

        descriptors = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    private JedisPool pool;
    private JedisPoolConfig config;

    @OnEnabled
    public void enable(final ConfigurationContext context) {
        String host = context.getProperty(HOST).getValue();
        String username = context.getProperty(USERNAME).getValue();
        String password = context.getProperty(PASSWORD).getValue();
        int port = context.getProperty(PORT).asInteger();
        int maxIdle = context.getProperty(MAX_IDLE_CONNECTIONS).asInteger();
        int minIdle = context.getProperty(MIN_IDLE_CONNECTIONS).asInteger();
        int maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).asInteger();
        int database = context.getProperty(DATABASE).asInteger();
        int timeout = context.getProperty(TIMEOUT).asInteger();

        config = new JedisPoolConfig();
        config.setMaxIdle(maxIdle);
        config.setMaxTotal(maxTotal);
        config.setMinIdle(minIdle);

        pool = new JedisPool(config, host, port, timeout, password, database, username);
    }

    @OnDisabled
    public void disable() {
        if (pool != null) {
            pool.close();
        }
    }

    @OnShutdown
    public void shutdownHook(final ConfigurationContext context) {
        pool.close();
    }

    @Override
    public Jedis getConnection() {
        return pool.getResource();
    }
}
