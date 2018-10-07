package com.github.grf0n6x.service.memcached;

import com.github.gr1f0n6x.service.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedClient;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MemcachedConnectionService extends AbstractControllerService implements MemcachedConnection {
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
            .defaultValue("11211")
            .required(true)
            .description("Port listened by redis node")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static List<PropertyDescriptor> descriptors;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HOST);
        props.add(PORT);

        descriptors = Collections.unmodifiableList(props);
    }

    private MemcachedClient client;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }


    @Override
    public MemcachedClient getClient() {
        return client;
    }

    @OnEnabled
    public void enable(final ConfigurationContext context) throws IOException {
        String host = context.getProperty(HOST).getValue();
        int port = context.getProperty(PORT).asInteger();
        client = new MemcachedClient(new InetSocketAddress(host, port));
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
        if (client != null) {
            client.shutdown();
        }
        client = null;
    }
}
