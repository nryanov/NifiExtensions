package com.github.gr1f0n6x.nifi.service.tarantool;

import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.tarantool.SocketChannelProvider;
import org.tarantool.TarantoolClient;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Tags({"tarantool", "cache"})
public class TarantoolConnectionService extends AbstractControllerService implements TarantoolConnection {
    public final static PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .required(true)
            .defaultValue("localhost")
            .name("Host")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public final static PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .required(true)
            .defaultValue("3301")
            .name("Port")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public final static PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .required(false)
            .name("Username")
            .build();

    public final static PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .required(false)
            .name("Password")
            .build();

    private static List<PropertyDescriptor> descriptors;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HOST);
        props.add(PORT);
        props.add(USERNAME);
        props.add(PASSWORD);

        descriptors = Collections.unmodifiableList(props);
    }

    private TarantoolClient client;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnEnabled
    public void enable(ConfigurationContext context) {
        String host = context.getProperty(HOST).getValue();
        int port = context.getProperty(PORT).asInteger();
        String username = context.getProperty(USERNAME).getValue();
        String password = context.getProperty(PASSWORD).getValue();

        TarantoolClientConfig tarantoolCfg = new TarantoolClientConfig();
        tarantoolCfg.username = username;
        tarantoolCfg.password = password;

        SocketChannelProvider provider = (i, throwable) -> {
            try {
                if (throwable != null) {
                    getLogger().error(throwable.getLocalizedMessage());
                }

                return SocketChannel.open(new InetSocketAddress(host, port));
            } catch (IOException e) {
                getLogger().error(e.getLocalizedMessage());
                throw new IllegalStateException(e);
            }
        };

        client = new TarantoolClientImpl(provider, tarantoolCfg);
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
            client.close();
        }

        client = null;
    }

    @Override
    public TarantoolClient getClient() {
        return client;
    }
}
