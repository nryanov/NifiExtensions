package com.github.gr1f0n6x.service.redispool;

import com.github.gr1f0n6x.service.common.Deserializer;
import com.github.gr1f0n6x.service.common.Serializer;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;

import java.io.IOException;


/**
 * Interface provides some basic operation on redis cache
 */
public interface RedisCommands extends ControllerService {
    <K> boolean exists(K key, Serializer<K> serializer) throws IOException;

    <K,V> String set(K key, V value, Serializer<K> kSerializer, Serializer<V> vSerializer) throws IOException;

    <K> Long delete(K key, Serializer<K> serializer) throws IOException;

    <K, V> V get(K key, Serializer<K> kSerializer, Deserializer<V> vDeserializer) throws IOException, ClassNotFoundException;
}
