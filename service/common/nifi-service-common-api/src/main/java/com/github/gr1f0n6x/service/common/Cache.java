package com.github.gr1f0n6x.service.common;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

import java.io.IOException;

@Tags({"client", "cache"})
@CapabilityDescription("")
public interface Cache extends ControllerService {
    <K> boolean exists(K key, Serializer<K> serializer) throws IOException;

    <K,V> String set(K key, V value, Serializer<K> kSerializer, Serializer<V> vSerializer) throws IOException;

    <K> Long delete(K key, Serializer<K> serializer) throws IOException;

    <K, V> V get(K key, Serializer<K> kSerializer, Deserializer<V> vDeserializer) throws IOException;

}
