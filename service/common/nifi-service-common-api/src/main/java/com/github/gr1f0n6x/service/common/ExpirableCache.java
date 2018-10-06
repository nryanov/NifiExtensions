package com.github.gr1f0n6x.service.common;

import java.io.IOException;

public interface ExpirableCache extends Cache {
    <K, V> void set(K key, V value, int ttl, Serializer<K> kSerializer, Serializer<V> vSerializer) throws IOException;
}
