package com.github.gr1f0n6x.service.redispool.util;

import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * Inspired by https://github.com/apache/nifi/blob/master/nifi-nar-bundles/nifi-redis-bundle/nifi-redis-extensions/src/main/java/org/apache/nifi/redis/util/RedisAction.java
 * @param <T>
 */
public interface RedisAction<T> {
    T execute(Jedis connection) throws IOException;
}
