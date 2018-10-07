package com.github.gr1f0n6x.nifi.service.redispool;

import org.apache.nifi.controller.ControllerService;
import redis.clients.jedis.Jedis;


/**
 * Interface which allow to get redis connection.
 * Clients should control the closing of returned connection by themselves.
 */
public interface RedisPool extends ControllerService {
    Jedis getConnection();
}
