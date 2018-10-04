package com.github.gr1f0n6x.service.redispool;

import com.github.gr1f0n6x.service.common.ExpirableCache;
import org.apache.nifi.controller.ControllerService;


/**
 * Interface provides some basic operation on redis cache
 */
public interface RedisCommand extends ControllerService, ExpirableCache {
}
