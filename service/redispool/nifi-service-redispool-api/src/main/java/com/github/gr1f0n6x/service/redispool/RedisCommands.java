package com.github.gr1f0n6x.service.redispool;

import com.github.gr1f0n6x.service.common.Cache;
import org.apache.nifi.controller.ControllerService;



/**
 * Interface provides some basic operation on redis cache
 */
public interface RedisCommands extends ControllerService, Cache {
}
