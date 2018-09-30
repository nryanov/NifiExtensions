package com.github.gr1f0n6x.service.redispool;

import com.github.gr1f0n6x.service.common.Cache;
import com.github.gr1f0n6x.service.common.Deserializer;
import com.github.gr1f0n6x.service.common.Serializer;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;

import java.io.IOException;


/**
 * Interface provides some basic operation on redis cache
 */
public interface RedisCommands extends ControllerService, Cache {
}
