package com.github.gr1f0n6x.service.memcached;

import net.spy.memcached.MemcachedClient;
import org.apache.nifi.controller.ControllerService;

public interface MemcachedConnection extends ControllerService {
    MemcachedClient getClient();
}
