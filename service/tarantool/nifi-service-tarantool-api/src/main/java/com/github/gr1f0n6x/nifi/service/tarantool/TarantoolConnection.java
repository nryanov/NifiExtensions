package com.github.gr1f0n6x.nifi.service.tarantool;

import org.apache.nifi.controller.ControllerService;
import org.tarantool.TarantoolClient;

public interface TarantoolConnection extends ControllerService {
    TarantoolClient getClient();
}
