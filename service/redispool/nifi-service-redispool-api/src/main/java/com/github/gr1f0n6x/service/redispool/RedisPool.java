package com.github.gr1f0n6x.service.redispool;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;

import java.io.IOException;

public interface RedisPool extends ControllerService {
    boolean exists(String key);

    void set(String key, FlowFile flowFile) throws IOException;

    void delete(String key);

    FlowFile get(String key) throws IOException, ClassNotFoundException;
}
