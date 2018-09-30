package com.github.gr1f0n6x.service.common;

import org.apache.nifi.controller.ControllerService;

public interface Deserializer<T> extends ControllerService {
    T deserialize(byte[] bytes);
}
