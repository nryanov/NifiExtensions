package com.github.gr1f0n6x.service.common;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

@Tags({"deserializer"})
@CapabilityDescription("")
public interface Deserializer<T> extends ControllerService {
    T deserialize(byte[] bytes);
}
