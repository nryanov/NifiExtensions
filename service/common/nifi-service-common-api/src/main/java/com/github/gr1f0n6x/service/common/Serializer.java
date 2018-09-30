package com.github.gr1f0n6x.service.common;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

import java.io.IOException;
import java.io.OutputStream;

@Tags({"serializer"})
@CapabilityDescription("")
public interface Serializer<T> extends ControllerService {
    void serialize(T o, OutputStream out) throws IOException;
}
