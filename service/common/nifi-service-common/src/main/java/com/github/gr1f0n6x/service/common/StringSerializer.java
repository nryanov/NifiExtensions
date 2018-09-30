package com.github.gr1f0n6x.service.common;

import org.apache.nifi.controller.AbstractControllerService;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class StringSerializer extends AbstractControllerService implements Serializer<String> {
    @Override
    public void serialize(String o, OutputStream out) throws IOException {
        out.write(o.getBytes(StandardCharsets.UTF_8));
    }
}
