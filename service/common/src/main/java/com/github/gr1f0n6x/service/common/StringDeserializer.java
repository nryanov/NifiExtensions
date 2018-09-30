package com.github.gr1f0n6x.service.common;

import java.nio.charset.StandardCharsets;

public class StringDeserializer implements Deserializer<String> {
    @Override
    public String deserialize(byte[] bytes) {
        return bytes != null ? new String(bytes, StandardCharsets.UTF_8) : null;
    }
}
