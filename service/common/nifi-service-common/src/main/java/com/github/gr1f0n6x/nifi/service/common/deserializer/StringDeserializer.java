package com.github.gr1f0n6x.nifi.service.common.deserializer;


import com.github.gr1f0n6x.nifi.service.common.Deserializer;

import java.nio.charset.StandardCharsets;

public class StringDeserializer implements Deserializer<String> {
    @Override
    public String deserialize(byte[] bytes) {
        return bytes != null ? new String(bytes, StandardCharsets.UTF_8) : null;
    }
}
