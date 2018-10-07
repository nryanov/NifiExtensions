package com.github.gr1f0n6x.nifi.service.common.serializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.gr1f0n6x.nifi.service.common.Serializer;

import java.io.IOException;

public class JsonSerializer implements Serializer<JsonNode> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(JsonNode o) throws IOException {
        return mapper.writeValueAsBytes(o);
    }
}
