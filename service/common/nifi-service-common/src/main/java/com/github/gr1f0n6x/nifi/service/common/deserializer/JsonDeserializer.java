package com.github.gr1f0n6x.nifi.service.common.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.gr1f0n6x.nifi.service.common.Deserializer;

import java.io.IOException;

public class JsonDeserializer implements Deserializer<JsonNode> {
    private final static ObjectMapper mapper = new ObjectMapper();

    @Override
    public JsonNode deserialize(byte[] bytes) throws IOException {
        return mapper.readTree(bytes);
    }
}
