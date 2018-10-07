package com.github.gr1f0n6x.nifi.service.common.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.gr1f0n6x.nifi.service.common.ValueJoiner;

public class SimpleJsonMerge implements ValueJoiner<JsonNode, JsonNode, JsonNode> {
    private final static ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
    }

    @Override
    public JsonNode join(JsonNode jsonNode, JsonNode jsonNode2) {
        ObjectNode node = MAPPER.createObjectNode();

        node.set("left", jsonNode);
        node.set("right", jsonNode2);

        return node;
    }
}
