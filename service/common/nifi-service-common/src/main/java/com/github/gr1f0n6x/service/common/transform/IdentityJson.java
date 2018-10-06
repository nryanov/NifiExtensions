package com.github.gr1f0n6x.service.common.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.gr1f0n6x.service.common.ValueJoiner;

public class IdentityJson implements ValueJoiner<JsonNode, JsonNode, JsonNode> {
    @Override
    public JsonNode join(JsonNode jsonNode, JsonNode jsonNode2) {
        return jsonNode;
    }
}
