package com.github.gr1f0n6x.processor.cache.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.gr1f0n6x.service.common.*;
import com.github.gr1f0n6x.service.common.deserializer.JsonDeserializer;
import com.github.gr1f0n6x.service.common.serializer.JsonSerializer;
import com.github.gr1f0n6x.service.common.transform.SimpleJsonMerge;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public abstract class CacheBase extends AbstractProcessor {
    public static final AllowableValue SIMPLE_JSON_MERGE = new AllowableValue(SimpleJsonMerge.class.getName(),
            SimpleJsonMerge.class.getSimpleName(), "Simple Json merger");

    public static final AllowableValue JSON_SERIALIZER = new AllowableValue(JsonSerializer.class.getName(),
            JsonSerializer.class.getSimpleName(), "Json serializer");

    public static final AllowableValue JSON_DESERIALIZER = new AllowableValue(JsonDeserializer.class.getName(),
            JsonDeserializer.class.getSimpleName(), "Json deserializer");

    public static final PropertyDescriptor EXPIRABLE_CACHE = new PropertyDescriptor.Builder()
            .name("Expirable Cache provider")
            .required(true)
            .identifiesControllerService(ExpirableCache.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE = new PropertyDescriptor.Builder()
            .name("Cache provider")
            .required(true)
            .identifiesControllerService(Cache.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_FIELD = new PropertyDescriptor.Builder()
            .name("Key field")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor SERIALIZER = new PropertyDescriptor.Builder()
            .name("Serializer")
            .required(true)
            .allowableValues(JSON_SERIALIZER)
            .defaultValue(JSON_SERIALIZER.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor VALUE_JOINER = new PropertyDescriptor.Builder()
            .name("Value joiner")
            .required(true)
            .allowableValues(SIMPLE_JSON_MERGE)
            .defaultValue(SIMPLE_JSON_MERGE.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESERIALIZER = new PropertyDescriptor.Builder()
            .name("Deserializer")
            .required(true)
            .allowableValues(JSON_DESERIALIZER)
            .defaultValue(JSON_DESERIALIZER.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TTL = new PropertyDescriptor.Builder()
            .name("TTL")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(false)
            .defaultValue("0 secs")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch size")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .defaultValue("10")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .build();

    public static final Relationship EXIST = new Relationship.Builder()
            .name("exist")
            .build();

    public static final Relationship NOT_EXIST = new Relationship.Builder()
            .name("not exist")
            .build();

    protected Serializer<JsonNode> serializer;
    protected Deserializer<JsonNode> deserializer;
    protected ValueJoiner<JsonNode, JsonNode, JsonNode> joiner;

    @OnStopped
    public void close() {
        serializer = null;
        deserializer = null;
        joiner = null;
    }

    protected final Serializer<JsonNode> getSerializer(ProcessContext context) {
        if (serializer != null) {
            return serializer;
        } else if (JSON_SERIALIZER.getValue().equals(context.getProperty(SERIALIZER).getValue())) {
            return new JsonSerializer();
        }

        return null;
    }

    protected final Deserializer<JsonNode> getDeserializer(ProcessContext context) {
        if (deserializer != null) {
            return deserializer;
        } else if (JSON_DESERIALIZER.getValue().equals(context.getProperty(DESERIALIZER).getValue())) {
            return new JsonDeserializer();
        }

        return null;
    }

    protected final ValueJoiner<JsonNode, JsonNode, JsonNode> getValueJoiner(ProcessContext context) {
        if (joiner != null) {
            return joiner;
        } else if (SIMPLE_JSON_MERGE.getValue().equals(context.getProperty(VALUE_JOINER).getValue())) {
            return new SimpleJsonMerge();
        }

        return null;
    }
}
