package com.github.gr1f0n6x.processor.cache.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.gr1f0n6x.service.common.Cache;
import com.github.gr1f0n6x.service.common.Deserializer;
import com.github.gr1f0n6x.service.common.Serializer;
import com.github.gr1f0n6x.service.common.deserializer.JsonDeserializer;
import com.github.gr1f0n6x.service.common.serializer.JsonSerializer;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public abstract class CacheBase extends AbstractProcessor {
    public static final AllowableValue JSON_SERIALIZER = new AllowableValue(JsonSerializer.class.getName(),
            JsonSerializer.class.getSimpleName(), "Json serializer");

    public static final AllowableValue JSON_DESERIALIZER = new AllowableValue(JsonDeserializer.class.getName(),
            JsonDeserializer.class.getSimpleName(), "Json deserializer");

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

    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .build();

    protected final Serializer<JsonNode> getSerializer(PropertyValue value) {
        if (JSON_SERIALIZER.getValue().equals(value.getValue())) {
            return new JsonSerializer();
        }

        return null;
    }

    protected final Deserializer<JsonNode> getDeserializer(PropertyValue value) {
        if (JSON_DESERIALIZER.getValue().equals(value.getValue())) {
            return new JsonDeserializer();
        }

        return null;
    }
}
