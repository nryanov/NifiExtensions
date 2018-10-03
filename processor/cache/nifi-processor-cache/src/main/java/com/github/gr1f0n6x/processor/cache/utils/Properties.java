package com.github.gr1f0n6x.processor.cache.utils;

import com.github.gr1f0n6x.service.common.*;
import com.github.gr1f0n6x.service.common.deserializer.JsonDeserializer;
import com.github.gr1f0n6x.service.common.deserializer.StringDeserializer;
import com.github.gr1f0n6x.service.common.serializer.JsonSerializer;
import com.github.gr1f0n6x.service.common.serializer.StringSerializer;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public class Properties {
    public static final AllowableValue STRING_SERIALIZER = new AllowableValue(StringSerializer.class.getName(),
            StringSerializer.class.getSimpleName(), "String serializer");

    public static final AllowableValue STRING_DESERIALIZER = new AllowableValue(StringDeserializer.class.getName(),
            StringDeserializer.class.getSimpleName(), "String deserializer");

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
            .allowableValues(STRING_SERIALIZER, JSON_SERIALIZER)
            .defaultValue(STRING_SERIALIZER.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESERIALIZER = new PropertyDescriptor.Builder()
            .name("Deserializer")
            .required(true)
            .allowableValues(STRING_DESERIALIZER, JSON_DESERIALIZER)
            .defaultValue(STRING_DESERIALIZER.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
}
