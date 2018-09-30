package com.github.gr1f0n6x.processor.cache.utils;

import com.github.gr1f0n6x.service.common.Cache;
import com.github.gr1f0n6x.service.common.Deserializer;
import com.github.gr1f0n6x.service.common.Serializer;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public class Properties {
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
            .identifiesControllerService(Serializer.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESERIALIZER = new PropertyDescriptor.Builder()
            .name("Deserializer")
            .required(true)
            .identifiesControllerService(Deserializer.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
}
