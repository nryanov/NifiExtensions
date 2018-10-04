package com.github.gr1f0n6x.processor.cache.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.gr1f0n6x.service.common.Deserializer;
import com.github.gr1f0n6x.service.common.ExpirableCache;
import com.github.gr1f0n6x.service.common.Serializer;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.BufferedInputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CachePutExpirable extends CachePut {
    private static List<PropertyDescriptor> descriptors;
    private static Set<Relationship> relationships;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(EXPIRABLE_CACHE);
        props.add(KEY_FIELD);
        props.add(SERIALIZER);
        props.add(DESERIALIZER);
        props.add(TTL);
        descriptors = Collections.unmodifiableList(props);

        Set<Relationship> relations = new HashSet<>();
        relations.add(SUCCESS);
        relations.add(FAILURE);
        relationships = Collections.unmodifiableSet(relations);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final ExpirableCache cache = context.getProperty(CACHE).asControllerService(ExpirableCache.class);
        final String keyField = context.getProperty(KEY_FIELD).getValue();
        final Serializer<JsonNode> serializer = getSerializer(context.getProperty(SERIALIZER));
        final Deserializer<JsonNode> deserializer = getDeserializer(context.getProperty(DESERIALIZER));
        final int seconds = Math.toIntExact(context.getProperty(TTL).asTimePeriod(TimeUnit.SECONDS));

        if (serializer == null || deserializer == null) {
            logger.error("Please, specify correct serializer/deserializer classes");
            session.transfer(flowFile, FAILURE);
            return;
        }

        try {
            session.read(flowFile, in -> {
                try(BufferedInputStream bin = new BufferedInputStream(in)) {
                    byte[] bytes = new byte[bin.available()];
                    bin.read(bytes);
                    JsonNode node = deserializer.deserialize(bytes);
                    FlowFile copy = session.create(flowFile);

                    if (node.hasNonNull(keyField)) {
                        if (seconds <= 0) {
                            cache.set(node.get(keyField), node, serializer, serializer);
                        } else {
                            cache.set(node.get(keyField), node, seconds, serializer, serializer);
                        }
                        session.transfer(copy, SUCCESS);
                    } else {
                        logger.error("Flowfile {} does not has key field: {}", new Object[]{flowFile, keyField});
                        session.transfer(copy, FAILURE);
                    }
                }
            });
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
            session.transfer(flowFile, FAILURE);
        }
    }
}
