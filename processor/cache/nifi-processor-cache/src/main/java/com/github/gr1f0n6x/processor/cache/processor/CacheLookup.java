package com.github.gr1f0n6x.processor.cache.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.gr1f0n6x.service.common.Cache;
import com.github.gr1f0n6x.service.common.Deserializer;
import com.github.gr1f0n6x.service.common.Serializer;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;


@Tags({"cache", "lookup", "json"})
public class CacheLookup extends CacheBase {
    private static List<PropertyDescriptor> descriptors;
    private static Set<Relationship> relationships;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CACHE);
        props.add(KEY_FIELD);
        props.add(SERIALIZER);
        props.add(DESERIALIZER);
        props.add(BATCH_SIZE);
        descriptors = Collections.unmodifiableList(props);

        Set<Relationship> relations = new HashSet<>();
        relations.add(EXIST);
        relations.add(NOT_EXIST);
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
        final int batch = context.getProperty(BATCH_SIZE).asInteger();

        final List<FlowFile> flowFiles = session.get(batch);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();
        final Cache cache = context.getProperty(CACHE).asControllerService(Cache.class);
        final String keyField = context.getProperty(KEY_FIELD).getValue();
        final Serializer<JsonNode> serializer = getSerializer(context);
        final Deserializer<JsonNode> deserializer = getDeserializer(context);

        if (serializer == null || deserializer == null) {
            logger.error("Please, specify correct serializer/deserializer classes");
            session.transfer(flowFiles, FAILURE);
            return;
        }

        flowFiles.forEach(f -> {
            try {
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                session.exportTo(f, bout);
                bout.close();

                JsonNode node = deserializer.deserialize(bout.toByteArray());
                if (node.hasNonNull(keyField)) {
                    if (cache.exists(node.get(keyField), serializer)) {
                        session.transfer(f, EXIST);
                    } else {
                        session.transfer(f, NOT_EXIST);
                    }
                } else {
                    logger.error("Flowfile {} does not has key field: {}", new Object[]{f, keyField});
                    session.transfer(f, FAILURE);
                }
            } catch (IOException e) {
                logger.error(e.getLocalizedMessage());
                session.transfer(f, FAILURE);
            }
        });
    }
}
