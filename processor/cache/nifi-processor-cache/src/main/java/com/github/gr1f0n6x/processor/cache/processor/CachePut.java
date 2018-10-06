package com.github.gr1f0n6x.processor.cache.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.gr1f0n6x.service.common.*;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;


@Tags({"cache", "put", "json"})
public class CachePut extends CacheBase {
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
        final int batch = context.getProperty(BATCH_SIZE).asInteger();

        final List<FlowFile> flowFiles = session.get(batch);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();
        final Cache cache = context.getProperty(CACHE).asControllerService(ExpirableCache.class);
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
                    cache.set(node.get(keyField), node, serializer, serializer);
                    session.transfer(f, SUCCESS);
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
