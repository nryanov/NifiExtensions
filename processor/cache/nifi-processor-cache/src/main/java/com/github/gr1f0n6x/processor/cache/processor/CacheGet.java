package com.github.gr1f0n6x.processor.cache.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.gr1f0n6x.service.common.*;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.*;
import java.util.*;

@Tags({"cache", "get", "json"})
public class CacheGet extends CacheBase {
    private static List<PropertyDescriptor> descriptors;
    private static Set<Relationship> relationships;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CACHE);
        props.add(KEY_FIELD);
        props.add(SERIALIZER);
        props.add(DESERIALIZER);
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
        final Cache cache = context.getProperty(CACHE).asControllerService(Cache.class);
        final String keyField = context.getProperty(KEY_FIELD).getValue();
        final Serializer<JsonNode> serializer = getSerializer(context.getProperty(SERIALIZER));
        final Deserializer<JsonNode> deserializer = getDeserializer(context.getProperty(DESERIALIZER));

        if (serializer == null || deserializer == null) {
            logger.error("Please, specify correct serializer/deserializer classes");
            session.transfer(flowFile, FAILURE);
            return;
        }

        try {
            FlowFile result = session.write(flowFile, (in, out) -> {
                try(BufferedInputStream bin = new BufferedInputStream(in);
                BufferedOutputStream bout = new BufferedOutputStream(out)) {
                    byte[] bytes = new byte[bin.available()];
                    bin.read(bytes);
                    JsonNode node = deserializer.deserialize(bytes);
                    FlowFile file = session.create(flowFile);

                    if (node.hasNonNull(keyField)) {
                        JsonNode value = cache.get(node.get(keyField), serializer, deserializer);
                        bout.write(serializer.serialize(value));
                    } else {
                        logger.error("Flowfile {} does not has key field: {}", new Object[]{flowFile, keyField});
                        session.transfer(file, FAILURE);
                    }
                }
            });

            session.transfer(result, SUCCESS);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
            session.transfer(flowFile, FAILURE);
        }
    }
}
