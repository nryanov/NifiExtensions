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
import java.util.stream.Collectors;

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
        props.add(VALUE_JOINER);
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
        final Cache cache = context.getProperty(CACHE).asControllerService(Cache.class);
        final String keyField = context.getProperty(KEY_FIELD).getValue();
        final Serializer<JsonNode> serializer = getSerializer(context);
        final Deserializer<JsonNode> deserializer = getDeserializer(context);
        final ValueJoiner<JsonNode, JsonNode, JsonNode> joiner = getValueJoiner(context);

        if (serializer == null || deserializer == null || joiner == null) {
            logger.error("Please, specify correct serializer/deserializer classes");
            session.transfer(flowFiles, FAILURE);
            return;
        }


        try {
            List<FlowFile> joined = flowFiles.stream().map(flowFile -> session.write(flowFile, (in, out) -> {
                try(BufferedInputStream bin = new BufferedInputStream(in);
                    BufferedOutputStream bout = new BufferedOutputStream(out)) {
                    byte[] bytes = new byte[bin.available()];
                    bin.read(bytes);
                    JsonNode node = deserializer.deserialize(bytes);

                    if (node.hasNonNull(keyField)) {
                        JsonNode value = cache.get(node.get(keyField), serializer, deserializer);
                        bout.write(serializer.serialize(joiner.join(node, value)));
                    } else {
                        logger.error("Flowfile {} does not has key field: {}", new Object[]{flowFile, keyField});
                    }
                }
            })).collect(Collectors.toList());

            session.transfer(joined, SUCCESS);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
            session.transfer(flowFiles, FAILURE);
        }
    }
}
