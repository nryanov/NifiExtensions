package com.github.gr1f0n6x.processor.cache.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.gr1f0n6x.processor.cache.utils.Properties;
import com.github.gr1f0n6x.processor.cache.utils.Relationships;
import com.github.gr1f0n6x.service.common.Cache;
import com.github.gr1f0n6x.service.common.Serializer;
import com.github.gr1f0n6x.service.common.serializer.StringSerializer;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.BufferedInputStream;
import java.util.*;

import static com.github.gr1f0n6x.processor.cache.utils.Relationships.FAILURE;
import static com.github.gr1f0n6x.processor.cache.utils.Relationships.SUCCESS;

@Tags({"cache", "delete", "json"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class CacheDelete extends AbstractProcessor {
    private static List<PropertyDescriptor> descriptors;
    private static Set<Relationship> relationships;
    private ObjectMapper mapper;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(Properties.CACHE);
        props.add(Properties.KEY_FIELD);
        props.add(Properties.SERIALIZER);
        descriptors = Collections.unmodifiableList(props);

        Set<Relationship> relations = new HashSet<>();
        relations.add(Relationships.SUCCESS);
        relations.add(Relationships.FAILURE);
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
    protected void init(ProcessorInitializationContext context) {
        super.init(context);
        mapper = new ObjectMapper();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final Cache cache = context.getProperty(Properties.CACHE).asControllerService(Cache.class);
        final String keyField = context.getProperty(Properties.KEY_FIELD).getValue();
        String serializerType = context.getProperty(Properties.SERIALIZER).getValue();
        Serializer<String> serializer;

        if (Properties.STRING_SERIALIZER.getValue().equals(serializerType)) {
            serializer = new StringSerializer();
        } else {
            logger.error("Serializer is incorrect");
            session.transfer(flowFile, FAILURE);
            return;
        }

        try {
            session.read(flowFile, in -> {
                try(BufferedInputStream bin = new BufferedInputStream(in)) {
                    byte[] bytes = new byte[bin.available()];
                    bin.read(bytes);
                    JsonNode node = mapper.readTree(bytes);

                    if (node.hasNonNull(keyField)) {
                        cache.delete(node.get(keyField).asText(), serializer);
                    } else {
                        logger.error("Flowfile {} does not have key field: {}", new Object[]{flowFile, keyField});
                        session.transfer(flowFile, FAILURE);
                    }
                }
            });

            session.transfer(flowFile, SUCCESS);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
            session.transfer(flowFile, FAILURE);
            return;
        }
    }
}
