package com.github.gr1f0n6x.processor.cache.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.gr1f0n6x.processor.cache.utils.Properties;
import com.github.gr1f0n6x.processor.cache.utils.Relationships;
import com.github.gr1f0n6x.service.common.*;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.github.gr1f0n6x.processor.cache.utils.Relationships.FAILURE;
import static com.github.gr1f0n6x.processor.cache.utils.Relationships.SUCCESS;

@Tags({"cache", "get", "json"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class CacheGet extends AbstractProcessor {
    private static List<PropertyDescriptor> descriptors;
    private static Set<Relationship> relationships;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(Properties.CACHE);
        props.add(Properties.KEY_FIELD);
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

    private Serializer<String> serializer;
    private Deserializer<String> deserializer;

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);
        serializer = new StringSerializer();
        deserializer = new StringDeserializer();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Cache cache = context.getProperty(Properties.CACHE).asControllerService(Cache.class);
        final String keyField = context.getProperty(Properties.KEY_FIELD).getValue();

        try {
            FlowFile result = session.write(flowFile, (in, out) -> {
                BufferedInputStream bin = new BufferedInputStream(in);
                BufferedOutputStream bout = new BufferedOutputStream(out);
                ObjectMapper mapper = new ObjectMapper();
                byte[] bytes = new byte[bin.available()];
                bin.read(bytes);
                JsonNode node = mapper.readTree(bytes);

                if (node.hasNonNull(keyField)) {
                    String value = cache.get(node.get(keyField).asText(), serializer, deserializer);
                    bout.write(value.getBytes(StandardCharsets.UTF_8));
                } else {
                    session.transfer(flowFile, FAILURE);
                }
            });

            session.transfer(result, SUCCESS);
        } catch (Exception e) {
            session.transfer(flowFile, FAILURE);
        }
    }
}
