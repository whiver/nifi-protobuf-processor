package com.github.whiver.nifi.processor;

import com.google.protobuf.DescriptorProtos;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.xml.sax.helpers.ParserFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


@SideEffectFree
@Tags({"Protobuf", "decoder", "Google Protocol Buffer"})
@CapabilityDescription("Decode incoming data encoded using a Google Protocol Buffer Schema.")
public class ProtobufDecoderProcessor extends AbstractProcessor {
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    private static final PropertyDescriptor PROTOBUF_SCHEMA = new PropertyDescriptor.Builder()
            .name("protobuf.schema")
            .required(true)
            .description("Protocol Buffer schema used to decode and encode data.")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Success relationship")
            .build();

    private static final Relationship INVALID_SCHEMA = new Relationship.Builder()
            .name("Invalid schema")
            .description("Relationship used in case of invalid Protocol Buffer schema.")
            .build();

    private static final Relationship ERROR = new Relationship.Builder()
            .name("error")
            .description("Error relationship")
            .build();

    @Override
    public void init(final ProcessorInitializationContext context){
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PROTOBUF_SCHEMA);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(INVALID_SCHEMA);
        relationships.add(ERROR);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession session) throws ProcessException {
        final AtomicReference<String> value = new AtomicReference<>();
        final AtomicReference<Relationship> error = new AtomicReference<>();

        final FlowFile flowfile = session.get();

        String protobufSchema = flowfile.getAttribute("protobuf.schema");

        // To write the results back out ot flow file
        FlowFile outputFlowfile = session.write(flowfile, (InputStream in, OutputStream out) -> {
            FileDescriptorProto descriptorProto = FileDescriptorProto.parseFrom(new ByteArrayInputStream(protobufSchema.getBytes(StandardCharsets.UTF_8.name())));
            try {
                FileDescriptor.buildFrom(descriptorProto, null);
            } catch (DescriptorValidationException e) {
                getLogger().error(e.getMessage());
                e.printStackTrace();
                error.set(ERROR);
                return;
            }

            /*// Init the parser
            ParserFactory parserFactory = new ParserFactory();
            try {
                PropertyValue productIdProperty = processContext.getProperty(PRODUCT_ID);
                int productId = productIdProperty.isSet()
                        ? Byte.parseByte(productIdProperty.getValue())
                        : parserFactory.parseProductId(bitInputStream.get());

                getLogger().info("Instantiating parser for product ID '" + productId + "'");
                AbstractParser parser = parserFactory.getParser(productId, getLogger());
                String json = parser.toJson(deviceId, topic, bitInputStream.get());
                getLogger().info("Parsed values for product ID '" + productId + "': " + json);
                value.set(json);
            } catch (UnknownProductIdException e) {
                getLogger().error(e.getMessage());
                error[0] = 1;
                return;
            } catch (Exception e) {
                getLogger().error(e.getMessage());
                error[0] = 2;
                return;
            }*/

            out.write(value.get().getBytes());
        });

        if (error.get() != null) {
            session.transfer(flowfile, error.get());
        } else {
            session.transfer(outputFlowfile, SUCCESS);
        }
    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }
}
