package com.github.whiver.nifi.processor;


import com.github.whiver.nifi.exception.MessageEncodingException;
import com.github.whiver.nifi.exception.SchemaLoadingException;
import com.github.whiver.nifi.exception.UnknownMessageTypeException;
import com.github.whiver.nifi.mapper.JSONMapper;
import com.github.whiver.nifi.parser.SchemaParser;
import com.github.whiver.nifi.service.ProtobufService;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@Tags({"Protobuf", "decoder", "Google Protocol Buffer"})
@CapabilityDescription("Decode incoming data encoded using a Google Protocol Buffer Schema.")
public class ProtobufEncoder extends AbstractProcessor {
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    // TODO Implement a processor-wide schema if none is specified in the flowfile
    /*
    private static final PropertyDescriptor PROTOBUF_SCHEMA = new PropertyDescriptor.Builder()
            .name("protobuf.schema")
            .required(true)
            .description("Protocol Buffer schema used to decode and encode data.")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    */

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Success relationship")
            .build();

    static final Relationship INVALID_SCHEMA = new Relationship.Builder()
            .name("Invalid schema")
            .description("Relationship used in case of invalid Protocol Buffer schema.")
            .build();

    static final Relationship ERROR = new Relationship.Builder()
            .name("error")
            .description("Error relationship")
            .build();

    @Override
    public void init(final ProcessorInitializationContext context){
        List<PropertyDescriptor> properties = new ArrayList<>();
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(INVALID_SCHEMA);
        relationships.add(ERROR);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession session) throws ProcessException {
        final AtomicReference<Relationship> error = new AtomicReference<>();

        final FlowFile flowfile = session.get();

        String protobufSchema = flowfile.getAttribute("protobuf.schemaPath");
        String messageType = flowfile.getAttribute("protobuf.messageType");

        if (protobufSchema == null) {
            getLogger().error("No schema path given, please fill in the protobuf.schemaPath property.");
            session.transfer(flowfile, INVALID_SCHEMA);
        } else if (messageType == null) {
            getLogger().error("Unable to find the message type in protobuf.messageType, unable to decode data.");
            session.transfer(flowfile, ERROR);
        } else {

            // To write the results back out ot flow file
            FlowFile outputFlowfile = session.write(flowfile, (InputStream in, OutputStream out) -> {
                try {
                    ProtobufService.encodeProtobuf(protobufSchema, messageType, in, out);
                } catch (Descriptors.DescriptorValidationException e) {
                    getLogger().error("Invalid schema file: " + e.getMessage(), e);
                    error.set(INVALID_SCHEMA);
                } catch (SchemaLoadingException e) {
                    getLogger().error(e.getMessage(), e);
                    error.set(INVALID_SCHEMA);
                } catch (Exception e) {
                    getLogger().error(e.getMessage(), e);
                    error.set(ERROR);
                }
            });

            if (error.get() != null) {
                session.transfer(flowfile, error.get());
            } else {
                session.transfer(outputFlowfile, SUCCESS);
            }
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
