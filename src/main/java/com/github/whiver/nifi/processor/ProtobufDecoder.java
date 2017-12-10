/*
 * MIT License
 *
 * NiFi Protobuf Processor
 * Copyright (c) 2017 William Hiver
 * https://github.com/whiver/nifi-protobuf-processor
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.github.whiver.nifi.processor;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.whiver.nifi.exception.MessageDecodingException;
import com.github.whiver.nifi.exception.SchemaCompilationException;
import com.github.whiver.nifi.exception.SchemaLoadingException;
import com.github.whiver.nifi.exception.UnknownMessageTypeException;
import com.github.whiver.nifi.service.ProtobufService;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


@SideEffectFree
@Tags({"Protobuf", "decoder", "Google Protocol Buffer"})
@CapabilityDescription("Decode incoming data encoded using a Google Protocol Buffer Schema.")
public class ProtobufDecoder extends AbstractProcessor {
    /**
     * NiFi properties of the processor, that can be configured using the Web UI
     */
    private List<PropertyDescriptor> properties;

    /**
     * The different relationships of the processor
     */
    private Set<Relationship> relationships;

    /**
     * The compiled descriptor used to parse incoming binaries in case where the schema has been specified in the
     * processor level (protobuf.schema property)
     */
    private DynamicSchema schema;


    /*          PROPERTIES          */

    private static final PropertyDescriptor PROTOBUF_SCHEMA = new PropertyDescriptor.Builder()
            .name("protobuf.schemaPath")
            .displayName("Schema path")
            .required(false)
            .description("Path to the Protocol Buffers schema to use for decoding the data. If set, this schema will " +
                    "be used when the flowfile protobuf.schemaPath is missing.")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.createURLorFileValidator())
            .build();

    private static final PropertyDescriptor COMPILE_SCHEMA = new PropertyDescriptor.Builder()
            .name("protobuf.compileSchema")
            .displayName("Compile schema")
            .required(true)
            .defaultValue("false")
            .description("Set this property to true if the given schema file must be compiled using protoc before " +
                    "decoding the data. It is useful if the given schema file is in .proto format. Try to always use " +
                    "precompiled .desc schema whenever possible, since it is more performant.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    /*          RELATIONSHIPS           */

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
        properties.add(PROTOBUF_SCHEMA);
        properties.add(COMPILE_SCHEMA);
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

        String protobufSchema = flowfile.getAttribute(PROTOBUF_SCHEMA.getName());
        boolean compileSchema = processContext.getProperty(COMPILE_SCHEMA.getName()).asBoolean();
        String messageType = flowfile.getAttribute("protobuf.messageType");

        if (protobufSchema == null && this.schema == null) {
            getLogger().error("No schema path given, please fill in the "+ PROTOBUF_SCHEMA.getName() + " property.");
            session.transfer(flowfile, INVALID_SCHEMA);
        } else if (messageType == null) {
            getLogger().error("Unable to find the message type in protobuf.messageType, unable to decode data.");
            session.transfer(flowfile, ERROR);
        } else {

            // Write the results back out ot flow file
            FlowFile outputFlowfile = session.write(flowfile, (InputStream in, OutputStream out) -> {
                try {
                    if (protobufSchema == null) {
                        out.write(ProtobufService.decodeProtobuf(this.schema, compileSchema, messageType, in).getBytes());
                    } else {
                        out.write(ProtobufService.decodeProtobuf(protobufSchema, compileSchema, messageType, in).getBytes());
                    }
                } catch (DescriptorValidationException e) {
                    getLogger().error("Invalid schema file: " + e.getMessage(), e);
                    error.set(INVALID_SCHEMA);
                } catch (SchemaLoadingException | SchemaCompilationException e) {
                    getLogger().error(e.getMessage(), e);
                    error.set(INVALID_SCHEMA);
                } catch (UnknownMessageTypeException | MessageDecodingException e) {
                    getLogger().error(e.getMessage());
                    error.set(ERROR);
                } catch (InvalidProtocolBufferException e) {
                    getLogger().error("Unable to encode message into JSON: " + e.getMessage(), e);
                    error.set(ERROR);
                } catch (InterruptedException e) {
                    e.printStackTrace();
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

    /**
     * Compile the given schema file when the protobuf.schema property is given
     * @see AbstractProcessor
     */
    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);

        if (descriptor != PROTOBUF_SCHEMA) return;

        // If the property is unset, just delete the existing descriptor
        if (newValue == null || newValue.isEmpty()) {
            this.schema = null;
            return;
        }

        if (!newValue.equals(oldValue)) {
            // First drop the old schema
            this.schema = null;

            // Compile the new schema
            FileInputStream schemaFile;
            try {
                schemaFile = new FileInputStream(newValue);
            } catch (FileNotFoundException e) {
                getLogger().error("File " + newValue + " not found on the disk.", e);
                return;
            }

            try {
                this.schema = DynamicSchema.parseFrom(schemaFile);
            } catch (DescriptorValidationException e) {
                getLogger().error("Invalid schema file: " + e.getMessage(), e);
            } catch (IOException e) {
                getLogger().error("Unable to read file: " + e.getMessage(), e);
            }
        }
    }
}
