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
import com.github.whiver.nifi.exception.SchemaCompilationException;
import com.github.whiver.nifi.exception.SchemaLoadingException;
import com.github.whiver.nifi.parser.SchemaParser;
import com.google.protobuf.Descriptors;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static org.apache.nifi.components.Validator.VALID;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttributes({
        @ReadsAttribute(attribute = "protobuf.schemaPath", description = "will use this attribute as default for schema path."),
        @ReadsAttribute(attribute = "protobuf.messageType", description = "will use this attribute as default for message type.")
})
@Tags({"Protobuf", "decoder", "Google Protocol Buffer"})
public abstract class AbstractProtobufProcessor extends AbstractProcessor {
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
    protected DynamicSchema schema = null;

    /*          PROPERTIES          */

    static final PropertyDescriptor PROTOBUF_SCHEMA = new PropertyDescriptor.Builder()
            .name("protobuf.schemaPath")
            .displayName("Schema Path")
            .required(false)
            .description("Path to the Protocol Buffers schema to use to encode or decode the data. If set, this schema will " +
                    "be used when the flowfile protobuf.schemaPath is missing.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.createURLorFileValidator())
            .build();

    static final PropertyDescriptor PROTOBUF_MESSAGE_TYPE = new PropertyDescriptor.Builder()
            .name("protobuf.messageType")
            .displayName("Message Type")
            .required(false)
            .description("Path to the Protocol Buffers message type to use to encode or decode the data. If set, this message type will " +
                    "be used when the flowfile protobuf.messageType is missing.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(VALID)
            .build();

    static final PropertyDescriptor COMPILE_SCHEMA = new PropertyDescriptor.Builder()
            .name("protobuf.compileSchema")
            .displayName("Compile schema")
            .required(true)
            .defaultValue("false")
            .description("Set this property to true if the given schema file must be compiled using protoc before " +
                    "processing the data. It is useful if the given schema file is in .proto format. Try to always use " +
                    "precompiled .desc schema whenever possible, since it is more performant.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
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
    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PROTOBUF_SCHEMA);
        properties.add(PROTOBUF_MESSAGE_TYPE);
        properties.add(COMPILE_SCHEMA);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(INVALID_SCHEMA);
        relationships.add(ERROR);
        this.relationships = Collections.unmodifiableSet(relationships);
    }



    /**
     * Compile the given schema file when the protobuf.schemaPath property is given
     *
     */
    @OnScheduled
    public void setUpSchema(final ProcessContext processContext) {
        String protoSchemaPath = processContext.getProperty(PROTOBUF_SCHEMA).evaluateAttributeExpressions().getValue();
        // If the property is unset, just delete the existing descriptor
        if (protoSchemaPath == null || protoSchemaPath.isEmpty()) {
                this.schema = null;
            } else {
                try {
                    this.schema = SchemaParser.parseSchema(protoSchemaPath, processContext.getProperty(COMPILE_SCHEMA).evaluateAttributeExpressions().asBoolean());
                } catch (FileNotFoundException e) {
                    getLogger().error("File " + protoSchemaPath + " not found on the disk.", e);
                } catch (Descriptors.DescriptorValidationException e) {
                    getLogger().error("Invalid schema file: " + e.getMessage(), e);
                } catch (IOException e) {
                    getLogger().error("Unable to read file: " + e.getMessage(), e);
                } catch (SchemaLoadingException | SchemaCompilationException e) {
                    getLogger().error(e.getMessage(), e);
                } catch (InterruptedException e) {
                    getLogger().error("Unable to compile schema: " + e.getMessage(), e);
                }
            }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}
