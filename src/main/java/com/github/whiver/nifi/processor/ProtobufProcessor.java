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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

public abstract class ProtobufProcessor extends AbstractProcessor {
    /**
     * NiFi properties of the processor, that can be configured using the Web UI
     */
    protected List<PropertyDescriptor> properties;

    /**
     * The different relationships of the processor
     */
    protected Set<Relationship> relationships;

    /**
     * The compiled descriptor used to parse incoming binaries in case where the schema has been specified in the
     * processor level (protobuf.schema property)
     */
    protected DynamicSchema schema;


    /*          PROPERTIES          */

    protected static final PropertyDescriptor PROTOBUF_SCHEMA = new PropertyDescriptor.Builder()
            .name("protobuf.schemaPath")
            .displayName("Schema path")
            .required(false)
            .description("Path to the Protocol Buffers schema to use to encode or decode the data. If set, this schema will " +
                    "be used when the flowfile protobuf.schemaPath is missing.")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.createURLorFileValidator())
            .build();

    protected static final PropertyDescriptor COMPILE_SCHEMA = new PropertyDescriptor.Builder()
            .name("protobuf.compileSchema")
            .displayName("Compile schema")
            .required(true)
            .defaultValue("false")
            .description("Set this property to true if the given schema file must be compiled using protoc before " +
                    "processing the data. It is useful if the given schema file is in .proto format. Try to always use " +
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
}
