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
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Enum;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Processor to convert protobuf format to avro
 */
@SideEffectFree
@SeeAlso(value = {EncodeProtobuf.class, DecodeProtobuf.class, ConvertProtobufToAvro.class})
@CapabilityDescription("Decodes incoming data using Avro and converts it into a Google Protocol Buffer Schema.")
public class ConvertAvroToProtobuf extends AbstractProtobufProcessor {
    private DatumReader<GenericRecord> datumReader;

    static final PropertyDescriptor PROTOBUF_MESSAGE_TYPE = new PropertyDescriptor.Builder()
            .name("protobuf.messageType")
            .displayName("Message Type")
            .required(true)
            .description("Path to the Protocol Buffers message type to use to encode or decode the data. If set, this message type will " +
                    "be used when the flowfile protobuf.messageType is missing.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor DEMARCATOR = new PropertyDescriptor.Builder()
            .name("demarcator")
            .displayName("Demarcator")
            .required(true)
            .defaultValue("|||")
            .description("This property is used to produce/consume messages separated by a demarcator")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    protected Schema avroSchema;
    protected String messageType;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> list = new LinkedList<>(super.getSupportedPropertyDescriptors());
        list.remove(AbstractProtobufProcessor.PROTOBUF_MESSAGE_TYPE);
        list.remove(AbstractProtobufProcessor.DEMARCATOR);
        list.add(DEMARCATOR);
        list.add(PROTOBUF_MESSAGE_TYPE);
        return list;
    }

    @OnScheduled
    @Override
    public void setUpSchema(ProcessContext processContext) {
        super.setUpSchema(processContext);
        messageType = processContext.getProperty(PROTOBUF_MESSAGE_TYPE).evaluateAttributeExpressions().getValue();
        Descriptors.Descriptor messageDescriptor = this.schema.getMessageDescriptor(messageType);
        avroSchema = ProtobufData.get().getSchema(messageDescriptor);
        List<Schema.Field> avroSchemaFields = new LinkedList<>();
        for (Schema.Field field : avroSchema.getFields()) {
            if (field.schema().getType().equals(Schema.Type.ENUM)) {
                Schema newFieldSchema = Schema.createUnion(field.schema(), Schema.create(Schema.Type.INT),
                        Schema.create(Schema.Type.STRING));
                Schema.Field newField = new Schema.Field(field.name(), newFieldSchema);
                avroSchemaFields.add(newField);
            }
        }
        avroSchema = Schema.createRecord(avroSchema.getName(), avroSchema.getDoc(), avroSchema.getNamespace(), avroSchema.isError(), avroSchemaFields);
        datumReader = new GenericDatumReader<>(avroSchema);
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession session) throws ProcessException {
        final AtomicReference<Relationship> error = new AtomicReference<>();

        FlowFile flowfile = session.get();

        if (flowfile == null) {
            return;
        }

        String demarcator = processContext.getProperty(DEMARCATOR).evaluateAttributeExpressions(flowfile).getValue();
        String protobufSchema = flowfile.getAttribute(PROTOBUF_SCHEMA.getName());

        if (protobufSchema == null && this.schema == null) {
            getLogger().error("No schema path given, please fill in the " + PROTOBUF_SCHEMA.getName() +
                    " property, either at processor or flowfile level..");
            session.transfer(flowfile, INVALID_SCHEMA);
        } else {

            // Write the results back out ot flow file
            FlowFile outputFlowfile;
            try {
                outputFlowfile = processBatch(session, error, flowfile, demarcator);
                outputFlowfile = session.putAttribute(outputFlowfile, CoreAttributes.MIME_TYPE.key(), "application/protobuf-binary");
                session.transfer(outputFlowfile, SUCCESS);
            } catch (RuntimeException e) {
                session.transfer(flowfile, error.get());
            }
        }
    }

    private FlowFile processBatch(ProcessSession session,
                                  AtomicReference<Relationship> error,
                                  FlowFile flowfile,
                                  String demarcator) {
        return session.write(flowfile, (in, out) -> {
            try {
                byte[] demarcatorBytes = demarcator.getBytes(StandardCharsets.UTF_8);
                DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader);
                Descriptors.Descriptor messageDescriptor = schema.getMessageDescriptor(messageType);
                while (dataFileReader.hasNext()) {
                    GenericRecord record = dataFileReader.next();
                    DynamicMessage dynamicMessage = genericDataToDynamicMessage(messageDescriptor, record);
                    out.write(dynamicMessage.toByteArray());

                    if (dataFileReader.hasNext()) {
                        out.write(demarcatorBytes);
                    }
                }
            } catch (Exception e) {
                getLogger().error("encountered error while processing batch:", e);
                error.set(ERROR);
                throw new RuntimeException(e);
            }
        });
    }

    private DynamicMessage genericDataToDynamicMessage(Descriptors.Descriptor messageDescriptor, GenericRecord record) {
        DynamicMessage.Builder dynamicMessage = DynamicMessage
                .newBuilder(messageDescriptor);
        for (Schema.Field field : record.getSchema().getFields()) {
            if (record.hasField(field.name())) {
                Descriptors.FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(field.name());
                Object value = record.get(field.name());
                if (value != null) {
                    if (fieldDescriptor.getType().equals(Descriptors.FieldDescriptor.Type.ENUM)) {
                        if (value instanceof Number) {
                            dynamicMessage.setField(fieldDescriptor, fieldDescriptor.getEnumType()
                                    .findValueByNumber(((Number) value).intValue()));
                        } else {
                            dynamicMessage.setField(fieldDescriptor, fieldDescriptor.getEnumType()
                                    .findValueByName(value.toString()));
                        }
                    } else {
                        dynamicMessage.setField(fieldDescriptor, value);
                    }
                }
            }
        }
        return dynamicMessage.build();
    }
}
