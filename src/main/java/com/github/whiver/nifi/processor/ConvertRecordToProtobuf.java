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

import com.google.common.base.CaseFormat;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
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
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


/**
 * Processor to convert protobuf format to avro
 */
@SideEffectFree
@SeeAlso(value = {EncodeProtobuf.class, DecodeProtobuf.class, ConvertProtobufToAvro.class})
@CapabilityDescription("Decodes incoming data using RecordReader and converts it into a Google Protocol Buffer Schema.")
public class ConvertRecordToProtobuf extends AbstractProtobufProcessor {

    static final PropertyDescriptor PROTOBUF_MESSAGE_TYPE = new PropertyDescriptor.Builder()
            .name("protobuf.messageType")
            .displayName("Message Type")
            .required(true)
            .description("Path to the Protocol Buffers message type to use to encode or decode the data. If set, this message type will " +
                    "be used when the flowfile protobuf.messageType is missing.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
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

    protected String messageType;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> list = new LinkedList<>(super.getSupportedPropertyDescriptors());
        list.add(RECORD_READER);
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
                outputFlowfile = processBatch(processContext, session, error, flowfile, demarcator);
                outputFlowfile = session.putAttribute(outputFlowfile, CoreAttributes.MIME_TYPE.key(), "application/protobuf-binary");
                session.transfer(outputFlowfile, SUCCESS);
            } catch (RuntimeException e) {
                session.transfer(flowfile, error.get());
            }
        }
    }

    private FlowFile processBatch(ProcessContext context,
                                  ProcessSession session,
                                  AtomicReference<Relationship> error,
                                  FlowFile flowfile,
                                  String demarcator) {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        return session.write(flowfile, (in, out) -> {
            try (final RecordReader reader = readerFactory.createRecordReader(flowfile, in, getLogger())) {
                byte[] demarcatorBytes = demarcator.getBytes(StandardCharsets.UTF_8);
                Descriptors.Descriptor messageDescriptor = schema.getMessageDescriptor(messageType);
                Record record;
                boolean firstTime = true;
                while ((record = reader.nextRecord()) != null) {
                    if (!firstTime) { // add demarcator
                        out.write(demarcatorBytes);
                    }
                    firstTime = false;
                    getLogger().debug("converting record to dynamic message");
                    DynamicMessage dynamicMessage = recordDataToDynamicMessage(messageDescriptor, record);
                    out.write(dynamicMessage.toByteArray());
                }
            } catch (Exception e) {
                getLogger().error("encountered error while processing batch:", e);
                error.set(ERROR);
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Builds a protobuf DynamicMessage using a NiFi {@link Record}
     *
     * @param messageDescriptor the message descriptor
     * @param record            the record
     * @return a {@link DynamicMessage}
     */
    private DynamicMessage recordDataToDynamicMessage(Descriptors.Descriptor messageDescriptor, Record record) {
        DynamicMessage.Builder dynamicMessage = DynamicMessage
                .newBuilder(messageDescriptor);
        for (Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            String fieldName = field.getName();
            getLogger().debug("attempting to extract value for field {}", fieldName);
            AtomicReference<String> nameRepresentedByRecord = new AtomicReference<>();
            Object value = attemptToExtractWithDifferentNames((name) -> getValue(record, name, field), fieldName, nameRepresentedByRecord);
            if (value != null) {
                if (field.getType().equals(Descriptors.FieldDescriptor.Type.ENUM)) {
                    setFieldForEnum(dynamicMessage, field, fieldName, value);
                } else if (field.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE)) {
                    handleMessageType(record, dynamicMessage, field, fieldName, value, nameRepresentedByRecord);
                } else {
                    getLogger().debug("setting value {} for {}", value, fieldName);
                    dynamicMessage.setField(field, value);
                }
            }
        }
        return dynamicMessage.build();
    }

    private void handleMessageType(Record record, DynamicMessage.Builder dynamicMessage,
                                   Descriptors.FieldDescriptor field,
                                   String fieldName, Object value, AtomicReference<String> nameRepresentedByRecord) {
        // MESSAGE type is represented as MapRecord by NiFi
        if (value instanceof MapRecord) {
            dynamicMessage.setField(field, buildMessage((MapRecord) value, field));
        } else if (field.getMessageType().getFields().size() == 1) {
            // Since the NiFi record is not recognizing this field as a MESSAGE field but the flat out field,
            // and since the MESSAGE has only one field, it is possible to address this one field as the intended field
            // by the NiFi record.
            handleSingleFieldMessageField(record, dynamicMessage, field, nameRepresentedByRecord);
        } else {
            throw new IllegalStateException("Field " + fieldName + " should be of type MESSAGE but is not!");
        }
    }

    private void handleSingleFieldMessageField(Record record,
                                               DynamicMessage.Builder dynamicMessage,
                                               Descriptors.FieldDescriptor field,
                                               AtomicReference<String> nameRepresentedByRecord) {
        Object value;
        Descriptors.FieldDescriptor innerField = field.getMessageType().getFields().get(0);
        String innerFieldName = innerField.getName();
        Optional<RecordField> currentRecordField = record
                .getSchema()
                .getField(nameRepresentedByRecord.get());
        if (currentRecordField.isPresent()) {
            value = getValue(record, nameRepresentedByRecord.get(), innerField);
            HashMap<String, Object> map = new HashMap<>();
            map.put(innerFieldName, value);
            MapRecord mapRecord =
                    new MapRecord(new SimpleRecordSchema(Collections.singletonList(currentRecordField.get())),
                            map);
            dynamicMessage.setField(field, buildMessage(mapRecord, field));
        } else {
            throw new IllegalStateException("Field value was extracted but field doesn't exist!");
        }
    }

    private void setFieldForEnum(DynamicMessage.Builder dynamicMessage, Descriptors.FieldDescriptor field, String fieldName, Object value) {
        getLogger().debug("encountered enum for {}", fieldName);
        if (value instanceof Number) {
            getLogger().debug("value is number for {}, will convert to enum", fieldName);
            dynamicMessage.setField(field, field.getEnumType()
                    .findValueByNumber(((Number) value).intValue()));
        } else {
            dynamicMessage.setField(field, field.getEnumType()
                    .findValueByName(value.toString()));
        }
    }

    @Nonnull
    private Message buildMessage(MapRecord mapRecord, Descriptors.FieldDescriptor field) {
        DynamicMessage.Builder message = DynamicMessage.newBuilder(field.getMessageType());
        for (Descriptors.FieldDescriptor fieldDescriptor : field.getMessageType().getFields()) {
            AtomicReference<String> name = new AtomicReference<>();
            Object value = attemptToExtractWithDifferentNames(currentFieldName -> getValue(mapRecord, currentFieldName, fieldDescriptor),
                    fieldDescriptor.getName(), name);
            if (value != null) {
                if (fieldDescriptor.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.MESSAGE)) {
                    value = buildMessage((MapRecord) value, fieldDescriptor);
                }
                message.setField(fieldDescriptor, value);
            }
        }

        return message.build();
    }

    /**
     * Attempts to extract the value with different formats for the name(UpperCamelCase, lowerCamelCase, lower_underscore).
     * The function will try every format until it yields a non-null result.
     * Then, returns the value.
     *
     * @param extractValue the function to attempt to extract the value
     * @param fieldName    the original field name
     * @return the value(if successful, should not be null)
     */
    @Nullable
    private <T> T attemptToExtractWithDifferentNames(Function<String, T> extractValue, String fieldName) {
        return attemptToExtractWithDifferentNames(extractValue, fieldName, null);
    }

    /**
     * Attempts to extract the value with different formats for the name(UpperCamelCase, lowerCamelCase, lower_underscore).
     * The function will try every format until it yields a non-null result.
     * Then, returns the value.
     *
     * @param extractValue the function to attempt to extract the value
     * @param fieldName    the original field name
     * @param finalName    an atomic reference to hold the last field name attempt. If the function succeeded,
     *                     it will hold the name used to extract the value.
     * @return the value(if successful, should not be null)
     */
    @Nullable
    private <T> T attemptToExtractWithDifferentNames(Function<String, T> extractValue, String fieldName,
                                                     AtomicReference<String> finalName) {
        T value = extractValue.apply(fieldName);
        String originalFieldName = fieldName;

        if (value == null) {
            fieldName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, originalFieldName);
            value = extractValue.apply(fieldName);
        }

        if (value == null) {
            fieldName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, originalFieldName);
            value = extractValue.apply(fieldName);
        }

        if (value == null) {
            fieldName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, originalFieldName);
            value = extractValue.apply(fieldName);
        }

        if (value == null) {
            fieldName = CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, originalFieldName);
            value = extractValue.apply(fieldName);
        }


        if (value == null) {
            fieldName = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, originalFieldName);
            value = extractValue.apply(fieldName);
        }

        if (value == null) {
            fieldName = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, originalFieldName);
            value = extractValue.apply(fieldName);
        }

        if (finalName != null) {
            finalName.set(fieldName);
        }

        return value;
    }

    /**
     * Get value according to target type. It checks what kind of type it should be and converts accordingly.
     *
     * @param record          the nifi {@link Record}
     * @param fieldName       the field name
     * @param fieldDescriptor the field descriptor
     * @return the value
     */
    private Object getValue(Record record, String fieldName, Descriptors.FieldDescriptor fieldDescriptor) {
        switch (fieldDescriptor.getJavaType()) {
            case DOUBLE:
                return record.getAsDouble(fieldName);
            case INT:
                return record.getAsInt(fieldName);
            case FLOAT:
                return record.getAsFloat(fieldName);
            case LONG:
                return record.getAsLong(fieldName);
            case STRING:
                return record.getAsString(fieldName);
            case BOOLEAN:
                Optional<RecordField> recordField = record.getSchema().getField(fieldName);
                if (recordField.isPresent()) {
                    RecordFieldType fieldType = recordField.get().getDataType().getFieldType();
                    if (fieldType.equals(RecordFieldType.INT)) {
                        return record.getAsInt(fieldName) == 1;
                    } else if (fieldType.equals(RecordFieldType.STRING)) {
                        String value = record.getAsString(fieldName).toLowerCase(Locale.ROOT);
                        if (value.equals("true")) {
                            return true;
                        } else if (value.equals("false")) {
                            return false;
                        }
                    }
                }
                return record.getAsBoolean(fieldName);
            case ENUM:
                Optional<RecordField> field = record.getSchema().getField(fieldName);
                if (field.isPresent()) {
                    if (field.get().getDataType().getFieldType().equals(RecordFieldType.INT)) {
                        return record.getAsInt(fieldName);
                    }
                }
            default:
                return record.getValue(fieldName);
        }
    }
}
