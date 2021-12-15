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

import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.avro.protobuf.ProtobufDatumWriter;
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

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Processor to convert protobuf format to avro
 */
@SideEffectFree
@SeeAlso(value = {EncodeProtobuf.class, DecodeProtobuf.class})
@CapabilityDescription("Decodes incoming data using a Google Protocol Buffer Schema and converts into Avro.")
public class ConvertProtobufToAvro extends AbstractProtobufProcessor {
    private DatumWriter<DynamicMessage> datumWriter;
    private DataFileWriter<DynamicMessage> dataFileWriter;

    static final PropertyDescriptor DEMARCATOR = new PropertyDescriptor.Builder()
            .name("demarcator")
            .displayName("Demarcator")
            .required(false)
            .description("This property is used to consume/produce messages separated by a demarcator")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor PROTOBUF_MESSAGE_TYPE = new PropertyDescriptor.Builder()
            .name("protobuf.messageType")
            .displayName("Message Type")
            .required(true)
            .description("Path to the Protocol Buffers message type to use to encode or decode the data. If set, this message type will " +
                    "be used when the flowfile protobuf.messageType is missing.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    protected Schema avroSchema;
    protected String messageType;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> list = new LinkedList<>(super.getSupportedPropertyDescriptors());
        list.add(DEMARCATOR);
        list.remove(AbstractProtobufProcessor.PROTOBUF_MESSAGE_TYPE);
        list.add(PROTOBUF_MESSAGE_TYPE);
        return list;
    }

    @OnScheduled
    @Override
    public void setUpSchema(ProcessContext processContext) {
        super.setUpSchema(processContext);
        messageType = processContext.getProperty(PROTOBUF_MESSAGE_TYPE).evaluateAttributeExpressions().getValue();
        avroSchema = ProtobufData.get().getSchema(this.schema.getMessageDescriptor(messageType));
        datumWriter = new ProtobufDatumWriter<>(avroSchema);
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
            outputFlowfile = processBatch(session, error, flowfile, demarcator);

            if (error.get() != null) {
                session.transfer(flowfile, error.get());
            } else {
                outputFlowfile = session.putAttribute(outputFlowfile, CoreAttributes.MIME_TYPE.key(), "application/avro-binary");
                session.transfer(outputFlowfile, SUCCESS);
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
                byte[] batch = new byte[(int) flowfile.getSize()];
                in.read(batch);
                in.close();
                List<byte[]> messages = ByteArrayUtil.split(batch, demarcatorBytes);
                dataFileWriter = new DataFileWriter<>(datumWriter);
                dataFileWriter.create(avroSchema, out);

                for (byte[] message : messages) {
                    DynamicMessage dynamicMessage = DynamicMessage.parseFrom(this.schema.getMessageDescriptor(messageType), message);
                    dataFileWriter.append(dynamicMessage);
                }
                dataFileWriter.close();

            } catch (Exception e) {
                getLogger().error("encountered error while processing batch:", e);
                error.set(ERROR);
            }
        });
    }
}
