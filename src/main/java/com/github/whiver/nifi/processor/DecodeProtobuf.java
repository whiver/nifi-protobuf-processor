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
import com.github.whiver.nifi.exception.MessageDecodingException;
import com.github.whiver.nifi.exception.SchemaCompilationException;
import com.github.whiver.nifi.exception.SchemaLoadingException;
import com.github.whiver.nifi.exception.UnknownMessageTypeException;
import com.github.whiver.nifi.service.ProtobufService;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


@SideEffectFree
@SeeAlso(EncodeProtobuf.class)
@CapabilityDescription("Decodes incoming data using a Google Protocol Buffer Schema.")
public class DecodeProtobuf extends AbstractProtobufProcessor {
    static final PropertyDescriptor DEMARCATOR = new PropertyDescriptor.Builder()
            .name("demarcator")
            .displayName("Demarcator")
            .required(false)
            .description("This property is used to consume messages separated by a demarcator")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor PRESERVE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("preserve_field_names")
            .displayName("Preserve Original Proto Field Names")
            .required(true)
            .defaultValue("false")
            .description("Whether to preserve original proto field names. If not, fields like field_name would be converted to camel case fieldName.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> list = new LinkedList<>(super.getSupportedPropertyDescriptors());
        list.add(DEMARCATOR);
        list.add(PRESERVE_FIELD_NAMES);
        return list;
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

        String messageTypeValue = flowfile.getAttribute(PROTOBUF_MESSAGE_TYPE.getName());
        final String messageType = messageTypeValue != null ? messageTypeValue : processContext.getProperty(PROTOBUF_MESSAGE_TYPE).evaluateAttributeExpressions(flowfile).getValue();
        final boolean preserveFieldNames = processContext.getProperty(PRESERVE_FIELD_NAMES).evaluateAttributeExpressions(flowfile).asBoolean();

        if (protobufSchema == null && this.schema == null) {
            getLogger().error("No schema path given, please fill in the " + PROTOBUF_SCHEMA.getName() +
                    " property, either at processor or flowfile level..");
            session.transfer(flowfile, INVALID_SCHEMA);
        } else if (messageType == null) {
            getLogger().error("Unable to find the message type in protobuf.messageType, unable to decode data.");
            session.transfer(flowfile, ERROR);
        } else {

            // Write the results back out ot flow file
            FlowFile outputFlowfile;

            if (demarcator == null || demarcator.isEmpty()) {
                outputFlowfile = processSingleFlowFile(session, error, flowfile, messageType, preserveFieldNames);
            } else {
                outputFlowfile = processBatch(session, error, flowfile, messageType, demarcator, preserveFieldNames);
            }

            if (error.get() != null) {
                session.transfer(flowfile, error.get());
            } else {
                outputFlowfile = session.putAttribute(outputFlowfile, CoreAttributes.MIME_TYPE.key(), "application/json");
                session.transfer(outputFlowfile, SUCCESS);
            }
        }
    }

    private FlowFile processBatch(ProcessSession session,
                                  AtomicReference<Relationship> error,
                                  FlowFile flowfile,
                                  String messageType,
                                  String demarcator,
                                  boolean preserveFieldNames) {
        return session.write(flowfile, (in, out) -> {
            try {
                byte[] demarcatorBytes = demarcator.getBytes(StandardCharsets.UTF_8);
                byte[] batch = new byte[(int) flowfile.getSize()];
                in.read(batch);
                in.close();
                List<byte[]> messages = ByteArrayUtil.split(batch, demarcatorBytes);
                out.write('[');
                Iterator<byte[]> iterator = messages.iterator();
                while (iterator.hasNext()) {
                    processMessage(messageType, preserveFieldNames, iterator.next(), out);
                    if (iterator.hasNext()) {
                        out.write(',');
                    }
                }
                out.write(']');
            } catch (Exception e) {
                getLogger().error("encountered error while processing batch:", e);
                error.set(ERROR);
            }
        });
    }

    private FlowFile processSingleFlowFile(ProcessSession session,
                                           AtomicReference<Relationship> error,
                                           FlowFile flowfile,
                                           String messageType,
                                           boolean preserveFieldNames) {
        return session.write(flowfile, (InputStream in, OutputStream out) -> {
            try {
                processMessage(messageType, preserveFieldNames, in, out);
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
    }

    private void processMessage(String messageType, boolean preserveFieldNames, InputStream in, OutputStream out) throws IOException, DescriptorValidationException, UnknownMessageTypeException, MessageDecodingException, SchemaLoadingException, InterruptedException, SchemaCompilationException {
        out.write(ProtobufService.decodeProtobuf(this.schema, messageType, in, preserveFieldNames).getBytes());
    }

    private void processMessage(String messageType, boolean preserveFieldNames, byte[] in, OutputStream out) throws IOException, DescriptorValidationException, UnknownMessageTypeException, MessageDecodingException, SchemaLoadingException, InterruptedException, SchemaCompilationException {
        out.write(ProtobufService.decodeProtobuf(this.schema, messageType, in, preserveFieldNames).getBytes());
    }
}
