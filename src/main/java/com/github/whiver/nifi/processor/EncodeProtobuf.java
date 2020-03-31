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


import com.github.whiver.nifi.exception.SchemaCompilationException;
import com.github.whiver.nifi.exception.SchemaLoadingException;
import com.github.whiver.nifi.service.ProtobufService;
import com.google.protobuf.Descriptors;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@SeeAlso(DecodeProtobuf.class)
@CapabilityDescription("Decodes incoming data using a Google Protocol Buffer Schema.")
public class EncodeProtobuf extends AbstractProtobufProcessor {

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession session) throws ProcessException {
        final AtomicReference<Relationship> error = new AtomicReference<>();

        final FlowFile flowfile = session.get();

        // We check if the protobuf.schemaPath property is defined in the flowfile
        String protobufSchema = flowfile.getAttribute(PROTOBUF_SCHEMA.getName());

        boolean compileSchema = processContext.getProperty(COMPILE_SCHEMA).evaluateAttributeExpressions().asBoolean();

        String messageTypeValue = flowfile.getAttribute(PROTOBUF_MESSAGE_TYPE.getName());
        final String messageType = messageTypeValue != null ? messageTypeValue : processContext.getProperty(PROTOBUF_MESSAGE_TYPE).evaluateAttributeExpressions(flowfile).getValue();


        if (protobufSchema == null && this.schema == null) {
            getLogger().error("No schema path given, please fill in the " + PROTOBUF_SCHEMA.getName() +
                    " property, either at processor or flowfile level..");
            session.transfer(flowfile, INVALID_SCHEMA);
        } else if (messageType == null) {
            getLogger().error("Unable to find the message type in protobuf.messageType, unable to decode data.");
            session.transfer(flowfile, ERROR);
        } else {

            FlowFile outputFlowfile = session.write(flowfile, (InputStream in, OutputStream out) -> {
                try {
                    // If the protobufSchema property is defined, we use the schema from the flowfile instead of the
                    // processor-wide one
                    if (protobufSchema == null) {
                        ProtobufService.encodeProtobuf(this.schema, messageType, in, out);
                    } else {
                        ProtobufService.encodeProtobuf(protobufSchema, compileSchema, messageType, in, out);
                    }
                } catch (Descriptors.DescriptorValidationException e) {
                    getLogger().error("Invalid schema file: " + e.getMessage(), e);
                    error.set(INVALID_SCHEMA);
                } catch (SchemaLoadingException | SchemaCompilationException e) {
                    getLogger().error(e.getMessage(), e);
                    error.set(INVALID_SCHEMA);
                } catch (InterruptedException e) {
                    getLogger().error("Unable to compile schema: " + e.getMessage(), e);
                    error.set(ERROR);
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
}
