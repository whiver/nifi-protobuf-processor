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

package com.github.whiver.nifi.service;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.whiver.nifi.exception.*;
import com.github.whiver.nifi.mapper.JSONMapper;
import com.github.whiver.nifi.parser.SchemaParser;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.io.*;

public class ProtobufService {
    /**
     * Handle all the logic leading to the decoding of a Protobuf-encoded binary given a schema file path.
     * @param schema  Schema used to decode the binary data
     * @param compileSchema true if the given schema is still in raw .proto format
     * @param messageType   Type of Protobuf Message
     * @param encodedData   Encoded data source
     * @return  A JSON representation of the data, contained in a Java String
     * @throws InvalidProtocolBufferException   Thrown when an error occurs during the encoding of the decoded data into JSON
     * @throws Descriptors.DescriptorValidationException    Thrown when the schema is invalid
     * @throws UnknownMessageTypeException  Thrown when the given message type is not contained in the schema
     * @throws MessageDecodingException Thrown when an error occurs during the binary decoding
     * @throws SchemaLoadingException   Thrown when an error occurs while reading the schema file
     */
    public static String decodeProtobuf(DynamicSchema schema, boolean compileSchema, String messageType, InputStream encodedData) throws InvalidProtocolBufferException, Descriptors.DescriptorValidationException, UnknownMessageTypeException, MessageDecodingException, SchemaLoadingException {
        Descriptors.Descriptor descriptor;
        DynamicMessage message;

        descriptor = schema.getMessageDescriptor(messageType);

        if (descriptor == null) {
            throw new UnknownMessageTypeException(messageType);
        }

        try {
            message = DynamicMessage.parseFrom(descriptor, encodedData);
        } catch (IOException e) {
            throw new MessageDecodingException(e);
        }

        return JSONMapper.toJSON(message);
    }

    /**
     * Handle all the logic leading to the decoding of a Protobuf-encoded binary given a schema file path.
     * @param pathToSchema  Path to the .desc schema file on disk
     * @param compileSchema true if the given schema is still in raw .proto format
     * @param messageType   Type of Protobuf Message
     * @param encodedData   Encoded data source
     * @return  A JSON representation of the data, contained in a Java String
     * @throws InvalidProtocolBufferException   Thrown when an error occurs during the encoding of the decoded data into JSON
     * @throws Descriptors.DescriptorValidationException    Thrown when the schema is invalid
     * @throws UnknownMessageTypeException  Thrown when the given message type is not contained in the schema
     * @throws MessageDecodingException Thrown when an error occurs during the binary decoding
     * @throws SchemaLoadingException   Thrown when an error occurs while reading the schema file
     */
    public static String decodeProtobuf(String pathToSchema, boolean compileSchema, String messageType, InputStream encodedData) throws IOException, Descriptors.DescriptorValidationException, UnknownMessageTypeException, MessageDecodingException, SchemaLoadingException, InterruptedException, SchemaCompilationException {
        return decodeProtobuf(SchemaParser.parseSchema(pathToSchema, compileSchema), compileSchema, messageType, encodedData);
    }

    /**
     * Handle all the logic leading to the encoding of a Protobuf-encoded binary given a schema file path and a JSON
     * data file.
     * @param schema  Schema object to use to encode binary data
     * @param compileSchema true if the given schema is still in raw .proto format
     * @param messageType   Type of Protobuf Message
     * @param jsonData      Data to encode, structured in a JSON format
     * @param binaryOutput  The stream where to output the encoded data
     * @throws Descriptors.DescriptorValidationException    Thrown when the schema is invalid
     * @throws IOException  Thrown when an errors occurs while parsing the JSON data
     * @throws MessageEncodingException Thrown when an error occurs during the binary encoding
     * @throws UnknownMessageTypeException  Thrown when the given message type is not contained in the schema
     * @throws SchemaLoadingException   Thrown when an error occurs while reading the schema file
     */
    public static void encodeProtobuf(DynamicSchema schema, boolean compileSchema, String messageType, InputStream jsonData, OutputStream binaryOutput) throws Descriptors.DescriptorValidationException, IOException, MessageEncodingException, UnknownMessageTypeException, SchemaLoadingException {
        Descriptors.Descriptor descriptor;
        Message message;

        descriptor = schema.getMessageDescriptor(messageType);

        if (descriptor == null) {
            throw new UnknownMessageTypeException(messageType);
        }

        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        BufferedReader jsonReader = new BufferedReader(new InputStreamReader(jsonData));

        try {
            message = JSONMapper.fromJSON(new BufferedReader(jsonReader), builder);
        } catch (IOException e) {
            throw new IOException("Unable to parse JSON data: " + e.getMessage(), e);
        }

        try {
            message.writeTo(binaryOutput);
        } catch (IOException e) {
            throw new MessageEncodingException(e);
        }
    }

    /**
     * Handle all the logic leading to the encoding of a Protobuf-encoded binary given a schema file path and a JSON
     * data file.
     * @param pathToSchema  Path to the .desc schema file on disk
     * @param compileSchema true if the given schema is still in raw .proto format
     * @param messageType   Type of Protobuf Message
     * @param jsonData      Data to encode, structured in a JSON format
     * @param binaryOutput  The stream where to output the encoded data
     * @throws Descriptors.DescriptorValidationException    Thrown when the schema is invalid
     * @throws IOException  Thrown when an errors occurs while parsing the JSON data
     * @throws MessageEncodingException Thrown when an error occurs during the binary encoding
     * @throws UnknownMessageTypeException  Thrown when the given message type is not contained in the schema
     * @throws SchemaLoadingException   Thrown when an error occurs while reading the schema file
     */
    public static void encodeProtobuf(String pathToSchema, boolean compileSchema, String messageType, InputStream jsonData, OutputStream binaryOutput) throws Descriptors.DescriptorValidationException, IOException, MessageEncodingException, UnknownMessageTypeException, SchemaLoadingException, SchemaCompilationException, InterruptedException {
        encodeProtobuf(SchemaParser.parseSchema(pathToSchema, compileSchema), compileSchema, messageType, jsonData, binaryOutput);
    }
}
