package com.github.whiver.nifi.service;

import com.github.whiver.nifi.exception.MessageDecodingException;
import com.github.whiver.nifi.exception.MessageEncodingException;
import com.github.whiver.nifi.exception.SchemaLoadingException;
import com.github.whiver.nifi.exception.UnknownMessageTypeException;
import com.github.whiver.nifi.mapper.JSONMapper;
import com.github.whiver.nifi.parser.SchemaParser;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.io.*;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.sym.error;

public class ProtobufService {

    /**
     * Handle all the logic leading to the decoding of a Protobuf-encoded binary given a schema file path.
     * @param pathToSchema  Path to the .desc schema file on disk
     * @param messageType   Type of Protobuf Message
     * @param encodedData   Encoded data source
     * @return  A JSON representation of the data, contained in a Java String
     * @throws InvalidProtocolBufferException   Thrown when an error occurs during the encoding of the decoded data into JSON
     * @throws Descriptors.DescriptorValidationException    Thrown when the schema is invalid
     * @throws UnknownMessageTypeException  Thrown when the given message type is not contained in the schema
     * @throws MessageDecodingException Thrown when an error occurs during the binary decoding
     * @throws SchemaLoadingException   Thrown when an error occurs while reading the schema file
     */
    public static String decodeProtobuf(String pathToSchema, String messageType, InputStream encodedData) throws InvalidProtocolBufferException, Descriptors.DescriptorValidationException, UnknownMessageTypeException, MessageDecodingException, SchemaLoadingException {
        Descriptors.Descriptor descriptor;
        DynamicMessage message;

        try {
            descriptor = SchemaParser.parseProto(pathToSchema, messageType);
        } catch (IOException e) {
            throw new SchemaLoadingException(e);
        }

        if (descriptor == null) {
            throw new UnknownMessageTypeException(messageType, pathToSchema);
        }

        try {
            message = DynamicMessage.parseFrom(descriptor, encodedData);
        } catch (IOException e) {
            throw new MessageDecodingException(e);
        }

        return JSONMapper.toJSON(message);
    }


    /**
     * Handle all the logic leading to the encoding of a Protobuf-encoded binary given a schema file path and a JSON
     * data file.
     * @param pathToSchema  Path to the .desc schema file on disk
     * @param messageType   Type of Protobuf Message
     * @param jsonData      Data to encode, structured in a JSON format
     * @param binaryOutput  The stream where to output the encoded data
     * @throws Descriptors.DescriptorValidationException    Thrown when the schema is invalid
     * @throws IOException  Thrown when an errors occurs while parsing the JSON data
     * @throws MessageEncodingException Thrown when an error occurs during the binary encoding
     * @throws UnknownMessageTypeException  Thrown when the given message type is not contained in the schema
     * @throws SchemaLoadingException   Thrown when an error occurs while reading the schema file
     */
    public static void encodeProtobuf(String pathToSchema, String messageType, InputStream jsonData, OutputStream binaryOutput) throws Descriptors.DescriptorValidationException, IOException, MessageEncodingException, UnknownMessageTypeException, SchemaLoadingException {
        Descriptors.Descriptor descriptor;
        Message message;

        try {
            descriptor = SchemaParser.parseProto(pathToSchema, messageType);
        } catch (IOException e) {
            throw new SchemaLoadingException(e);
        }

        if (descriptor == null) {
            throw new UnknownMessageTypeException(messageType, pathToSchema);
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
}
