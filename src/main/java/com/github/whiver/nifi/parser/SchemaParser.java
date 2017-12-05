package com.github.whiver.nifi.parser;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.protobuf.Descriptors;

import java.io.FileInputStream;
import java.io.IOException;

public class SchemaParser {
    public static Descriptors.Descriptor parseProto (String protoPath, String messageType) throws IOException, Descriptors.DescriptorValidationException {
        FileInputStream schemaFile = new FileInputStream(protoPath);
        DynamicSchema schema = DynamicSchema.parseFrom(schemaFile);
        return schema.getMessageDescriptor(messageType);
    }
}
