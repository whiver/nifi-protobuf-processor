package com.github.whiver.nifi.parser;

import com.google.protobuf.DescriptorProtos;

import java.io.FileInputStream;
import java.io.IOException;

public class SchemaParser {
    public static DescriptorProtos.FileDescriptorSet parseProto (String protoPath) throws IOException {
        FileInputStream inputStream = new FileInputStream(protoPath);
        return DescriptorProtos.FileDescriptorSet.parseFrom(inputStream);
    }
}
