package com.github.whiver.nifi.mapper;


import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

public class JSONMapper {
    public static String toJSON(Message data) throws InvalidProtocolBufferException {
        JsonFormat.Printer printer = JsonFormat.printer();
        return printer.print(data);
    }
}
