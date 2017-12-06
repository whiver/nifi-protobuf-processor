package com.github.whiver.nifi.mapper;


import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.io.Reader;

public class JSONMapper {
    /**
     * Format a Protocol Buffers Message to a JSON string
     * @param data  The Message to be formatted
     * @return  A JSON String representing the data
     * @throws InvalidProtocolBufferException   Thrown in case of invalid Message data
     */
    public static String toJSON(Message data) throws InvalidProtocolBufferException {
        JsonFormat.Printer printer = JsonFormat.printer();
        return printer.print(data);
    }

    /**
     * Extract data from a JSON String and use them to construct a Protocol Buffers Message.
     * @param jsonReader  A reader providing the JSON data to parse
     * @param builder   A Message builder to use to construct the resulting Message
     * @return  the constructed Message
     * @throws InvalidProtocolBufferException   Thrown in case of invalid Message data
     */
    public static Message fromJSON(Reader jsonReader, Message.Builder builder) throws IOException {
        JsonFormat.Parser parser = JsonFormat.parser();
        parser.merge(jsonReader, builder);
        return builder.build();
    }
}
