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
     * @param preserveFieldNames whether to preserve original field names
     * @return  A JSON String representing the data
     * @throws InvalidProtocolBufferException   Thrown in case of invalid Message data
     */
    public static String toJSON(Message data, boolean preserveFieldNames) throws InvalidProtocolBufferException {
        JsonFormat.Printer printer = JsonFormat.printer();
        if (preserveFieldNames) {
            printer = printer.preservingProtoFieldNames();
        }
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
