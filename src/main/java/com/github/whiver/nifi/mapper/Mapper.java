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

import com.github.whiver.nifi.exception.UnknownFormatException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.XmlFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Mapper {
    public enum MapperTarget {JSON, XML};

    /**
     * Format a Protobuf Message into a given format, invoking the appropriate mapper class.
     * @param message   The Protobuf Message to format
     * @param target    The target format
     * @return  A string representing the formatted data in the desired format
     * @throws InvalidProtocolBufferException   Thrown when an error occurs in the message parsing
     * @throws UnknownFormatException           Thrown when the requested format is not supported
     */
    public static String encodeAs(Message message, MapperTarget target) throws InvalidProtocolBufferException, UnknownFormatException {
        switch (target) {
            case JSON:
                return JSONMapper.toJSON(message);
            case XML:
                XmlFormat xmlFormat = new XmlFormat();
                return xmlFormat.printToString(message);
            default:
                throw new UnknownFormatException(target);
        }
    }

    /**
     * Parse a file written in a specified format and turn it into a Protobuf Message.
     * @param inputData The data to parse
     * @param messageBuilder    A message build of the desired message type
     * @param target    The input data format
     * @return  The resulting Protobuf Message
     * @throws UnknownFormatException   Thrown when the requested format is not supported
     * @throws IOException              Thrown when an error occurs during the data parsing
     */
    public static Message decodeFrom(InputStream inputData, Message.Builder messageBuilder, MapperTarget target) throws UnknownFormatException, IOException {
        switch (target) {
            case JSON:
                BufferedReader jsonReader = new BufferedReader(new InputStreamReader(inputData));
                return JSONMapper.fromJSON(new BufferedReader(jsonReader), messageBuilder);
            case XML:
                XmlFormat xmlFormat = new XmlFormat();
                xmlFormat.merge(inputData, messageBuilder);
                return messageBuilder.build();
            default:
                throw new UnknownFormatException(target);
        }
    }
}
