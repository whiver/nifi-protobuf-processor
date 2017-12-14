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

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.whiver.nifi.exception.SchemaCompilationException;
import com.github.whiver.nifi.exception.SchemaLoadingException;
import com.github.whiver.nifi.parser.SchemaParser;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.commons.io.IOUtils;
import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.XMLUnit;
import org.junit.Assert;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class XMLMapperTest {

    @Test
    public void decodeEncode() throws InterruptedException, SchemaLoadingException, Descriptors.DescriptorValidationException, SchemaCompilationException, IOException, SAXException {
        InputStream inputData = XMLMapperTest.class.getResourceAsStream("/data/AddressBook_several.xml");
        DynamicSchema schema = SchemaParser.parseSchema(XMLMapperTest.class.getResource("/schemas/AddressBook.desc").getPath(), false);
        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("AddressBook");

        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);

        // Decode-Encode message
        XMLMapper xmlMapper = new XMLMapper();
        Message decodedMessage = xmlMapper.decode(inputData, builder);
        String encodedMessage = xmlMapper.encode(decodedMessage);

        // Compare results
        XMLUnit.setIgnoreWhitespace(true);
        XMLUnit.setIgnoreAttributeOrder(true);

        String expected = IOUtils.toString(this.getClass().getResourceAsStream("/data/AddressBook_several.xml"), StandardCharsets.UTF_8);

        DetailedDiff diff = new DetailedDiff(XMLUnit.compareXML(expected, encodedMessage));

        List<?> allDifferences = diff.getAllDifferences();
        Assert.assertEquals("Decoding-Encoding does not result in the initial data. Differences found: "+ diff.toString(), 0, allDifferences.size());
    }
}