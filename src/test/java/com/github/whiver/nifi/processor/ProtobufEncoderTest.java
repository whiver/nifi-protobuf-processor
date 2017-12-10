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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;

public class ProtobufEncoderTest {

    @Test
    public void onTrigger() throws Exception {
        final String[] validTestFiles = {"AddressBook_basic", "AddressBook_several"};

        TestRunner runner = TestRunners.newTestRunner(new ProtobufEncoder());
        runner.setProperty("protobuf.format", "JSON");

        HashMap<String, String> adressBookProperties = new HashMap<>();
        adressBookProperties.put("protobuf.schemaPath", ProtobufEncoderTest.class.getResource("/schemas/AddressBook.desc").getPath());
        adressBookProperties.put("protobuf.messageType", "AddressBook");

        // AddressBook test
        for (String filename: validTestFiles) {
            InputStream jsonFile = ProtobufEncoderTest.class.getResourceAsStream("/data/" + filename + ".json");
            adressBookProperties.put("testfile", filename);
            runner.enqueue(jsonFile, adressBookProperties);
        }

        // Ensure the configuration is valid as-is
        runner.assertValid();

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(validTestFiles.length);
        runner.assertQueueEmpty();

        // Check if the data was processed without failure
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ProtobufDecoder.SUCCESS);
        Assert.assertEquals("All flowfiles should be returned to success", validTestFiles.length, results.size());

        // Check if the content of the flowfile is as expected
        for (MockFlowFile result: results) {
            result.assertContentEquals(ProtobufEncoderTest.class.getResourceAsStream("/data/" + result.getAttribute("testfile") + ".data"));
        }

        // Change schema to a .proto file
        runner.clearTransferState();
        runner.setProperty("protobuf.compileSchema", "true");
        InputStream jsonFile = ProtobufEncoderTest.class.getResourceAsStream("/data/Person.json");
        HashMap<String, String> personProperties = new HashMap<>();
        personProperties.put("protobuf.schemaPath", ProtobufEncoderTest.class.getResource("/schemas/Person.proto").getPath());
        personProperties.put("protobuf.messageType", "Person");
        runner.enqueue(jsonFile, personProperties);

        runner.assertValid();
        runner.run(1);
        runner.assertQueueEmpty();

        runner.assertAllFlowFilesTransferred(ProtobufEncoder.SUCCESS);
        results = runner.getFlowFilesForRelationship(ProtobufEncoder.SUCCESS);
        Assert.assertEquals("The Person flowfile should be returned to success", 1, results.size());
        results.get(0).assertContentEquals(ProtobufEncoderTest.class.getResourceAsStream("/data/Person.data"));
    }

    @Test
    public void onPropertyModified() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new ProtobufEncoder());
        runner.setProperty("protobuf.format", "JSON");

        HashMap<String, String> addressBookProperties = new HashMap<>();
        addressBookProperties.put("protobuf.messageType", "AddressBook");


        /*
            First try to decode using a schema set in a processor property
         */

        runner.setProperty("protobuf.schemaPath", ProtobufEncoderTest.class.getResource("/schemas/AddressBook.desc").getPath());
        runner.assertValid();

        runner.enqueue(ProtobufEncoderTest.class.getResourceAsStream("/data/AddressBook_basic.json"), addressBookProperties);

        runner.run(1);

        // Check if the flowfile has been successfully processed
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(ProtobufDecoder.SUCCESS);

        // Finally check the content
        MockFlowFile result = runner.getFlowFilesForRelationship(ProtobufDecoder.SUCCESS).get(0);
        result.assertContentEquals(ProtobufEncoderTest.class.getResourceAsStream("/data/AddressBook_basic.data"));


        /*
            Then try to remove the schema from the processor property and see if it still parse
         */

        runner.clearTransferState();
        runner.removeProperty(runner.getProcessor().getPropertyDescriptor("protobuf.schemaPath"));
        Assert.assertFalse("The schema property should now be null", runner.getProcessContext().getProperty("protobuf.schemaPath").isSet());
        runner.assertValid();

        runner.enqueue(ProtobufEncoderTest.class.getResourceAsStream("/data/AddressBook_basic.json"), addressBookProperties);

        runner.run(1);

        // Check if the flowfile has been successfully processed
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(ProtobufDecoder.INVALID_SCHEMA);


        /*
            Finally add the property again to see if it works again
         */

        runner.clearTransferState();
        runner.setProperty("protobuf.schemaPath", ProtobufEncoderTest.class.getResource("/schemas/AddressBook.desc").getPath());
        runner.assertValid();

        runner.enqueue(ProtobufEncoderTest.class.getResourceAsStream("/data/AddressBook_basic.json"), addressBookProperties);

        runner.run(1);

        // Check if the flowfile has been successfully processed
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(ProtobufDecoder.SUCCESS);

        // Finally check the content
        result = runner.getFlowFilesForRelationship(ProtobufDecoder.SUCCESS).get(0);
        result.assertContentEquals(ProtobufEncoderTest.class.getResourceAsStream("/data/AddressBook_basic.data"));

    }
}