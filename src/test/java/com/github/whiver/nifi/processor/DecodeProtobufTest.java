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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;


/**
 * A test class mocking a NiFi flow
 */
public class DecodeProtobufTest {
    private final String[] validTestFiles = {"AddressBook_basic", "AddressBook_several"};

    /**
     * Test decoding valid files given a .desc schema
     * @throws IOException
     */
    @Test
    public void onTriggerDecodeValidFiles() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(new DecodeProtobuf());

        // AddressBook test
        HashMap<String, String> addressBookProperties = new HashMap<>();
        addressBookProperties.put(AbstractProtobufProcessor.PROTOBUF_SCHEMA.getName(), DecodeProtobufTest.class.getResource("/schemas/AddressBook.desc").getPath());
        addressBookProperties.put(AbstractProtobufProcessor.PROTOBUF_MESSAGE_TYPE.getName(), "AddressBook");

        // AddressBook test
        for (String filename: validTestFiles) {
            InputStream jsonFile = DecodeProtobufTest.class.getResourceAsStream("/data/" + filename + ".data");
            addressBookProperties.put("testfile", filename);
            runner.enqueue(jsonFile, addressBookProperties);
        }

        // Ensure the configuration is valid as-is
        runner.assertValid();

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(validTestFiles.length);
        runner.assertQueueEmpty();

        // Check if the data was processed without failure
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(DecodeProtobuf.SUCCESS);
        Assert.assertEquals("All flowfiles should be returned to success", validTestFiles.length, results.size());

        // Check if the content of the flowfile is as expected
        ObjectMapper mapper = new ObjectMapper();

        for (MockFlowFile result: results) {
            JsonNode expected = mapper.readTree(this.getClass().getResourceAsStream("/data/" + result.getAttribute("testfile") + ".json"));
            JsonNode given = mapper.readTree(runner.getContentAsByteArray(result));
            Assert.assertEquals("The parsing result of " + result.getAttribute("testfile") + ".data is not as expected", expected, given);
        }
    }

    /**
     * Test encoding valid files given an already compiled schema specified at processor level
     * @throws Exception
     */
    @Test
    public void onTriggerDecodeValidFilesWithSchemaAtProcessorLevel() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new DecodeProtobuf());
        runner.setProperty(AbstractProtobufProcessor.COMPILE_SCHEMA, "false");
        runner.setProperty(AbstractProtobufProcessor.PROTOBUF_SCHEMA, DecodeProtobufTest.class.getResource("/schemas/Person.desc").getPath());

        InputStream dataFile = DecodeProtobufTest.class.getResourceAsStream("/data/Person.data");
        HashMap<String, String> personProperties = new HashMap<>();
        personProperties.put(AbstractProtobufProcessor.PROTOBUF_MESSAGE_TYPE.getName(), "Person");
        runner.enqueue(dataFile, personProperties);

        runner.assertValid();
        runner.run(1);
        runner.assertQueueEmpty();

        runner.assertAllFlowFilesTransferred(DecodeProtobuf.SUCCESS);
        MockFlowFile result = runner.getFlowFilesForRelationship(DecodeProtobuf.SUCCESS).get(0);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(this.getClass().getResourceAsStream("/data/Person.json"));
        JsonNode given = mapper.readTree(runner.getContentAsByteArray(result));
        Assert.assertEquals("The parsing result of Person.data is not as expected", expected, given);
    }

    /**
     * Test decoding valid files given an uncompiled .proto schema specified at flowfile level
     * @throws Exception
     */
    @Test
    public void onTriggerCompileFlowfileSchemaAndDecodeValidFiles() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new DecodeProtobuf());
        runner.setProperty(AbstractProtobufProcessor.COMPILE_SCHEMA, "true");

        InputStream dataFile = DecodeProtobufTest.class.getResourceAsStream("/data/Person.data");
        HashMap<String, String> personProperties = new HashMap<>();
        personProperties.put(AbstractProtobufProcessor.PROTOBUF_SCHEMA.getName(), DecodeProtobufTest.class.getResource("/schemas/Person.proto").getPath());
        personProperties.put(AbstractProtobufProcessor.PROTOBUF_MESSAGE_TYPE.getName(), "Person");
        runner.enqueue(dataFile, personProperties);

        runner.assertValid();
        runner.run(1);
        runner.assertQueueEmpty();

        runner.assertAllFlowFilesTransferred(DecodeProtobuf.SUCCESS);
        MockFlowFile result = runner.getFlowFilesForRelationship(DecodeProtobuf.SUCCESS).get(0);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(this.getClass().getResourceAsStream("/data/Person.json"));
        JsonNode given = mapper.readTree(runner.getContentAsByteArray(result));
        Assert.assertEquals("The parsing result of Person.data is not as expected", expected, given);
    }

    /**
     * Test decoding valid files given an uncompiled .proto schema specified at processor level
     * @throws Exception
     */
    @Test
    public void onTriggerCompileProcessorSchemaAndDecodeValidFiles() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new DecodeProtobuf());
        runner.setProperty(AbstractProtobufProcessor.COMPILE_SCHEMA, "true");
        runner.setProperty(AbstractProtobufProcessor.PROTOBUF_SCHEMA, DecodeProtobufTest.class.getResource("/schemas/Person.proto").getPath());
        //runner.setProperty(AbstractProtobufProcessor.PROTOBUF_MESSAGE_TYPE, "Person");

        InputStream dataFile = DecodeProtobufTest.class.getResourceAsStream("/data/Person.data");
        HashMap<String, String> personProperties = new HashMap<>();
        personProperties.put(AbstractProtobufProcessor.PROTOBUF_MESSAGE_TYPE.getName(), "Person");
        runner.enqueue(dataFile, personProperties);

        runner.assertValid();
        runner.run(1);
        runner.assertQueueEmpty();

        runner.assertAllFlowFilesTransferred(DecodeProtobuf.SUCCESS);
        MockFlowFile result = runner.getFlowFilesForRelationship(DecodeProtobuf.SUCCESS).get(0);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(this.getClass().getResourceAsStream("/data/Person.json"));
        JsonNode given = mapper.readTree(runner.getContentAsByteArray(result));
        Assert.assertEquals("The parsing result of Person.data is not as expected", expected, given);
    }

    /**
     * Test if the per-flowfile schema have priority on the processor-wide one
     * @throws IOException
     */
    @Test
    public void onTriggerUsePerFlowfileSchemaIfAvailable() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(new DecodeProtobuf());
        runner.setProperty("protobuf.schemaPath", EncodeProtobufTest.class.getResource("/schemas/AddressBook.desc").getPath());

        InputStream dataFile = DecodeProtobufTest.class.getResourceAsStream("/data/Person.data");
        HashMap<String, String> personProperties = new HashMap<>();
        personProperties.put(AbstractProtobufProcessor.PROTOBUF_SCHEMA.getName(), DecodeProtobufTest.class.getResource("/schemas/Person.desc").getPath());
        personProperties.put(AbstractProtobufProcessor.PROTOBUF_MESSAGE_TYPE.getName(), "Person");
        runner.enqueue(dataFile, personProperties);

        runner.assertValid();
        runner.run(1);
        runner.assertQueueEmpty();

        runner.assertAllFlowFilesTransferred(DecodeProtobuf.SUCCESS);
        MockFlowFile result = runner.getFlowFilesForRelationship(DecodeProtobuf.SUCCESS).get(0);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(this.getClass().getResourceAsStream("/data/Person.json"));
        JsonNode given = mapper.readTree(runner.getContentAsByteArray(result));
        Assert.assertEquals("The decoder should use the schema from flowfile instead of processor if given", expected, given);
    }

    /**
     * Test decoding using a processor-wide schema and switches between flowfile schema and processor schema
     * @throws Exception
     */
    @Test
    public void onPropertyModified() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new DecodeProtobuf());

        HashMap<String, String> addressBookProperties = new HashMap<>();
        addressBookProperties.put("protobuf.messageType", "AddressBook");


        /*
            First try to decode using a schema set in a processor property
         */

        runner.setProperty(AbstractProtobufProcessor.PROTOBUF_SCHEMA.getName(), DecodeProtobufTest.class.getResource("/schemas/AddressBook.desc").getPath());
        runner.assertValid();

        runner.enqueue(DecodeProtobufTest.class.getResourceAsStream("/data/AddressBook_basic.data"), addressBookProperties);

        runner.run(1);

        // Check if the flowfile has been successfully processed
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(DecodeProtobuf.SUCCESS);

        // Finally check the content
        MockFlowFile result = runner.getFlowFilesForRelationship(DecodeProtobuf.SUCCESS).get(0);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(this.getClass().getResourceAsStream("/data/AddressBook_basic.json"));
        JsonNode given = mapper.readTree(runner.getContentAsByteArray(result));
        Assert.assertEquals("The parsing result of AddressBook_basic.data is not as expected", expected, given);


        /*
            Then try to remove the schema from the processor property and see if it still parse
         */

        runner.clearTransferState();
        runner.removeProperty(AbstractProtobufProcessor.PROTOBUF_SCHEMA);
        Assert.assertFalse("The schema property should now be null", runner.getProcessContext().getProperty(AbstractProtobufProcessor.PROTOBUF_SCHEMA).isSet());
        runner.assertValid();

        runner.enqueue(DecodeProtobufTest.class.getResourceAsStream("/data/AddressBook_basic.data"), addressBookProperties);

        runner.run(1);

        // Check if the flowfile has been successfully processed
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(DecodeProtobuf.INVALID_SCHEMA);


        /*
            Finally add the property again to see if it works again
         */

        runner.clearTransferState();
        runner.setProperty(AbstractProtobufProcessor.PROTOBUF_SCHEMA, DecodeProtobufTest.class.getResource("/schemas/AddressBook.desc").getPath());
        runner.assertValid();

        runner.enqueue(DecodeProtobufTest.class.getResourceAsStream("/data/AddressBook_basic.data"), addressBookProperties);

        runner.run(1);

        // Check if the flowfile has been successfully processed
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(DecodeProtobuf.SUCCESS);

        // Finally check the content
        result = runner.getFlowFilesForRelationship(DecodeProtobuf.SUCCESS).get(0);
        expected = mapper.readTree(this.getClass().getResourceAsStream("/data/AddressBook_basic.json"));
        given = mapper.readTree(runner.getContentAsByteArray(result));
        Assert.assertEquals("The parsing result of AddressBook_basic.data is not as expected", expected, given);

    }
}