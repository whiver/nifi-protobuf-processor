package com.github.whiver.nifi.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;


/**
 * A test class mocking a NiFi flow
 */
public class ProtobufDecoderTest {

    @Test
    public void onTrigger() throws Exception {
        final String[] validTestFiles = {"AddressBook_basic", "AddressBook_several"};
        TestRunner runner = TestRunners.newTestRunner(new ProtobufDecoder());

        // AddressBook test
        HashMap<String, String> addressBookProperties = new HashMap<>();
        addressBookProperties.put("protobuf.schemaPath", ProtobufDecoderTest.class.getResource("/schemas/AddressBook.desc").getPath());
        addressBookProperties.put("protobuf.messageType", "AddressBook");

        // AddressBook test
        for (String filename: validTestFiles) {
            InputStream jsonFile = ProtobufDecoderTest.class.getResourceAsStream("/data/" + filename + ".data");
            addressBookProperties.put("testfile", filename);
            runner.enqueue(jsonFile, addressBookProperties);
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
        ObjectMapper mapper = new ObjectMapper();

        for (MockFlowFile result: results) {
            JsonNode expected = mapper.readTree(this.getClass().getResourceAsStream("/data/" + result.getAttribute("testfile") + ".json"));
            JsonNode given = mapper.readTree(runner.getContentAsByteArray(result));
            Assert.assertEquals("The parsing result of " + result.getAttribute("testfile") + ".data is not as expected", expected, given);
        }

        // Change schema to a .proto file
        runner.clearTransferState();
        runner.setProperty("protobuf.compileSchema", "true");
        InputStream dataFile = ProtobufDecoderTest.class.getResourceAsStream("/data/Person.data");
        HashMap<String, String> personProperties = new HashMap<>();
        personProperties.put("protobuf.schemaPath", ProtobufDecoderTest.class.getResource("/schemas/Person.proto").getPath());
        personProperties.put("protobuf.messageType", "Person");
        runner.enqueue(dataFile, personProperties);

        runner.assertValid();
        runner.run(1);
        runner.assertQueueEmpty();

        runner.assertAllFlowFilesTransferred(ProtobufDecoder.SUCCESS);
        MockFlowFile result = runner.getFlowFilesForRelationship(ProtobufDecoder.SUCCESS).get(0);

        JsonNode expected = mapper.readTree(this.getClass().getResourceAsStream("/data/Person.json"));
        JsonNode given = mapper.readTree(runner.getContentAsByteArray(result));
        Assert.assertEquals("The parsing result of Person.data is not as expected", expected, given);
    }

    @Test
    public void onPropertyModified() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new ProtobufDecoder());

        HashMap<String, String> addressBookProperties = new HashMap<>();
        addressBookProperties.put("protobuf.messageType", "AddressBook");


        /*
            First try to decode using a schema set in a processor property
         */

        runner.setProperty("protobuf.schemaPath", ProtobufDecoderTest.class.getResource("/schemas/AddressBook.desc").getPath());
        runner.assertValid();

        runner.enqueue(ProtobufDecoderTest.class.getResourceAsStream("/data/AddressBook_basic.data"), addressBookProperties);

        runner.run(1);

        // Check if the flowfile has been successfully processed
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(ProtobufDecoder.SUCCESS);

        // Finally check the content
        MockFlowFile result = runner.getFlowFilesForRelationship(ProtobufDecoder.SUCCESS).get(0);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(this.getClass().getResourceAsStream("/data/AddressBook_basic.json"));
        JsonNode given = mapper.readTree(runner.getContentAsByteArray(result));
        Assert.assertEquals("The parsing result of AddressBook_basic.data is not as expected", expected, given);


        /*
            Then try to remove the schema from the processor property and see if it still parse
         */

        runner.clearTransferState();
        runner.removeProperty(runner.getProcessor().getPropertyDescriptor("protobuf.schemaPath"));
        Assert.assertFalse("The schema property should now be null", runner.getProcessContext().getProperty("protobuf.schemaPath").isSet());
        runner.assertValid();

        runner.enqueue(ProtobufDecoderTest.class.getResourceAsStream("/data/AddressBook_basic.data"), addressBookProperties);

        runner.run(1);

        // Check if the flowfile has been successfully processed
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(ProtobufDecoder.INVALID_SCHEMA);


        /*
            Finally add the property again to see if it works again
         */

        runner.clearTransferState();
        runner.setProperty("protobuf.schemaPath", ProtobufDecoderTest.class.getResource("/schemas/AddressBook.desc").getPath());
        runner.assertValid();

        runner.enqueue(ProtobufDecoderTest.class.getResourceAsStream("/data/AddressBook_basic.data"), addressBookProperties);

        runner.run(1);

        // Check if the flowfile has been successfully processed
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(ProtobufDecoder.SUCCESS);

        // Finally check the content
        result = runner.getFlowFilesForRelationship(ProtobufDecoder.SUCCESS).get(0);
        expected = mapper.readTree(this.getClass().getResourceAsStream("/data/AddressBook_basic.json"));
        given = mapper.readTree(runner.getContentAsByteArray(result));
        Assert.assertEquals("The parsing result of AddressBook_basic.data is not as expected", expected, given);

    }
}