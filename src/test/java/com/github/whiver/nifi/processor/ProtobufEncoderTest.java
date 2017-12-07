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

public class ProtobufEncoderTest {
    /**
     * List the files used to test the encoder. Only list the file names without extension, as the .data file will be
     * used as a reference and the .json file will be used as a source file.
     * Note that every files will be encoded against the AddressBook schema.
     */
    private final String[] validTestFiles = {
        "AddressBook_basic", "AddressBook_several"
    };

    @Test
    public void onTrigger() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new ProtobufEncoder());

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

    }

    @Test
    public void onPropertyModified() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new ProtobufEncoder());

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