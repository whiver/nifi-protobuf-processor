package com.github.whiver.nifi.processor;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;

public class ProtobufEncoderProcessorTest {
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
        TestRunner runner = TestRunners.newTestRunner(new ProtobufEncoderProcessor());

        HashMap<String, String> adressBookProperties = new HashMap<>();
        adressBookProperties.put("protobuf.schemaPath", ProtobufEncoderProcessorTest.class.getResource("/schemas/AddressBook.desc").getPath());
        adressBookProperties.put("protobuf.messageType", "AddressBook");

        // AddressBook test
        for (String filename: validTestFiles) {
            InputStream jsonFile = ProtobufEncoderProcessorTest.class.getResourceAsStream("/data/" + filename + ".json");
            adressBookProperties.put("testfile", filename);
            runner.enqueue(jsonFile, adressBookProperties);
        }

        // Ensure the configuration is valid as-is
        runner.assertValid();

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(validTestFiles.length);
        runner.assertQueueEmpty();

        // Check if the data was processed without failure
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ProtobufDecoderProcessor.SUCCESS);
        Assert.assertEquals("All flowfiles should be returned to success", validTestFiles.length, results.size());

        // Check if the content of the flowfile is as expected
        for (MockFlowFile result: results) {
            result.assertContentEquals(ProtobufEncoderProcessorTest.class.getResourceAsStream("/data/" + result.getAttribute("testfile") + ".data"));
        }

    }

}