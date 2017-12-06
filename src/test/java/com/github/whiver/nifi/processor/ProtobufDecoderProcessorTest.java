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
public class ProtobufDecoderProcessorTest {
    /**
     * List the files used to test the decoder. Only list the file names without extension, as the .data file will be
     * used as Protobuf encoded data and the .json file will be used as the reference decoded result.
     * Note that every files will be encoded against the AddressBook schema.
     */
    private final String[] validTestFiles = {
            "AddressBook_basic", "AddressBook_several"
    };

    @Test
    public void onTrigger() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new ProtobufDecoderProcessor());

        // AddressBook test
        HashMap<String, String> adressBookProperties = new HashMap<>();
        adressBookProperties.put("protobuf.schemaPath", ProtobufDecoderProcessorTest.class.getResource("/schemas/AddressBook.desc").getPath());
        adressBookProperties.put("protobuf.messageType", "AddressBook");

        // AddressBook test
        for (String filename: validTestFiles) {
            InputStream jsonFile = ProtobufDecoderProcessorTest.class.getResourceAsStream("/data/" + filename + ".data");
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
        ObjectMapper mapper = new ObjectMapper();

        for (MockFlowFile result: results) {
            JsonNode expected = mapper.readTree(this.getClass().getResourceAsStream("/data/" + result.getAttribute("testfile") + ".json"));
            JsonNode given = mapper.readTree(runner.getContentAsByteArray(result));
            Assert.assertEquals("The parsing result of " + result.getAttribute("testfile") + ".data is not as expected", expected, given);
        }
    }

}