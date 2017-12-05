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
    @Test
    public void onTrigger() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new ProtobufDecoderProcessor());

        // AddressBook test
        InputStream basicTestEncrypted = ProtobufDecoderProcessorTest.class.getResourceAsStream("/data/AddressBook_basic.data");
        InputStream severalEntriesTestEncrypted = ProtobufDecoderProcessorTest.class.getResourceAsStream("/data/AddressBook_several.data");
        HashMap<String, String> adressBookProperties = new HashMap<>();
        adressBookProperties.put("protobuf.schemaPath", ProtobufDecoderProcessorTest.class.getResource("/schemas/AddressBook.desc").getPath());
        adressBookProperties.put("protobuf.messageType", "AddressBook");

        // Ensure the configuration is valid as-is
        runner.assertValid();

        // Enqueue the flowfile
        runner.enqueue(basicTestEncrypted, adressBookProperties);
        runner.enqueue(severalEntriesTestEncrypted, adressBookProperties);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(2);
        runner.assertQueueEmpty();

        // Check if the data was processed without failure
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ProtobufDecoderProcessor.SUCCESS);
        Assert.assertEquals("2 flowfiles should be returned to success", 2, results.size());

        // Check if the content of the flowfile is as expected
        ObjectMapper mapper = new ObjectMapper();

        JsonNode expectedBasicTest = mapper.readTree(this.getClass().getResourceAsStream("/data/AddressBook_basic.json"));
        JsonNode givenBasicTest = mapper.readTree(runner.getContentAsByteArray(results.get(0)));

        JsonNode expectedSeveralEntriesTest = mapper.readTree(this.getClass().getResourceAsStream("/data/AddressBook_several.json"));
        JsonNode givenSeveralEntriesTestTest = mapper.readTree(runner.getContentAsByteArray(results.get(1)));

        Assert.assertEquals("The parsing result of AddressBook_basic.data is not as expected", expectedBasicTest, givenBasicTest);
        Assert.assertEquals("The parsing result of AddressBook_several.data is not as expected", expectedSeveralEntriesTest, givenSeveralEntriesTestTest);

    }

}