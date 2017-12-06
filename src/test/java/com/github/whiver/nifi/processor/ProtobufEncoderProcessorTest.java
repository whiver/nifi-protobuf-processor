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
    @Test
    public void onTrigger() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new ProtobufEncoderProcessor());

        // AddressBook test
        InputStream basicTestJson = ProtobufEncoderProcessorTest.class.getResourceAsStream("/data/AddressBook_basic.json");
        InputStream severalEntriesTestJson = ProtobufEncoderProcessorTest.class.getResourceAsStream("/data/AddressBook_several.json");
        HashMap<String, String> adressBookProperties = new HashMap<>();
        adressBookProperties.put("protobuf.schemaPath", ProtobufDecoderProcessorTest.class.getResource("/schemas/AddressBook.desc").getPath());
        adressBookProperties.put("protobuf.messageType", "AddressBook");

        // Ensure the configuration is valid as-is
        runner.assertValid();

        // Enqueue the flowfile
        runner.enqueue(basicTestJson, adressBookProperties);
        runner.enqueue(severalEntriesTestJson, adressBookProperties);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(2);
        runner.assertQueueEmpty();

        // Check if the data was processed without failure
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ProtobufDecoderProcessor.SUCCESS);
        Assert.assertEquals("2 flowfiles should be returned to success", 2, results.size());

        // Check if the content of the flowfile is as expected
        InputStream basicTestExpected = ProtobufDecoderProcessorTest.class.getResourceAsStream("/data/AddressBook_basic.data");
        InputStream severalEntriesTestExpected = ProtobufDecoderProcessorTest.class.getResourceAsStream("/data/AddressBook_several.data");

        results.get(0).assertContentEquals(basicTestExpected);
        results.get(1).assertContentEquals(severalEntriesTestExpected);

    }

}