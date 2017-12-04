package com.github.whiver.nifi.processor;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;


/**
 * A test class mocking a NiFi flow
 */
public class ProtobufDecoderProcessorTest {
    @Test
    public void onTrigger() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new ProtobufDecoderProcessor());

        // Basic Person test
        InputStream basicTestEncrypted = ProtobufDecoderProcessorTest.class.getResourceAsStream("/data/Person_basic.data");
        HashMap<String, String> personProperties = new HashMap<>();
        personProperties.put("protobuf.schemaPath", ProtobufDecoderProcessorTest.class.getResource("/schemas/PersonSchema.desc").getPath());
        personProperties.put("protobuf.messageType", "Person");

        // Ensure the configuration is valid as-is
        runner.assertValid();

        // Enqueue the flowfile
        runner.enqueue(basicTestEncrypted, personProperties);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);
        runner.assertQueueEmpty();

        // First were processed without failure
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ProtobufDecoderProcessor.SUCCESS);
        assertEquals("1 flowfile should be returned to success", 1, results.size());

        System.out.println(results.get(0).toString());
        // Second were processed with "unknown product ID"
        //results = runner.getFlowFilesForRelationship(ProtobufDecoderProcessor.INVALID_SCHEMA);
        //assertEquals("A unique flowfile should be returned to unknown", 1, results.size());
    }

}