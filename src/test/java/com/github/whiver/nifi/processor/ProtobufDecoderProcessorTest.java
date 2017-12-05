package com.github.whiver.nifi.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


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

        // Check if the data was processed without failure
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ProtobufDecoderProcessor.SUCCESS);
        assertEquals("1 flowfile should be returned to success", 1, results.size());

        // Check if the content of the flowfile is as expected
        ObjectMapper mapper = new ObjectMapper();

        JsonNode expectedBasicPerson = mapper.readTree(this.getClass().getResourceAsStream("/data/Person_basic.json"));
        JsonNode givenBasicPerson = mapper.readTree(runner.getContentAsByteArray(results.get(0)));
        System.out.println(givenBasicPerson.toString());

        assertTrue("The parsing result of Person_basic.data is not as expected", expectedBasicPerson.equals(givenBasicPerson));

    }

}