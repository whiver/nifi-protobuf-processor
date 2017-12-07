package com.github.whiver.nifi.exception;

public class UnknownMessageTypeException extends Exception {
    public UnknownMessageTypeException(String messageType, String pathToSchema) {
        super("No message type '" + messageType + "' found in the schema file " + pathToSchema);
    }
}
