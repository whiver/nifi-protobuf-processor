package com.github.whiver.nifi.exception;

import java.io.IOException;

public class MessageEncodingException extends Exception {
    public MessageEncodingException(IOException parent) {
        super("Unable to encode data: " + parent.getMessage(), parent);
    }
}
