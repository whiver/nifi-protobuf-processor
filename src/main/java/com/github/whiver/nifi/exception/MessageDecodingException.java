package com.github.whiver.nifi.exception;

import java.io.IOException;

public class MessageDecodingException extends Exception {
    public MessageDecodingException(IOException parent) {
        super("Unable to decode data: " + parent.getMessage(), parent);
    }
}
