package com.github.whiver.nifi.exception;

import java.io.IOException;

public class SchemaLoadingException extends Exception {
    public SchemaLoadingException(IOException parent) {
        super("Unable to read schema file: " + parent.getMessage(), parent);
    }
}
