
package com.github.whiver.nifi.exception;

public class SchemaCompilationException extends Exception {
    public SchemaCompilationException(String filepath) {
        super("An error occurred while compiling " + filepath);
    }
}
