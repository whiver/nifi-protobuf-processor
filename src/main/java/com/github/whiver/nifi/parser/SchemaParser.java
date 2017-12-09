package com.github.whiver.nifi.parser;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protocjar.Protoc;
import com.github.whiver.nifi.exception.SchemaCompilationException;
import com.github.whiver.nifi.exception.SchemaLoadingException;
import com.github.whiver.nifi.parser.parser.CompiledSchema;
import com.google.protobuf.Descriptors;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class SchemaParser {
    static CompiledSchema compileProto(String schemaPath) throws IOException, InterruptedException, SchemaCompilationException {
        // Get a temp file path to write the compiled output
        File outFile = File.createTempFile("protobuf-desc-tempfile", ".desc");
        File inFile = new File(schemaPath);

        String[] args = {"--include_imports", "-I", inFile.getParentFile().getAbsolutePath(), "-o", outFile.getAbsolutePath(), inFile.getAbsolutePath()};
        if (Protoc.runProtoc(args) != 0) {
            throw new SchemaCompilationException(schemaPath);
        }

        return new CompiledSchema(outFile);
    }

    public static DynamicSchema parseSchema(String pathToSchema, boolean compileSchema) throws Descriptors.DescriptorValidationException, SchemaLoadingException, InterruptedException, SchemaCompilationException, IOException {
        DynamicSchema schema = null;

        try {
            if (compileSchema) {
                try (CompiledSchema compiledSchema = SchemaParser.compileProto(pathToSchema)) {
                    schema = DynamicSchema.parseFrom(compiledSchema.read());
                } catch (IOException | InterruptedException | SchemaCompilationException e) {
                    // Compilation exceptions prevent us from continuing, so we rethrow the exception
                    throw  e;
                } catch (Exception e) {
                    // Else we just could not delete the temp file, so we log it but we continue
                    e.getMessage();
                    e.printStackTrace();
                }
            } else {
                FileInputStream schemaFile = new FileInputStream(pathToSchema);
                schema = DynamicSchema.parseFrom(schemaFile);
            }
        } catch (IOException e) {
            throw new SchemaLoadingException(e);
        }

        return schema;
    }
}
