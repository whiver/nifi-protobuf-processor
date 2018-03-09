# nifi-protobuf-processor [![codecov](https://codecov.io/gh/whiver/nifi-protobuf-processor/branch/develop/graph/badge.svg)](https://codecov.io/gh/whiver/nifi-protobuf-processor) [![Build Status](https://travis-ci.org/whiver/nifi-protobuf-processor.svg?branch=develop)](https://travis-ci.org/whiver/nifi-protobuf-processor)
An Apache NiFi processor to encode and decode data using Google Protocol Buffers schemas.

## Features

As this processor is a work-in-progress, the features already implemented and tested are checked.

- Encode/decode Protocol Buffer messages from/to JSON format
- Read a compiled schema file (`.desc`) from disk
- Use directly a raw `.proto` schema file, from disk or directly embedded in a property
- Can handle embedded `.proto` files at processor level (as a processor property) or directly in a flowfile property
- Support dependencies in proto files (see [below](#usage))
- Provide a ready-to-use Docker image of Apache NiFi with the NiFi Protobuf Processor

## Installation

### Using Docker
A pre-packaged version of NiFi with the processor installed is
[available on Docker Hub](https://hub.docker.com/r/whiver/nifi-protobuf/). To run it just type:

    docker run -p 8080:8080 whiver/nifi-protobuf:latest

Note that the `-p` option publishes the port 8080 used by NiFi to the host, so that you can access the UI directly *via*
`http://localhost:8080/nifi`.

### Using a stable release
Grab the latest release directly from the releases page and copy the `.nar` file in the Apache NiFi `lib` folder.

### Building the latest version
Clone this project and build the processor `nar` file using Maven:

    mvn compile
    mvn nifi-nar:nar
    
Then simply copy the generated `nar` file into the Apache NiFi `lib` folder.

#### Building the Docker image

The project also includes a Dockerfile to easily build a Docker image of the project. In fact you just need to run:

    mvn package
    
and everything should be fine ! :)

## Usage

See the installation section to learn how to integrate this processor in Apache NiFi.
This projects add 2 different new processors in NiFi:

- `ProtobufDecoder`, which **decodes** a Protobuf-encoded payload to different kind of structured formats ;
- `ProtobufEncoder`, which **encodes** a payload in a structured format using a Protobuf schema.

### Specifying the schema file
In both processors, you have to specify a schema file to use for data encoding/decoding. You can do so either
processor-wide (meaning that every incoming flowfiles will be processed using the same schema) or per-flowfile. In both
cases, it is done by writing the absolute schema file path in the `protobuf.schemaPath` property of the flowfile or
processor. Note that *if the property is set in the flowfile, it will override the one from the processor*.

### Schema file format
I strongly recommend you to use a compiled `.desc` file whenever possible, for a performance reason. This file can be
obtained by compiling the `.proto` file with [Google's `protoc`](https://github.com/google/protobuf/releases).
However, if you cannot compile your `.proto` file, you can set it directly as a schema file and set the
`protobuf.compileSchema` property of the processor to tell it to compile the schema dynamically.

> **Important**: The processor allows you to import **only one schema file**, so you need to package all you dependencies
> into one file. To do so, compile your main `.proto` file using the `--include_imports` option of the `protoc` compiler.
> If you are using a raw `.proto` file, you need to bundle all imports inside the file.

> *Note*: if you don't have a compiled `.desc` file yet, you should
> [take a look at `protoc`](https://github.com/google/protobuf/releases), the Protobuf compiler from Google. 

For now, the only structured format the processors can process is the JSON. In the future, there should be more formats
available (XML and flowfile properties are expected).

### Performances

By design, this processor cannot use precompiled code to handle messages (otherwise you would have already generated them)
and wouldn't be here. So this processor is using the runtime part of the Protobuf library, which dynamically parses the files,
given a compiled schema (`.desc`).

For convenience, the processor also allows you to provide a raw `.proto` file but, to be used, it must be compiled, so this
is what the processor does before anything else. To avoid multiple compilation when not needed, the result file is cached,
and if you specified the schema in the processor configuration (and not in the flowfile properties), it will be directly
reused for each operation, and it will even avoid reading the schema from the disk.

So, if you can, *specify the schema at the processor level to get the best performances*.

## Contributing

This project is Free as in Freedom, so feel free to contribute by posting bug report or pull requests!

## License

This project is licensed under the MIT license. The terms of this license can be found in the [LICENSE file](LICENSE).