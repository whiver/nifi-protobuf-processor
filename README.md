# nifi-protobuf-processor [![codecov](https://codecov.io/gh/whiver/nifi-protobuf-processor/branch/master/graph/badge.svg)](https://codecov.io/gh/whiver/nifi-protobuf-processor) [![Build Status](https://travis-ci.org/whiver/nifi-protobuf-processor.svg?branch=master)](https://travis-ci.org/whiver/nifi-protobuf-processor)
An Apache NiFi processor to encode and decode data using Google Protocol Buffers schemas.

## Features

As this processor is a work-in-progress, the features already implemented and tested are checked.

### Main functionalities
- [x] Graphical configuration interface in Apache NiFi
- [ ] Encode a raw payload to Protocol Buffers
- [ ] Decode a Protocol Buffers payload to JSON
- [ ] Support dependencies in proto files
- [ ] Allow to encode/decode from/to XML, JSON, YAML

### Protobuf schema specification
- [ ] Support compiled schema at a specified user location on the disk
- [ ] Support compiled schema embedded in the flowfile
- [ ] Support embedding raw .proto files in the flowfile

### Processor packaging
- [ ] Provides a ready-to-use Docker image of Apache NiFi with the NiFi Protobuf Processor

## Installation

### Using a stable release
Grab the latest release directly from the releases page and copy the `.nar` file in the Apache NiFi `lib` folder.

### Building the latest version
Clone this project and build the processor `nar` file using Maven:

    mvn nifi-nar:nar
    
Then simply copy the generated `nar` file into the Apache NiFi `lib` folder.