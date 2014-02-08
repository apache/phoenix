These are the protobuf definition files. The produced java classes are generated into 
phoenix-core/src/main/java/org/apache/phoenix/coprocessor/generated. Please check them in so no need
to recreate them in every build.

To regenerate the classes after making definition file changes, ensure first that
the protobuf protoc(at least 2.5) tool is in your $PATH (You may need to download it and build
it first; its part of the protobuf package obtainable from here: 
http://code.google.com/p/protobuf/downloads/list).

Goto folder phoenix-protocol/src/main and run "./build-proto.sh" 
