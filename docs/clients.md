# Clients

## gRPC command-line tool

Users have a chance to use any command-line tool to interact with the Banyand server.
The only limitation is the CLI tool has to support [file descriptor files](https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/descriptor.proto) since the database server does not support server reflection.

[Buf](https://buf.build/) is a Protobuf building tooling the BanyanDB relies on. It can provide `FileDescriptorSet`s usable by gRPC CLI tools like [grpcurl](https://github.com/fullstorydev/grpcurl)

BanyanDB recommends installing `Buf` by issuing

```shell
$ make -C api generate
Protobuf schema files are compiled
```

Above command will compile `*.proto` after downloading `buf` into `<project_root>/bin`

Users could leverage `buf`'s internal compiler to generate the `FileDescriptorSet`s

```shell
$ cd api
$ ../bin/buf build -o image.bin
```

If grpcurl is the CLI tool to access the APIs of BanyanDb. To use `image.bin` with it on the fly:

```shell
$ grpcurl -plaintext -protoset image.bin localhost:17912 ...
```

## Java Client

The java native client is hosted at [skywalking-banyandb-java-client](https://github.com/apache/skywalking-banyandb-java-client).

## Web application (TBD)

## Command Line (TBD)
