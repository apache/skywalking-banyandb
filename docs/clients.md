# Clients

## Command Line

The command line tool named `bydbctl` improves users' interactive experience. The examples listed in this folder show how to use this command to create, update, read and delete schemas. Furthermore, `bydbctl` could help in querying data stored in streams, measures and properties.

These are several ways to install:

* Get binaries from [download](https://skywalking.apache.org/downloads/).
* Build from [sources](https://github.com/apache/skywalking-banyandb/tree/main/bydbctl) to get latest features.

The config file named `.bydbctl.yaml` will be created in `$HOME` folder after the first CRUD command is applied.
```shell
> more ~/.bydbctl.yaml
addr: http://127.0.0.1:64299
group: ""
```

`bydbctl` leverages HTTP endpoints to retrieve data instead of gRPC.

## HTTP client

Users could select any HTTP client to access the HTTP based endpoints. The default address is `localhost:17913/api`

## Java Client

The java native client is hosted at [skywalking-banyandb-java-client](https://github.com/apache/skywalking-banyandb-java-client).

## Web application

The web application is hosted at [skywalking-banyandb-webapp](http://localhost:17913/) when you boot up the BanyanDB server.

## gRPC command-line tool

Users have a chance to use any command-line tool to interact with the Banyand server's gRPC endpoints. The only limitation is the CLI tool has to support [file descriptor files](https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/descriptor.proto) since the database server does not support server reflection.

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
