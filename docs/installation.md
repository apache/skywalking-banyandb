
# Installation

`Banyand` is the daemon server of the BanyanDB database. This section will show several paths installing it in your environment.

## Get Binaries

### Released binaries

Get binaries from the [download](https://skywalking.apache.org/downloads/).

### Build From Source

#### Requirements

Users who want to build a binary from sources have to set up:

* Go 1.20
* Node 18.16
* Git >= 2.30
* Linux, macOS or Windows+WSL2
* GNU make

#### Windows

BanyanDB is built on Linux and macOS that introduced several platform-specific characters to the building system. Therefore, we highly recommend you use [WSL2+Ubuntu](https://ubuntu.com/wsl) to execute tasks of the Makefile.

#### Build Binaries

To issue the below command to get basic binaries of banyand and bydbctl.

```shell
$ make generate
...
$ make build
...
--- banyand: all ---
make[1]: Entering directory '<path_to_project_root>/banyand'
...
chmod +x build/bin/banyand-server
Done building banyand server
make[1]: Leaving directory '<path_to_project_root>/banyand'
...
--- bydbctl: all ---
make[1]: Entering directory '<path_to_project_root>/bydbctl'
...
chmod +x build/bin/bydbctl
Done building bydbctl
make[1]: Leaving directory '<path_to_project_root>/bydbctl'
```

The build system provides a series of binary options as well.

* `make -C banyand banyand-server` generates a basic `banyand-server`.
* `make -C banyand release` builds out a static binary for releasing.
* `make -C banyand debug` gives a binary for debugging without the complier's optimizations.
* `make -C banyand debug-static` is a static binary for debugging.
* `make -C bydbctl release` cross-builds several binaries for multi-platforms.

Then users get binaries as below

``` shell
$ ls banyand/build/bin
banyand-server  
banyand-server-debug  
banyand-server-debug-static  
banyand-server-static

$ ls banyand/build/bin
bydbctl
```

## Setup Banyand

`Banyand` shows its available commands and arguments by

```shell
$ ./banyand-server

██████╗  █████╗ ███╗   ██╗██╗   ██╗ █████╗ ███╗   ██╗██████╗ ██████╗ 
██╔══██╗██╔══██╗████╗  ██║╚██╗ ██╔╝██╔══██╗████╗  ██║██╔══██╗██╔══██╗
██████╔╝███████║██╔██╗ ██║ ╚████╔╝ ███████║██╔██╗ ██║██║  ██║██████╔╝
██╔══██╗██╔══██║██║╚██╗██║  ╚██╔╝  ██╔══██║██║╚██╗██║██║  ██║██╔══██╗
██████╔╝██║  ██║██║ ╚████║   ██║   ██║  ██║██║ ╚████║██████╔╝██████╔╝
╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═══╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═══╝╚═════╝ ╚═════╝ 

BanyanDB, as an observability database, aims to ingest, analyze and store Metrics, Tracing and Logging data

Usage:
   [command]

Available Commands:
  completion  generate the autocompletion script for the specified shell
  help        Help about any command
  standalone  Run as the standalone mode

Flags:
  -h, --help      help for this command
  -v, --version   version for this command

Use " [command] --help" for more information about a command.
```

Banyand is running as a standalone process by

```shell
$ ./banyand-server standalone
██████╗  █████╗ ███╗   ██╗██╗   ██╗ █████╗ ███╗   ██╗██████╗ ██████╗ 
██╔══██╗██╔══██╗████╗  ██║╚██╗ ██╔╝██╔══██╗████╗  ██║██╔══██╗██╔══██╗
██████╔╝███████║██╔██╗ ██║ ╚████╔╝ ███████║██╔██╗ ██║██║  ██║██████╔╝
██╔══██╗██╔══██║██║╚██╗██║  ╚██╔╝  ██╔══██║██║╚██╗██║██║  ██║██╔══██╗
██████╔╝██║  ██║██║ ╚████║   ██║   ██║  ██║██║ ╚████║██████╔╝██████╔╝
╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═══╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═══╝╚═════╝ ╚═════╝ 
***starting as a standalone server****
...
...
***Listening to**** addr::17912 module:LIAISON-GRPC
```

The banyand-server would be listening on the `0.0.0.0:17912` if no errors occurred.

To discover more options to configure the banyand by

```shell
$ ./banyand-server standalone -h
Usage:
   standalone [flags]

Flags:
      --addr string                          the address of banyand listens (default ":17912")
      --cert-file string                     the TLS cert file
      --etcd-listen-client-url string        A URL to listen on for client traffic (default "http://localhost:2379")
      --etcd-listen-peer-url string          A URL to listen on for peer traffic (default "http://localhost:2380")
  -h, --help                                 help for standalone
      --http-addr string                     listen addr for http (default ":17913")
      --http-cert-file string                the TLS cert file of http server
      --http-grpc-addr string                http server redirect grpc requests to this address (default "localhost:17912")
      --http-grpc-cert-file string           the grpc TLS cert file if grpc server enables tls
      --http-key-file string                 the TLS key file of http server
      --http-tls                             connection uses TLS if true, else plain HTTP
      --key-file string                      the TLS key file
      --logging-env string                   the logging (default "prod")
      --logging-level string                 the root level of logging (default "info")
      --logging-levels stringArray           the level logging of logging
      --logging-modules stringArray          the specific module
      --max-recv-msg-size bytes              the size of max receiving message (default 10.00MiB)
      --measure-buffer-size bytes            block buffer size (default 4.00MiB)
      --measure-encoder-buffer-size bytes    block fields buffer size (default 12.00MiB)
      --measure-idx-batch-wait-sec int       index batch wait in second (default 1)
      --measure-root-path string             the root path of database (default "/tmp")
      --measure-seriesmeta-mem-size bytes    series metadata memory size (default 1.00MiB)
      --metadata-root-path string            the root path of metadata (default "/tmp")
  -n, --name string                          name of this service (default "standalone")
      --observability-listener-addr string   listen addr for observability (default ":2121")
      --pprof-listener-addr string           listen addr for pprof (default ":6060")
      --show-rungroup-units                  show rungroup units
      --stream-block-buffer-size bytes       block buffer size (default 8.00MiB)
      --stream-global-index-mem-size bytes   global index memory size (default 2.00MiB)
      --stream-idx-batch-wait-sec int        index batch wait in second (default 1)
      --stream-root-path string              the root path of database (default "/tmp")
      --stream-seriesmeta-mem-size bytes     series metadata memory size (default 1.00MiB)
      --tls                                  connection uses TLS if true, else plain TCP
  -v, --version                              version for standalone
```
