
# Installation

`Banyand` is the daemon server of the BanyanDB database. This section will show several paths installing it in your environment.

## Get Banyand

### Build From Source

#### Requirements

Users who want to build a binary from sources have to set up:

* Go >= 1.17
* Linux or MacOS
* GNU make

#### Build Banyand

To issue the below command to get a basic binary.

```shell
$ make build
chmod +x build/bin/banyand-server
Done building banyand server
```

The build system provides a series of binary options as well.

* `make -C banyand banyand-server` generates a basic `banyand-server`.
* `make -C banyand release` builds out a static binary for releasing.
* `make -C banyand debug` gives a binary for debugging without the complier's optimizations.
* `make -C banyand debug-static` is a static binary for debugging.

Then users get binaries as below

``` shell
$ ls banyand/build/bin
banyand-server  
banyand-server-debug  
banyand-server-debug-static  
banyand-server-static
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

To discovery more options to configure the banyand by

```shell
$ ./banyand-server standalone -h
Usage:
   standalone [flags]

Flags:
      --addr string                          the address of banyand listens (default ":17912")
      --cert-file string                     the TLS cert file
  -h, --help                                 help for standalone
      --key-file string                      the TLS key file
      --logging.env string                   the logging (default "dev")
      --logging.level string                 the level of logging (default "debug")
      --max-recv-msg-size int                the size of max receiving message (default 10485760)
      --measure-root-path string             the root path of database (default "/tmp")
      --metadata-root-path string            the root path of metadata (default "/tmp")
  -n, --name string                          name of this service (default "standalone")
      --observability-listener-addr string   listen addr for observability (default ":2121")
      --pprof-listener-addr string           listen addr for pprof (default "127.0.0.1:6060")
      --show-rungroup-units                  show rungroup units
      --stream-root-path string              the root path of database (default "/tmp")
      --tls                                  connection uses TLS if true, else plain TCP
  -v, --version                              version for standalone
```
