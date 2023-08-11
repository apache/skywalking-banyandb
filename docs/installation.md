
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
  liaison     Run as the liaison server
  meta        Run as the meta server
  standalone  Run as the standalone server
  storage     Run as the storage server

Flags:
  -h, --help      help for this command
  -v, --version   version for this command

Use " [command] --help" for more information about a command.
```

Banyand is running as a standalone server by

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

## Setup Multiple Banyand as Cluster(TBD)

### Setup Standalone Nodes

The standalone node is running as a standalone process by

```shell
$ ./banyand-server standalone <flags> --data-node-id n1
$ ./banyand-server standalone <flags> --data-node-id n2
$ ./banyand-server standalone <flags> --data-node-id n3
```

`data-node-id` is the unique identifier of the standalone node.

The standalone node would be listening on the `<ports>` if no errors occurred.

### Setup Role-Based Nodes

The meta nodes should boot up firstly to provide the metadata service for the whole cluster. The meta node is running as a standalone process by

```shell
$ ./banyand-server meta <flags>
```

The meta node would be listening on the `<ports>` if no errors occurred.


Data nodes, query nodes and liaison nodes are running as independent processes by

```shell
$ ./banyand-server storage --mode data --data-node-id n1 <flags>
$ ./banyand-server storage --mode data --data-node-id n2 <flags>
$ ./banyand-server storage --mode data --data-node-id n3 <flags>
$ ./banyand-server storage --mode query <flags>
$ ./banyand-server storage --mode query <flags>
$ ./banyand-server liaison <flags>
```

`data-node-id` is the unique identifier of the data node.

The data node, query node and liaison node would be listening on the `<ports>` if no errors occurred.

If you want to use a `mix` mode instead of separate query and data nodes, you can run the banyand-server as processes by

```shell
$ ./banyand-server storage --data-node-id n1 <flags>
$ ./banyand-server storage --data-node-id n2 <flags>
$ ./banyand-server storage --data-node-id n3 <flags>
$ ./banyand-server liaison <flags>
```
