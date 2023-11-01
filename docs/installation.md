
# Installation

`Banyand` is the daemon server of the BanyanDB database. This section will show several paths installing it in your environment.

## Get Binaries

### Released binaries

Get binaries from the [download](https://skywalking.apache.org/downloads/).

### Build From Source

#### Requirements

Users who want to build a binary from sources have to set up:

* Go 1.21
* Node 20.9
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

## Setup Multiple Banyand as Cluster

Firstly, you need to setup a etcd cluster which is required for the metadata module to provide the metadata service and nodes discovery service for the whole cluster. The etcd cluster can be setup by the [etcd installation guide](https://etcd.io/docs/v3.5/install/). The etcd version should be `v3.1` or above.

Then, you can start the metadata module by

```shell

Considering the etcd cluster is spread across three nodes with the addresses `10.0.0.1:2379`, `10.0.0.2:2379`, and `10.0.0.3:2379`, Data nodes and liaison nodes are running as independent processes by

```shell
$ ./banyand-server storage --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
$ ./banyand-server storage --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
$ ./banyand-server storage --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
$ ./banyand-server liaison --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
```

## Docker & Kubernetes

The docker image of banyandb is available on [Docker Hub](https://hub.docker.com/r/apache/skywalking-banyandb).

If you want to onboard banyandb to the Kubernetes, you can refer to the [banyandb-helm](https://github.com/apache/skywalking-banyandb-helm).
