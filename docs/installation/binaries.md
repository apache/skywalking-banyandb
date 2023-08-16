# Get Binaries

This page shows how to get binaries of Banyand.

## Prebuilt Released binaries

Get binaries from the [download](https://skywalking.apache.org/downloads/).

## Build From Source

### Requirements

Users who want to build a binary from sources have to set up:

* Go 1.20
* Node 18.16
* Git >= 2.30
* Linux, macOS or Windows+WSL2
* GNU make

### Windows

BanyanDB is built on Linux and macOS that introduced several platform-specific characters to the building system. Therefore, we highly recommend you use [WSL2+Ubuntu](https://ubuntu.com/wsl) to execute tasks of the Makefile.

### Build Binaries

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
