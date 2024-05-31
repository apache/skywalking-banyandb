# Get Binaries

This page shows how to get binaries of Banyand.

## Prebuilt Released binaries
                           
Go to the [SkyWalking download page](https://skywalking.apache.org/downloads/#Database) .

Select and download the distribution from the suggested location for your platform, such as `skywalking-banyandb-x.x.x-bin.tgz`.

> It is essential that you verify the integrity of the downloaded file using the PGP signature ( .asc file) or a hash ( .md5 or .sha* file).

unpack and extract the package.

```shell
$ tar -zxvf skywalking-banyandb-x.x.x-bin.tgz
```

The directory structure is as follows.

```shell
├── CHANGES.md
├── LICENSE
├── LICENSE.tpl
├── NOTICE
├── README.md
├── bin
│   ├── banyand-server-static
│   ├── bydbctl-0.6.0-darwin-amd64
│   ├── bydbctl-0.6.0-darwin-arm64
│   ├── bydbctl-0.6.0-linux-386
│   ├── bydbctl-0.6.0-linux-amd64
│   ├── bydbctl-0.6.0-linux-arm64
│   ├── bydbctl-0.6.0-windows-386
│   └── bydbctl-0.6.0-windows-amd64
└── licenses
```

## Build From Source

### Requirements

Users who want to build a binary from sources have to set up:

* Go 1.22
* Node 20.12
* Git >= 2.30
* Linux, macOS or Windows+WSL2
* GNU make

### Windows

BanyanDB is built on Linux and macOS that introduced several platform-specific characters to the building system. Therefore, we highly recommend you use [WSL2+Ubuntu](https://ubuntu.com/desktop/wsl) to execute tasks of the Makefile.

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
* `make -C banyand release` or `make -C banyand banyand-server-static` builds out a static binary `banyand-server-static` for releasing.
* `make -C banyand debug` gives a binary for debugging without the complier's optimizations.
* `make -C banyand debug-static` is a static binary for debugging.
* `make -C bydbctl release` cross-builds several binaries for multi-platforms.

Then users get binaries as below

``` shell
$ ls banyand/build/bin
banyand-server  
banyand-server-static
banyand-server-debug  
banyand-server-debug-static  

$ ls bydbctl/build/bin
bydbctl  bydbctl--darwin-amd64  bydbctl--darwin-arm64  bydbctl--linux-386  bydbctl--linux-amd64  bydbctl--linux-arm64  bydbctl--windows-386  bydbctl--windows-amd64
```

> The build script now checks if the binary file exists before rebuilding. If you want to rebuild, please remove the binary file manually.
