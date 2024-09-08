# Get Binaries

This page shows how to get binaries of Banyand.

## Prebuilt Released binaries
                           
Go to the [SkyWalking download page](https://skywalking.apache.org/downloads/#Database) .

Select and download the distribution from the suggested location for your platform, such as `skywalking-banyandb-x.x.x-banyand.tgz` and `skywalking-bydbctl-x.x.x-bydbctl.tgz`.

> It is essential that you verify the integrity of the downloaded file using the PGP signature ( .asc file) or a hash ( .md5 or .sha* file).

unpack and extract the package.

```shell
tar -zxvf skywalking-banyandb-x.y.z-banyand.tgz
tar -zxvf skywalking-bydbctl-x.y.z-bydbctl.tgz
```

The banyand and bydbctl directory structure is as follows.

```shell
├── CHANGES.md
├── LICENSE
├── LICENSE.tpl
├── NOTICE
├── README.md
├── bin
│   ├──banyand-server-slim-linux-amd64 
│   ├──banyand-server-slim-linux-arm64 
│   ├──banyand-server-static-linux-amd64 
│   └──banyand-server-static-linux-arm64
└── licenses
```

```shell
├── CHANGES.md
├── LICENSE
├── LICENSE.tpl
├── NOTICE
├── README.md
├── bin
│   ├── bydbctl-cli-static-linux-386
│   ├── bydbctl-cli-static-linux-amd64
│   ├── bydbctl-cli-static-linux-arm64
│   ├── bydbctl-cli-static-windows-386
│   ├── bydbctl-cli-static-windows-amd64
│   ├── bydbctl-cli-static-darwin-amd64
│   └── bydbctl-cli-static-darwin-arm64
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
make generate
...
make build
--- ui: all ---
...
Done building ui
--- banyand: all ---
...
chmod +x build/bin/banyand-server;
Done building build/bin/dev/banyand-server
...
--- bydbctl: all ---
...
chmod +x build/bin/dev/bydbctl-cli;
Done building build/bin/dev/bydbctl-cli
```

Then users get binaries as below

``` shell
ls banyand/build/bin/dev
banyand-server  

ls bydbctl/build/bin/dev
bydbctl-cli
```

The build system provides a series of binary options as well.

* `make -C banyand banyand-server` generates a basic `banyand-server`.
* `make -C banyand banyand-server-static` builds out a static binary `banyand-server-static` which is statically linked with all dependencies.
* `make -C banyand banyand-server-slim` builds out a slim binary `banyand-server-slim` which doesn't include `UI`.
* `make -C banyand release` builds out the static and slim binaries for releasing.
* `make -C bydbctl bydbctl-cli` generates a basic `bydbctl-cli`.
* `make -C bydbctl release` or `make -C banyand bydbctl-cli-static` builds out a static binary `bydbctl-cli-static` for releasing. This binary is statically linked with all dependencies.

> The build script now checks if the binary file exists before rebuilding. If you want to rebuild, please remove the binary file manually by running `make clean-build`.

### Cross-compile Binaries

The build system supports cross-compiling binaries for different platforms. For example, to build a Windows binary on a Linux machine, you can issue the following command:

```shell
TARGET_OS=windows PLATFORMS=windows/amd64 make release
```

The `PLATFORMS` variable is a list of platforms separated by commas. The `TARGET_OS` variable is the target operating system. You could specify several platforms at once:

```shell
TARGET_OS=linux PLATFORMS=linux/amd64,linux/arm64 make release
TARGET_OS=darwin PLATFORMS=darwin/amd64,darwin/arm64 make release
```

The build system will generate binaries for each platform in the `build/bin` directory.

```shell
darwin/amd64/banyand-server-static
darwin/arm64/banyand-server-static
linux/amd64/banyand-server-static
linux/arm64/banyand-server-static
windows/amd64/banyand-server-static

darwin/amd64/bydbctl-cli-static
darwin/arm64/bydbctl-cli-static
linux/amd64/bydbctl-cli-static
linux/arm64/bydbctl-cli-static
windows/amd64/bydbctl-cli-static
```
