# Standalone Mode

The standalone mode is the simplest way to run Banyand. It is suitable for the development and testing environment.

Follow these steps to install BanyanDB and start up in standalone mode.

## Download the BanyanDB packages

Go to the [SkyWalking download page](https://skywalking.apache.org/downloads/#Database).

Select and download the release package from the suggested location for your platform, such as `skywalking-banyandb-x.x.x-bin.tgz`.

## Install the BanyanDB packages

unpack and extract the package.

```shell
$ tar -zxpf skywalking-banyandb-x.x.x-bin.tgz

-rw-r--r--@   1 banyand  group   6449  5 10 05:45 CHANGES.md
-rw-r--r--@   1 banyand  group  23825  5 10 05:45 LICENSE
-rw-r--r--@   1 banyand  group    290  5 10 05:45 LICENSE.tpl
-rw-r--r--@   1 banyand  group    171  5 10 05:45 NOTICE
-rw-r--r--@   1 banyand  group   3034  5 10 05:45 README.md
drwxr-xr-x@  10 banyand  group    320  5 10 05:45 bin
drwx------@ 145 banyand  group   4640  5 10 05:45 licenses
```

start up BanyanDB standalone server.

```shell
$ ./bin/banyand-server-static standalone
```

Then the server running as a standalone process.

```shell

██████╗  █████╗ ███╗   ██╗██╗   ██╗ █████╗ ███╗   ██╗██████╗ ██████╗ 
██╔══██╗██╔══██╗████╗  ██║╚██╗ ██╔╝██╔══██╗████╗  ██║██╔══██╗██╔══██╗
██████╔╝███████║██╔██╗ ██║ ╚████╔╝ ███████║██╔██╗ ██║██║  ██║██████╔╝
██╔══██╗██╔══██║██║╚██╗██║  ╚██╔╝  ██╔══██║██║╚██╗██║██║  ██║██╔══██╗
██████╔╝██║  ██║██║ ╚████║   ██║   ██║  ██║██║ ╚████║██████╔╝██████╔╝
╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═══╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═══╝╚═════╝ ╚═════╝ 
{"level":"info","time":"2024-05-28T11:27:58+08:00","message":"starting as a standalone server"}
...
...
{"level":"info","module":"LIAISON-GRPC","addr":":17912","time":"2024-05-28T11:27:59+08:00","message":"Listening to"}
{"level":"info","module":"LIAISON-HTTP","listenAddr":":17913","time":"2024-05-28T11:27:59+08:00","message":"Start liaison http server"}
```

The banyand server would be listening on the `0.0.0.0:17912` to access gRPC requests. if no errors occurred.

At the same time, the banyand server would be listening on the `0.0.0.0:17913` to access HTTP requests. if no errors occurred. The HTTP server is used for CLI and Web UI.

The Web UI is hosted at `http://localhost:17913/`.
