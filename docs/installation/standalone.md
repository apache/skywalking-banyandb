# Installing BanyanDB Standalone Mode

The standalone mode is the simplest way to run Banyand. It is suitable for the development and testing environment.

Follow these steps to install BanyanDB and start up in standalone mode.


- Download or build the BanyanDB packages.
- unpack and extract the `skywalking-banyandb-x.x.x-bin.tgz`.
- start up BanyanDB standalone server.

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
