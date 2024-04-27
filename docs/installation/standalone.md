# Standalone Mode

The standalone mode is the simplest way to run Banyand. It is suitable for the development and testing environment. Once you unpack and extract the `skywalking-banyandb-x.x.x-bin.tgz`, you could startup BanyanDB server, the standalone mode is running as a standalone process.

```shell
$ cd skywalking-banyandb-x.x.x-bin/bin
$ ./banyand-server-static standalone
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

The banyand server would be listening on the `0.0.0.0:17912` to access gRPC requests. if no errors occurred.

At the same time, the banyand server would be listening on the `0.0.0.0:17913` to access HTTP requests. if no errors occurred. The HTTP server is used for CLI and Web UI.
