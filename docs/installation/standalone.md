# Installing BanyanDB Standalone Mode

The standalone mode is the simplest way to run Banyand. It is suitable for the development and testing environment.

Follow these steps to install BanyanDB and start up in standalone mode.

- Download or build the BanyanDB packages.
- Unpack and extract the `skywalking-banyandb-x.x.x-bin.tgz`.
- Select the binary for your platform, such as `banyand-linux-amd64` or `banyand-darwin-amd64`.
- Move the binary to the directory you want to run BanyanDB. For instance, `mv banyand-linux-amd64 /usr/local/bin/banyand`. The following steps assume that the binary is in the `/usr/local/bin` directory.
- Start up BanyanDB standalone server.

```shell
banyand standalone
```

Then the server running as a standalone process.

```shell

鈻堚枅鈻堚枅鈻堚枅鈺? 鈻堚枅鈻堚枅鈻堚晽 鈻堚枅鈻堚晽   鈻堚枅鈺椻枅鈻堚晽   鈻堚枅鈺?鈻堚枅鈻堚枅鈻堚晽 鈻堚枅鈻堚晽   鈻堚枅鈺椻枅鈻堚枅鈻堚枅鈻堚晽 鈻堚枅鈻堚枅鈻堚枅鈺?
鈻堚枅鈺斺晲鈺愨枅鈻堚晽鈻堚枅鈺斺晲鈺愨枅鈻堚晽鈻堚枅鈻堚枅鈺? 鈻堚枅鈺戔暁鈻堚枅鈺?鈻堚枅鈺斺暆鈻堚枅鈺斺晲鈺愨枅鈻堚晽鈻堚枅鈻堚枅鈺? 鈻堚枅鈺戔枅鈻堚晹鈺愨晲鈻堚枅鈺椻枅鈻堚晹鈺愨晲鈻堚枅鈺?鈻堚枅鈻堚枅鈻堚枅鈺斺暆鈻堚枅鈻堚枅鈻堚枅鈻堚晳鈻堚枅鈺斺枅鈻堚晽 鈻堚枅鈺?鈺氣枅鈻堚枅鈻堚晹鈺?鈻堚枅鈻堚枅鈻堚枅鈻堚晳鈻堚枅鈺斺枅鈻堚晽 鈻堚枅鈺戔枅鈻堚晳  鈻堚枅鈺戔枅鈻堚枅鈻堚枅鈻堚晹鈺?鈻堚枅鈺斺晲鈺愨枅鈻堚晽鈻堚枅鈺斺晲鈺愨枅鈻堚晳鈻堚枅鈺戔暁鈻堚枅鈺椻枅鈻堚晳  鈺氣枅鈻堚晹鈺? 鈻堚枅鈺斺晲鈺愨枅鈻堚晳鈻堚枅鈺戔暁鈻堚枅鈺椻枅鈻堚晳鈻堚枅鈺? 鈻堚枅鈺戔枅鈻堚晹鈺愨晲鈻堚枅鈺?鈻堚枅鈻堚枅鈻堚枅鈺斺暆鈻堚枅鈺? 鈻堚枅鈺戔枅鈻堚晳 鈺氣枅鈻堚枅鈻堚晳   鈻堚枅鈺?  鈻堚枅鈺? 鈻堚枅鈺戔枅鈻堚晳 鈺氣枅鈻堚枅鈻堚晳鈻堚枅鈻堚枅鈻堚枅鈺斺暆鈻堚枅鈻堚枅鈻堚枅鈺斺暆
鈺氣晲鈺愨晲鈺愨晲鈺?鈺氣晲鈺? 鈺氣晲鈺濃暁鈺愨暆  鈺氣晲鈺愨晲鈺?  鈺氣晲鈺?  鈺氣晲鈺? 鈺氣晲鈺濃暁鈺愨暆  鈺氣晲鈺愨晲鈺濃暁鈺愨晲鈺愨晲鈺愨暆 鈺氣晲鈺愨晲鈺愨晲鈺?
{"level":"info","time":"2024-05-28T11:27:58+08:00","message":"starting as a standalone server"}
...
...
{"level":"info","module":"LIAISON-GRPC","addr":":17912","time":"2024-05-28T11:27:59+08:00","message":"Listening to"}
{"level":"info","module":"LIAISON-HTTP","listenAddr":":17913","time":"2024-05-28T11:27:59+08:00","message":"Start liaison http server"}
```

The banyand server would be listening on the `0.0.0.0:17912` to access gRPC requests. if no errors occurred.

At the same time, the banyand server would be listening on the `0.0.0.0:17913` to access HTTP requests. if no errors occurred. The HTTP server is used for CLI and Web UI.

The Web UI is hosted at `http://localhost:17913/`.
