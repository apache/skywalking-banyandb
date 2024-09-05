# Installation On Docker

## Start a container in `standalone mode`
The following commands pull the docker image and run the BanyanDB on Docker. Replace `latest` with the version of the BanyanDB you want to run.
- pull the image
```shell
docker pull apache/skywalking-banyandb:latest
```
- run the container
```shell
docker run -d \
  -p 17912:17912 \
  -p 17913:17913 \
  --name banyandb \
  apache/skywalking-banyandb:latest \
  standalone
```

The BanyanDB server would be listening on the `0.0.0.0:17912` to access gRPC requests. if no errors occurred.

At the same time, the BanyanDB server would be listening on the `0.0.0.0:17913` to access HTTP requests. if no errors occurred. The HTTP server is used for CLI and Web UI.

The Web UI is hosted at `http://localhost:17913/`.
