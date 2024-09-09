# Installation On Docker

## Images on Docker Hub

The BanyanDB images are hosted on Docker Hub. You can pull the images from the following links: [Apache SkyWalking BanyanDB](https://hub.docker.com/r/apache/skywalking-banyandb)

There are two types of images:

- `apache/skywalking-banyandb:<version>` - The specific version of the BanyanDB.
- `apache/skywalking-banyandb:<version>-slim` - The slim version of the BanyanDB. It does not contain the Web UI.

We pushed `linux/amd64` and `linux/arm64` for each type of image. You can pull the image for the specific architecture.

## Images on GitHub Container Registry

The BanyanDB images are hosted on GitHub Container Registry for development or testing. You can pull the images from the following links: [ghcr.io/apache/skywalking-banyandb](https://github.com/apache/skywalking-banyandb/pkgs/container/skywalking-banyandb)

There are three types of images:

- `ghcr.io/apache/skywalking-banyandb:<github-sha>` - The specific version of the BanyanDB. We pushed `linux/amd64`,  `linux/arm64` and `windows/amd64` for each type of image.
- `ghcr.io/apache/skywalking-banyandb:<github-sha>-slim` - The slim version of the BanyanDB. It does not contain the Web UI. We pushed `linux/amd64`,  `linux/arm64` and `windows/amd64` for each type of image.
- `ghcr.io/apache/skywalking-banyandb:<github-sha>-testing` - The testing version of the BanyanDB. It contains the Web UI and the `bydbctl`. We pushed `linux/amd64` and  `linux/arm64` for each type of image.

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
