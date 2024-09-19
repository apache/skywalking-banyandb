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

## Build and Push the Custom Docker image

The BanyanDB Docker image can be built from the source code. You can build the Docker image by running the following commands:

```shell
make generate
make release
HUB=<private_hub> IMG_NAME=<private_name> TAG=<private_tag> make docker.build
```

The `make release` command builds binaries and generates the necessary files for building the Docker image. The `make docker.build` command builds the Docker image with the architecture aligned with the host machine.

`HUB` is the Docker Hub username or the Docker Hub organization name. `IMG_NAME` is the Docker image name. `TAG` is the Docker image tag.

Note: You can't build other OS or architecture images not identical to the host machine. That's because the docker build command fails to load the image after the build process.

The `make docker.push` command pushes the Docker image to the Docker Hub. You need to log in to the Docker Hub before running this command.

```shell
docker login
make generate
make release
make docker.push
```

If you want to push other OS or architecture images, you can run the following commands:

```shell
docker login
make generate
TARGET_OS=linux PLATFORMS=linux/amd64,linux/arm64 make release
PLATFORMS=linux/amd64,linux/arm64 make docker.push
```

The `TARGET_OS` environment variable specifies the target OS. The `PLATFORMS` environment variable specifies the target architectures. The `PLATFORMS` environment variable is a comma-separated list of the target architectures.

If you want to build and publish slim images, you can run the following commands:

```shell
docker login
make generate
make release
BINARYTYPE=slim make docker.build
BINARYTYPE=slim make docker.push
```
