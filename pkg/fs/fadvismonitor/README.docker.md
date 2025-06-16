# BPF Monitoring Tools Docker Environment

This directory contains Docker configurations for building and running the BPF monitoring tools for BanyanDB.

## Prerequisites

- Docker
- Docker Compose
- Linux host (BPF tools require Linux kernel)

## Docker Images

Two Docker environments are provided:

1. **Development Environment (bpf-dev)**: Contains all build tools and dependencies needed for BPF development.
2. **Runtime Environment (bpf-runtime)**: Contains minimal dependencies required to run the BPF tools.

## Usage

### Development Environment

Start a development container with all necessary tools installed:

```bash
cd pkg/fs/fadvismonitor
docker-compose run --rm bpf-dev
```

This provides a complete environment for BPF development, with source code mounted from the host.

Inside the container, you can:
- Generate BPF code: `make generate-bpf`
- Run benchmarks: `make bpf-benchmark`
- Build and test: `make all-benchmarks`

### Runtime Environment

For running the tools in production:

```bash
cd pkg/fs/fadvismonitor
docker-compose run --rm bpf-runtime
```

### Building Custom Images

To build a custom image:

```bash
# From project root
docker build -f pkg/fs/fadvismonitor/Dockerfile -t banyandb-bpf:dev .
```

## Integration with CI

The CI pipeline will continue to use the existing Makefile structure for building and testing. The Docker environment is provided as a convenience for development and deployment.

## Troubleshooting

### Kernel Headers

If you encounter kernel header issues, ensure your host has the matching Linux headers installed:

```bash
sudo apt-get install linux-headers-$(uname -r)
```

### Privileged Mode

BPF operations require privileged access. The Docker containers are configured to run with the necessary privileges.
