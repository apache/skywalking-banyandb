# Stream vs Trace Performance Test - Docker Version

This directory contains the Docker-based implementation of the Stream vs Trace performance test.

## Overview

The test runs two separate BanyanDB instances in Docker containers:
- **banyandb-stream**: Dedicated instance for stream model testing
- **banyandb-trace**: Dedicated instance for trace model testing

The test clients run outside the containers on the host machine, connecting to the containerized BanyanDB instances via exposed ports.

## Architecture

```
┌─────────────────────────────┐
│   Host Machine              │
│                             │
│  ┌─────────────────────┐    │
│  │  Test Client        │    │
│  │  (Go Test Suite)    │    │
│  └──────┬──────┬───────┘    │
│         │      │             │
│    :17912  :27912           │
└─────────┼──────┼────────────┘
          │      │
     ┌────▼──┐ ┌─▼─────┐
     │Stream │ │ Trace │
     │BanyanDB│ │BanyanDB│
     │Container│ │Container│
     └────────┘ └────────┘
```

## Resource Limits

Each container is configured with:
- **CPU**: 2 cores (limit), 1 core (reservation)
- **Memory**: 4GiB (limit), 2GiB (reservation)

## Port Mapping

### Stream Container (banyandb-stream)
- gRPC: 17912
- HTTP: 17913
- pprof: 6060
- metrics: 2121

### Trace Container (banyandb-trace)
- gRPC: 27912 (offset by 10000)
- HTTP: 27913 (offset by 10000)
- pprof: 16060 (offset by 10000)
- metrics: 12121 (offset by 10000)

## Prerequisites

- Docker and Docker Compose
- Go 1.23 or later
- GNU Make

## Usage

### Quick Start

```bash
# Run complete test workflow (build, start, test, stop)
make all

# Or using the script directly
./run-docker-test.sh all
```

### Available Commands

Both `make` and `./run-docker-test.sh` support the same commands:

| Command | Description |
|---------|-------------|
| `all` | Run complete test workflow (build → up → test → down) |
| `build` | Build BanyanDB binaries and Docker images |
| `up` | Start containers and wait for health |
| `test` | Run performance tests (containers must be running) |
| `down` | Stop and remove containers |
| `clean` | Clean up everything including volumes |
| `logs` | Show container logs (follow mode) |
| `ps` | Show container status |
| `stats` | Show container resource usage |
| `help` | Show usage information |

### Common Workflows

#### Full Test Run
```bash
# Using make
make all

# Using script
./run-docker-test.sh all
```

#### Development Workflow
```bash
# Build and start containers
make build up

# Run tests multiple times
make test
make test

# View logs in another terminal
make logs

# Check resource usage
make stats

# Clean up when done
make down
```

#### Debugging Workflow
```bash
# Start containers
make up

# Check status
make ps

# View logs
make logs

# Run specific test
export DOCKER_TEST=true
cd test/stress/stream-vs-trace/docker
go test -v -run TestStreamVsTraceDocker/specific_test

# Keep containers running for inspection
# (Don't run make down)
```

### Script Usage

The `run-docker-test.sh` script provides the same functionality:

```bash
# Show help
./run-docker-test.sh help

# Run individual commands
./run-docker-test.sh build
./run-docker-test.sh up
./run-docker-test.sh test
./run-docker-test.sh down

# View logs
./run-docker-test.sh logs

# Check status
./run-docker-test.sh ps
./run-docker-test.sh stats
```

## Files

- `Dockerfile`: Multi-stage build that compiles BanyanDB release binaries and creates minimal runtime image
- `docker-compose.yml`: Container orchestration configuration for stream and trace instances
- `Makefile`: Convenient interface that delegates all operations to run-docker-test.sh
- `run-docker-test.sh`: Main script containing all build, run, and test logic
- `wait-for-healthy.sh`: Helper script to wait for containers to be healthy
- `docker_test.go`: Test suite for Docker-based testing
- `schema_client.go`: gRPC client for loading schemas into BanyanDB
- `client_wrappers.go`: Wrapper clients exposing gRPC connections
- `README.md`: This documentation

## Architecture Details

### Build Process
1. The Dockerfile uses a multi-stage build:
   - **Builder stage**: Compiles BanyanDB release binaries using Go 1.23
   - **Runtime stage**: Creates minimal Alpine-based image with only the binary

2. Release binaries are built with:
   - Static linking for portability
   - Platform-specific paths (e.g., `linux/amd64`)
   - Optimizations enabled

### Test Architecture
- All logic is centralized in `run-docker-test.sh`
- The Makefile provides a familiar interface but delegates to the script
- This ensures consistency and single source of truth

## Differences from Non-Docker Version

The Docker version differs from the standalone version in several ways:

1. **Isolation**: Each BanyanDB instance runs in its own container with dedicated resources
2. **Port Mapping**: Trace instance uses ports offset by 10000 to avoid conflicts
3. **Binary Type**: Uses statically-linked release binaries for better container compatibility
4. **Schema Loading**: Schemas are loaded via gRPC after containers start (not via CLI)
5. **Resource Limits**: Enforced CPU and memory limits per container

## Troubleshooting

### Containers not starting
```bash
# Check container logs
docker-compose logs banyandb-stream
docker-compose logs banyandb-trace

# Check container health
docker inspect banyandb-stream | grep -A 5 "Health"
docker inspect banyandb-trace | grep -A 5 "Health"
```

### Connection refused errors
- Ensure Docker is running
- Check if ports are already in use: `lsof -i :17912` or `lsof -i :27912`
- Verify containers are healthy: `docker-compose ps`

### Schema loading failures
- Check if schema files exist in `../testdata/schema/`
- Verify file permissions
- Check container logs for specific errors

## Performance Monitoring

During the test, you can monitor container performance:

```bash
# Real-time stats
docker stats banyandb-stream banyandb-trace

# Access pprof (stream)
go tool pprof http://localhost:6060/debug/pprof/profile

# Access pprof (trace)
go tool pprof http://localhost:16060/debug/pprof/profile

# Metrics endpoints
curl http://localhost:2121/metrics   # Stream
curl http://localhost:12121/metrics  # Trace
```

## Expected Output

A successful test run will show:
1. Docker images being built with BanyanDB release binaries
2. Both containers starting and becoming healthy
3. Schema loading confirmation for both instances
4. Performance test results comparing stream vs trace models
5. Container resource usage statistics
6. Containers being stopped and cleaned up

Example output snippet:
```
✅ Stream vs Trace Performance Test - Complete Workflow
Building Docker containers...
Starting Docker containers...
Waiting for containers to be healthy...
Running performance tests...
=== RUN   TestStreamVsTraceDocker
...performance metrics...
--- PASS: TestStreamVsTraceDocker
Test completed successfully!
```