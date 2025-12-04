# FODC Detector E2E Tests

This directory contains end-to-end tests for the FODC (Failure Observer and Death Rattle Detector) detector functionality.

## Overview

The e2e tests verify that the FODC detector can:
- Run as a sidecar alongside BanyanDB
- Discover BanyanDB endpoints automatically
- Detect death rattle files
- Monitor health checks
- Poll metrics from BanyanDB
- Record snapshots in the flight recorder
- Expose health endpoints

## Test Structure

- `docker-compose.yml`: Defines the test environment with BanyanDB and FODC sidecar
- `e2e.yaml`: Main e2e test configuration
- `detector-cases.yaml`: Test cases for verification
- `trigger-scenarios.sh`: Script that triggers various test scenarios
- Test data files are located in `fodc/internal/detector/testdata/`

## Running the Tests

To run the e2e tests:

```bash
# From the project root
cd test/e2e-v2/cases/fodc/detector

# Run using the skywalking-infra-e2e framework
# (This requires the e2e framework to be set up)
```

Or using the standard e2e test runner:

```bash
# From the project root
make e2e-test FODC_DETECTOR=true
```

## Test Scenarios

1. **Health Endpoint**: Verifies FODC sidecar exposes a health endpoint
2. **Service Discovery**: Verifies FODC can discover BanyanDB endpoints
3. **Death Rattle Detection**: Verifies file-based death rattle detection
4. **Metrics Polling**: Verifies FODC polls metrics from BanyanDB
5. **Flight Recorder**: Verifies flight recorder file is created

## Prerequisites

- Docker and Docker Compose
- BanyanDB image available
- FODC Docker image built (via `make docker-build` in fodc directory)
- skywalking-infra-e2e framework (for CI/CD)

## Test Environment

The test uses docker-compose service names for communication:
- `banyandb`: BanyanDB service
- `fodc-sidecar`: FODC sidecar service
- `fodc-test-helper`: Helper container for running test commands

All containers communicate via the `e2e` docker network.

## Troubleshooting

If tests fail:
1. Check that BanyanDB is healthy: `docker ps` and check logs
2. Check FODC sidecar logs: `docker logs fodc-sidecar`
3. Verify shared volumes: `/tmp` should be shared between containers
4. Check network connectivity: containers should be on the same network

