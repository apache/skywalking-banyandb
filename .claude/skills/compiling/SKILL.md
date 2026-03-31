---
name: compiling
description: Compile and build the SkyWalking BanyanDB project. Use when the user asks to compile, build, or generate code for this project.
allowed-tools: Bash, Read, Grep, Glob
---

# Compiling SkyWalking BanyanDB

Follow these steps to compile the project.

## Step 1: Generate protobuf and mock code

Run `make generate` first. This regenerates `.pb.go` files from `.proto` definitions and mock files via mockgen.

This step is **required** whenever:
- Proto files (`.proto`) have been added or modified
- Go interfaces used by mockgen have changed

If generate fails, check:
- `protoc` and Go protobuf plugins are installed
- Proto file syntax is valid
- Interface signatures match mock expectations

## Step 2: Build all binaries

Run `make build` to compile all project components: ui, banyand, bydbctl, mcp, fodc/agent, fodc/proxy.

Binaries are output to each component's `build/bin/dev/` directory.

## Troubleshooting

- **Missing fields in .pb.go**: Proto files were updated but `make generate` was not run. Run it first.
- **Mock generation failures**: An interface changed but the mock wasn't regenerated. `make generate` fixes this.
- **Import errors**: Check import aliases match CLAUDE.md conventions (e.g., `commonv1`, `databasev1`).

## Other useful targets

- `make clean` — clean all build artifacts
- `make clean-build` — clean only build binaries
- `make lint` — run linters
- `make test` — run tests
