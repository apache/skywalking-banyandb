# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This Dockerfile is only for local development and testing purposes.

# Dockerfile: Multi-stage builder for eBPF object compilation and benchmark execution

# Stage 1: Build eBPF object files
FROM ubuntu:20.04 AS builder

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install eBPF compilation dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        clang \
        llvm \
        libelf-dev \
        linux-headers-generic \
        linux-tools-generic \
        linux-tools-common \
        make \
        git \
        golang && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Set working directory to fadvis test root
WORKDIR /workspace/test/stress/fadvis

# Copy Makefile and BPF source files
COPY test/stress/fadvis/Makefile ./
COPY test/stress/fadvis/bpf/ ./bpf/

# Generate vmlinux.h and BPF helper headers (adjust path)
RUN mkdir -p bpf/headers && \
    bpftool btf dump file /sys/kernel/btf/vmlinux format c > bpf/headers/vmlinux.h || \
    echo "Warning: Failed to generate vmlinux.h, using pre-generated file if available" && \
    cp -r /usr/include/bpf bpf/headers/ || echo "Warning: Failed to copy BPF headers"

# Compile object files for both ARM64 and AMD64 architectures (make should now work)
RUN make bpf

# Stage 2: Run benchmark tests
FROM golang:1.24-bullseye

# Working directory for Go module
WORKDIR /workspace

# Copy Go module files first to leverage caching
COPY go.mod go.sum ./

# Download dependencies before copying all source code
RUN go mod download

COPY pkg/fadvis/ ./pkg/fadvis/
COPY banyand/ ./banyand/
COPY test/stress/fadvis/ ./test/stress/fadvis/

# Copy the compiled BPF objects from the builder stage (adjust source path)
COPY --from=builder /workspace/test/stress/fadvis/bpf/*.o ./test/stress/fadvis/bpf/

# Build the necessary test code (optional, `go test` will build)
# We can potentially remove this RUN command if `go test` handles the build fine
RUN go build -o /dev/null ./test/stress/fadvis/... || echo "Build may have warnings, but we can proceed"

# Default entrypoint to run benchmarks with focus on the fadvis tests only
ENTRYPOINT ["go", "test", "-bench=.", "-v", "./test/stress/fadvis/..."]
