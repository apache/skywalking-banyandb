# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Build stage
FROM golang:1.24-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    clang \
    llvm \
    libbpf-dev \
    linux-headers-generic \
    make \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire project
COPY . .

# Build the eBPF sidecar
WORKDIR /build/ebpf-sidecar
RUN make generate && make build

# Runtime stage
FROM ubuntu:22.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for running the service
# Note: eBPF requires CAP_BPF and CAP_PERFMON capabilities
RUN useradd -m -u 1000 -s /bin/bash ebpf

# Copy the binary from builder
COPY --from=builder /build/ebpf-sidecar/build/bin/ebpf-sidecar /usr/local/bin/ebpf-sidecar

# Copy default configuration
RUN mkdir -p /etc/ebpf-sidecar
COPY --from=builder /build/ebpf-sidecar/configs/config.yaml /etc/ebpf-sidecar/config.yaml

# Create directories for eBPF maps pinning
RUN mkdir -p /sys/fs/bpf/ebpf-sidecar && \
    chown -R ebpf:ebpf /sys/fs/bpf/ebpf-sidecar

# Expose ports
EXPOSE 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Set the entrypoint
# Note: Container must be run with --privileged or specific capabilities
ENTRYPOINT ["/usr/local/bin/ebpf-sidecar"]
CMD ["--config-file", "/etc/ebpf-sidecar/config.yaml"]